rm(list=ls())
library(data.table)
library(httr2)
library(xml2)
library(tidyverse)
library(readxl)
library(writexl)

setwd("/Users/pauorive23/Desktop/Màster BSE/Thesis/")

# Polish the dataset for the FIRST VERSION:

# The methodology consists to edit some of the variables (e.g. sectors of demand) and impute values for the missing data in the full dataset,
# Missings for variables such as battery charge and discharge, for which we have very few data points, will not be imputed.
# Different imputation methods are used for different variables.

# Load full_dataset and copy it to full_dataset_imputed, the one that we will modify
# Since we add columns with computations, full_dataset_imputed will have more columns than full_dataset
# At the end of the document, we will make a selection of the columns that we want to keep for the Julia code (data_version1)
full_dataset <- read_xlsx("Data/full_dataset.xlsx")
full_dataset_imputed <- full_dataset

# Check the columns with missing values
colSums(is.na(full_dataset_imputed))

# 1. Generation data
# Impute missings to those columns that have 170 missings (including total national demand)
# Count NAs per column
na_counts <- colSums(is.na(full_dataset))

# Get column names 170, as well as 31 or 35 missings, which are quite random
cols_170 <- names(na_counts[na_counts == 170])
cols_35  <- names(na_counts[na_counts == 35])
cols_31  <- names(na_counts[na_counts == 31])


impute_columns <- c(cols_170, cols_35, cols_31)

# To take into account the seasonality of the data, we will impute the missing values using the mean of the same hour of the previous and next week
# Our beloved friend ChatGPT helped us to write a function that does what we want

impute_value <- function(df, idx, col_name) {
  target_time <- df$time_long[idx]
  target_hour <- hour(target_time)
  
  past_times <- seq(target_time - days(7), target_time - days(1), by = "1 day")
  future_times <- seq(target_time + days(1), target_time + days(7), by = "1 day")
  
  nearby_times <- c(past_times, future_times)
  
  match_rows <- df %>% 
    filter(time_long %in% nearby_times & hour(time_long) == target_hour)
  
  available_values <- match_rows[[col_name]]
  
  if (length(available_values[!is.na(available_values)]) > 0) {
    return(mean(available_values, na.rm = TRUE))
  } else {
    return(NA)
  }
}

# Main imputation process
impute_generation_data <- function(df) {
  df_imputed <- df
  
  for (col in impute_columns) {
    na_indices <- which(is.na(df[[col]]))
    
    if (length(na_indices) > 0) {
      imputed_values <- map_dbl(na_indices, ~ impute_value(df, .x, col))
      df_imputed[na_indices, col] <- imputed_values
    }
  }
  
  return(df_imputed)
}

full_dataset_imputed <- impute_generation_data(full_dataset_imputed)

colSums(is.na(full_dataset_imputed))

# Compute the capacity factors for wind and solar, as well as for nuclear and hydro
full_dataset_imputed <- full_dataset_imputed %>% 
  mutate(solar_pv_cap_factor = solar_pv_gen_mw / solar_pv_cap,
         solar_thermal_cap_factor = solar_thermal_gen_mw / solar_thermal_cap,
         wind_cap_factor = wind_gen_mw / wind_cap,
         hydro_cap_factor = hydro_self_reported_cap_mw / hydro_cap,
         nuclear_cap_factor = nuclear_self_reported_cap_mw / nuclear_cap)

# Recompute the gap variables, which had 201 missing values, though we will not use them
full_dataset_imputed <- full_dataset_imputed %>% 
  mutate(gap_obs_interconnections_mw = net_international_flows_mw - total_pen_interconnections_mw,
         gap_ind_flows_morocco = net_imp_MOR_mw - net_pen_interconnection_morocco_mw)


# 2. Demand data
# The main problem with disaggregated data is that we only have data for 8 months previous to data collection
# The method we follow is to impute the share of demand 1 year before the exact same day, scaled to match the total national demand.
impute_demand_data_fast <- function(df) {
  df_dt <- as.data.table(df)
  
  df_dt[, `:=`(
    year = year(time_long),
    month = month(time_long),
    day = day(time_long),
    hour = hour(time_long)
  )]
  
  demand_levels <- c(
    "demand_less1kV_mw",
    "demand_1kV_14kV_mw",
    "demand_14kV_36kV_mw",
    "demand_36kV_72.5kV_mw",
    "demand_72.5kV_145kV_mw",
    "demand_145kV_220kV_mw",
    "demand_more220kV_mw"
  )
  
  updated_names <- sub("_mw", "_upd_mw", demand_levels)
  
  # Identify complete rows
  complete_filter <- rowSums(is.na(df_dt[, ..demand_levels])) == 0
  df_complete <- df_dt[complete_filter]
  
  # Precompute shares for each demand level
  for (col in demand_levels) {
    df_complete[[paste0("share_", col)]] <- df_complete[[col]] / df_complete$total_national_demand_mw
  }
  
  setkey(df_complete, year, month, day, hour)
  
  # Identify missing rows
  df_missing <- df_dt[!complete_filter]
  
  # Try shifts of 1 to 3 years
  results <- list()
  for (shift in 1:3) {
    df_missing[, ref_year := year - shift]
    merged <- merge(df_missing, df_complete,
                    by.x = c("ref_year", "month", "day", "hour"),
                    by.y = c("year", "month", "day", "hour"),
                    all.x = TRUE, suffixes = c("", "_ref"))
    merged[, shift := shift]
    results[[shift]] <- merged
  }
  
  # Combine all shifted matches
  combined <- rbindlist(results)
  setorder(combined, time_long, shift)  # Prefer smaller shifts
  combined_unique <- combined[, .SD[1], by = .(time_long)]  # Keep first match
  
  # Impute using precomputed shares
  combined_unique[, (updated_names) := {
    total <- total_national_demand_mw
    shares <- mget(paste0("share_", demand_levels))
    Map(function(share) share * total, shares)
  }]
  
  # Merge imputed values back into original dataset
  df_out <- merge(df_dt, combined_unique[, c("time_long", ..updated_names)], by = "time_long", all.x = TRUE)
  
  # Fill in imputed values only where needed
  for (i in seq_along(demand_levels)) {
    df_out[is.na(get(demand_levels[i])) & !is.na(get(updated_names[i])),
           (demand_levels[i]) := get(updated_names[i])]
  }
  
  # Cleanup
  cols_to_remove <- c("year", "month", "day", "hour", "ref_year", "shift", updated_names,
                      paste0("share_", demand_levels))
  cols_to_remove <- intersect(cols_to_remove, names(df_out))
  df_out[, (cols_to_remove) := NULL]
  
  return(as.data.frame(df_out))
}

full_dataset_imputed <- impute_demand_data_fast(full_dataset_imputed)

# Recompute the sum and gap columns to get 0 missings overall
full_dataset_imputed <- full_dataset_imputed %>% 
  mutate(sum_disaggregated_demand_mw = demand_less1kV_mw + demand_1kV_14kV_mw + demand_14kV_36kV_mw + 
           demand_36kV_72.5kV_mw + demand_72.5kV_145kV_mw + demand_145kV_220kV_mw + demand_more220kV_mw,
         gap_obs_demand_mw = total_national_demand_mw - sum_disaggregated_demand_mw)

colSums(is.na(full_dataset_imputed))


# For the FIRST version of our data, we disaggregate demand only between residential (less than 1kV) and industrial (over 1kV)
#full_dataset_imputed <- full_dataset_imputed %>% 
#  mutate(residential_demand_mw = demand_less1kV_mw,
#         industrial_demand_mw = demand_1kV_14kV_mw + demand_14kV_36kV_mw + demand_36kV_72.5kV_mw + 
#         demand_72.5kV_145kV_mw + demand_145kV_220kV_mw + demand_more220kV_mw)

# We observe a gap between the total national demand and the sum of the residential and industrial demand between 400 and 8000 MW          
# We will cover the gap by distributing it between the two sectors of demand, using the share of each sector in the total demand
#full_dataset_imputed <- full_dataset_imputed %>% 
#  mutate(share_res_demand_mw = residential_demand_mw / (residential_demand_mw + industrial_demand_mw),
#         share_ind_demand_mw = industrial_demand_mw / (residential_demand_mw + industrial_demand_mw),
#         residential_demand_mw = residential_demand_mw + gap_obs_demand_mw * share_res_demand_mw,
#         industrial_demand_mw = industrial_demand_mw + gap_obs_demand_mw * share_ind_demand_mw) 

# For the SECOND version of our data, we disaggregate demand between residential (less than 1kV), commercial (1-36kV) and industrial (over 36kV)
# Note that commercial may include some small industries
full_dataset_imputed <- full_dataset_imputed %>% 
  mutate(residential_demand_mw = demand_less1kV_mw,
         commercial_demand_mw = demand_1kV_14kV_mw + demand_14kV_36kV_mw,
         industrial_demand_mw = demand_36kV_72.5kV_mw + demand_72.5kV_145kV_mw + demand_145kV_220kV_mw + demand_more220kV_mw)

# Since disaggregated demand is retrieved for the peninsula, there is a gao with total demand
# We will cover the gap by distributing it between the two sectors of demand, using the share of each sector in the total demand
full_dataset_imputed <- full_dataset_imputed %>% 
  mutate(share_res_demand_mw = residential_demand_mw / (residential_demand_mw + commercial_demand_mw + industrial_demand_mw),
         share_com_demand_mw = commercial_demand_mw / (residential_demand_mw + commercial_demand_mw + industrial_demand_mw),
         share_ind_demand_mw = industrial_demand_mw / (residential_demand_mw + commercial_demand_mw + industrial_demand_mw),
         residential_demand_mw = residential_demand_mw + gap_obs_demand_mw * share_res_demand_mw,
         commercial_demand_mw = commercial_demand_mw + gap_obs_demand_mw * share_com_demand_mw,
         industrial_demand_mw = industrial_demand_mw + gap_obs_demand_mw * share_ind_demand_mw) 


# 3. Add hourly marginal data
# This data was downloaded by Tomás directly to an excel file.
# Maybe there is no possible automation for this step, but if there is, it would be better to do it.

# 3.1. Natural gas data (daily)
hourly_df <- data.frame(
  time_long = seq(
    from = as.POSIXct("2020-01-01 00:00:00", tz = "UTC"),
    to = as.POSIXct("2024-12-31 23:00:00", tz = "UTC"),
    by = "hour")) %>%
  mutate(year_month_day = as.Date(time_long),
         year_month = format(time_long, "%Y-%m"),
         year = format(time_long, "%Y"))

gas_price <- read_xlsx("Data/natural_gas_prices.xlsx") %>% 
  select(year_month_day = `Trading day`, cost_gas_eur_mwh = `Reference Price [EUR/MWh]`)

hourly_gas_price <- left_join(hourly_df, gas_price, by = "year_month_day") %>% 
  select(-year_month_day, -year_month, -year) 


# 3.2 Diesel and coal prices (monthly, data is saved as 2020-01-01, 2020-02-01, etc.)
diesel_coal_prices <- read_xlsx("Data/diesel_coal_prices.xlsx", sheet = "only_values") %>% 
  select(date, cost_coal_eur_mwh, cost_diesel_eur_mwh) %>% 
  mutate(year_month = format(date, "%Y-%m")) %>% 
  select(-date)

# Join the monthly data to the hourly data 
hourly_diesel_coal_prices <- left_join(hourly_df, diesel_coal_prices, by = "year_month") %>% 
  select(-year_month_day, -year_month, -year) 


# 3.3 Uranium prices (annualy)
uranium_price <- read_xlsx("Data/uranium_prices.xlsx", sheet = "only_values") %>% 
  select(year, cost_uranium_eur_mwh) %>% 
  mutate(year = as.character(year))

hourly_uranium_prices <- left_join(hourly_df, uranium_price, by = "year") %>% 
  select(-year_month_day, -year_month, -year) 


# 3.4 EU ETS auction prices (daily, only some days)
eu_ets_prices <- read_excel("Data/eu_ets_auction_prices.xlsx") %>% 
  select(year_month_day = Date, eu_ets_price_eur_tco2 = `Auction Price €/tCO2`)

hourly_eu_ets_prices <- left_join(hourly_df, eu_ets_prices, by = "year_month_day") %>% 
  select(-year_month_day, -year_month, -year) 

colSums(is.na(hourly_eu_ets_prices))

# Since we have data only in some "random" days, the other days take NA as value
# For now, let's just assign the price of the latest non-missing value to each missing hour
# Since the first day with data is 2020-01-07, we assume that the previous days the price was the same

# Find the first datetime with a non-missing value
first_non_na_time <- hourly_eu_ets_prices %>%
  filter(!is.na(eu_ets_price_eur_tco2)) %>%
  slice(1) %>%
  pull(time_long)

# Get the value to use for initial NAs
initial_value <- hourly_eu_ets_prices %>%
  filter(time_long == first_non_na_time) %>%
  pull(eu_ets_price_eur_tco2)

# Impute missing values: first fill down, then manually assign early NAs
hourly_eu_ets_prices <- hourly_eu_ets_prices %>%
  fill(eu_ets_price_eur_tco2, .direction = "down") %>%
  mutate(
    eu_ets_price_eur_tco2 = 
      if_else(
        time_long < first_non_na_time & is.na(eu_ets_price_eur_tco2),
        initial_value,
        eu_ets_price_eur_tco2
        )
    )

# Join all the fuel costs and eu ets prices hourly data
fuel_and_cabron_costs_data <- hourly_gas_price %>% 
  left_join(hourly_diesel_coal_prices) %>% 
  left_join(hourly_uranium_prices) %>% 
  left_join(hourly_eu_ets_prices)

colSums(is.na(fuel_and_cabron_costs_data))

# Visualize the evolution of fuel and carbon prices
fuel_carbon_prices_plot <- ggplot() +
  geom_line(data = fuel_and_cabron_costs_data, aes(x = time_long, y = cost_gas_eur_mwh, color = "Gas")) +
  geom_line(data = fuel_and_cabron_costs_data, aes(x = time_long, y = cost_coal_eur_mwh, color = "Coal")) +
  geom_line(data = fuel_and_cabron_costs_data, aes(x = time_long, y = cost_diesel_eur_mwh, color = "Diesel")) +
  geom_line(data = fuel_and_cabron_costs_data, aes(x = time_long, y = cost_uranium_eur_mwh, color = "Uranium")) +
  geom_line(data = fuel_and_cabron_costs_data, aes(x = time_long, y = eu_ets_price_eur_tco2, color = "EU ETS (CO2)")) +
  labs(
    x = "Time",
    y = "Price (€/MWh or €/tCO2)",
    color = "Legend"
  ) +
  scale_color_manual(values = c(
    "Gas" = "green",
    "Coal" = "grey70",
    "Diesel" = "red",
    "Uranium" = "turquoise",
    "EU ETS (CO2)" = "blue"
  )) + 
  theme_linedraw()
  
fuel_carbon_prices_plot

# cbind full_dataset_imputed with fuel_and_cabron_costs_data to avoid problems with the timestamp
fuel_and_cabron_costs_data_merge <- fuel_and_cabron_costs_data %>% select(-time_long)

full_dataset_imputed <- cbind(full_dataset_imputed, fuel_and_cabron_costs_data_merge)

# And create more readable time columns
full_dataset_imputed <- full_dataset_imputed %>%
  mutate(
    year = year(time_long),
    month = month(time_long),
    day = day(time_long),
    hour = hour(time_long)
  ) %>%
  select(time_long, year, month, day, hour, spot_price_eur_mwh, everything())

colSums(is.na(full_dataset_imputed))

write_xlsx(full_dataset_imputed, "Data/full_dataset_imputed.xlsx")  


# Finally, to get the first version of the data, we will select the columns that we want to keep for the Julia code, and order them as we want
data_version1 <- full_dataset_imputed 

# For now, I think it is better to retrieve imports and exports data from esios (interconnection variables)
# I will create new columns using ifelse
data_version1$imports_France_mw <- ifelse(data_version1$net_pen_interconnection_france_mw > 0,
                                          data_version1$net_pen_interconnection_france_mw, 0)
data_version1$exports_France_mw <- ifelse(data_version1$net_pen_interconnection_france_mw < 0,
                                          -data_version1$net_pen_interconnection_france_mw, 0)

data_version1$imports_Portugal_mw <- ifelse(data_version1$net_pen_interconnection_portugal_mw > 0,
                                            data_version1$net_pen_interconnection_portugal_mw, 0)
data_version1$exports_Portugal_mw <- ifelse(data_version1$net_pen_interconnection_portugal_mw < 0,
                                            -data_version1$net_pen_interconnection_portugal_mw, 0)

data_version1$imports_Morocco_mw <- ifelse(data_version1$net_pen_interconnection_morocco_mw > 0,
                                           data_version1$net_pen_interconnection_morocco_mw, 0)
data_version1$exports_Morocco_mw <- ifelse(data_version1$net_pen_interconnection_morocco_mw < 0,
                                           -data_version1$net_pen_interconnection_morocco_mw, 0)

# Note that now we save it as data_version2!!
data_version2 <- data_version1 %>% 
  select(time_long, year, month, day, hour,
         # Price
         spot_price_eur_mwh, 
         
         # Disaggregated demand
         residential_demand_mw, commercial_demand_mw, industrial_demand_mw,
         
         # Installed capacity
         coal_cap, combined_cycle_cap, gas_turbine_cap, vapor_turbine_cap, diesel_cap,
         nuclear_cap, hydro_cap, pumped_hydro_cap,
         solar_pv_cap, solar_thermal_cap, wind_cap, 
         other_renewables_cap, renewable_waste_cap, nonrenewable_waste_cap, cogeneration_cap,
         
         # Generation
         coal_gen_mw, combined_cycle_gen_mw, gas_turbine_gen_mw, vapor_turbine_gen_mw, diesel_gen_mw,
         nuclear_gen_mw, hydro_gen_mw,
         solar_pv_gen_mw, solar_thermal_gen_mw, wind_gen_mw, 
         renewable_thermal_gen_mw, renewable_waste_gen_mw = pen_renewable_waste_gen_mw, 
         nonrenewable_waste_gen_mw = pen_nonrenewable_waste_gen_mw, cogeneration_gen_mw = pen_cogeneration_gen_mw,
         auxiliary_generation_gen_mw,
         
         # Capacity factors
         nuclear_cap_factor, hydro_cap_factor,
         solar_pv_cap_factor, solar_thermal_cap_factor, wind_cap_factor,
         
         # Imports and exports
         imports_France_mw, exports_France_mw, net_flows_France_mw = net_pen_interconnection_france_mw,
         imports_Portugal_mw, exports_Portugal_mw, net_flows_Portugal_mw = net_pen_interconnection_portugal_mw,
         imports_Morocco_mw, exports_Morocco_mw, net_flows_Morocco_mw = net_pen_interconnection_morocco_mw,
         
         # Fuel costs and EU ETS prices
         cost_coal_eur_mwh, cost_gas_eur_mwh, cost_diesel_eur_mwh, cost_uranium_eur_mwh, eu_ets_price_eur_tco2
       )        

# Since many many names are wrong, I rename the variables that end with "_mw" to be "_mwh" and the variables that end with "_cap" to be "_cap_mw"
data_version2 <- data_version2 %>%
  rename_with(~ str_replace(., "_mw$", "_mwh")) %>%
  rename_with(~ str_replace(., "_cap$", "_cap_mw"))

colnames(data_version2)

colSums(is.na(data_version2))

write_xlsx(data_version2, "Data/data_version2.xlsx")

################################################################################
################################################################################
