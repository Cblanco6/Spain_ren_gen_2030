rm(list=ls())
library(httr2)
library(xml2)
library(tidyverse)
library(readxl)
library(writexl)

setwd("/Users/pauorive23/Desktop/Màster BSE/Thesis/Data")

base_url_esios <- "https://api.esios.ree.es/"
APIKey_esios <- "e5abc167c3936f4d527b6e44b07a748f8796092b5fc767a358a2d84784dc7050"

base_url_entsoe <- "https://web-api.tp.entsoe.eu/api"
APIKey_entsoe <- "0b312fe1-9d1a-4de1-a307-767ff7968c3c"

# ===== List of all esios indicators ===== 

request_indicators <- request(base_url_esios) %>% 
  req_url_path_append("indicators") %>% 
  req_headers(
    Accept = "application/json; application/vnd.esios-api-v1+json",
    `Content-Type` = "application/json",
    `x-api-key` = APIKey_esios
  ) %>% 
  req_perform() %>% # To actually perform the request
  resp_body_json() %>% # To keep the JSON of the "body" element of the list
  pluck("indicators") %>% 
  map_dfr(as_tibble) 

clean_description <- function(html_string) {
  html_string %>%
    str_replace_all("<p>", "\n\n") %>%
    str_replace_all("</p>", "") %>%
    str_replace_all("<b>", "**") %>%
    str_replace_all("</b>", "**")
}

clean_indicators <- request_indicators %>%
  mutate(description_clean = map_chr(description, clean_description)) %>% 
  select(id, name, description_clean)

# write_xlsx(clean_indicators, "esios_indicators_description.xlsx")

# Here we list all the indicators that we use with their code of the esios API (others are retrieved from entso-E)
id_to_name <- c(
  
  # Demand
  `1201` = "demand_less1kV",
  `1202` = "demand_1kV_14kV",
  `1203` = "demand_14kV_36kV",  
  `1204` = "demand_36kV_72.5kV", 
  `1205` = "demand_72.5kV_145kV", 
  `1206` = "demand_145kV_220kV",
  `1207` = "demand_more220kV",
  `2037` = "total_national_demand",
  
  # Installed capacity
  `1475` = "hydro_cap",
  `1476` = "pumped_hydro_cap",
  `1477` = "nuclear_cap",
  `1478` = "coal_cap",
  `1479` = "diesel_cap",
  `1480` = "gas_turbine_cap",
  `1481` = "vapor_turbine_cap",
  `1482` = "fuel_gas_cap",
  `1483` = "combined_cycle_cap",
  `1484` = "hydro_wind_cap",
  `1485` = "wind_cap",
  `1486` = "solar_pv_cap",
  `1487` = "solar_thermal_cap",
  `1488` = "other_renewables_cap",
  `1489` = "cogeneration_cap",
  `1490` = "nonrenewable_waste_cap",
  `1491` = "renewable_waste_cap",
  
  # Generation
  `2038` = "wind_gen",
  `2039` = "nuclear_gen",
  `2040` = "coal_gen",
  `2041` = "combined_cycle_gen",
  `2042` = "hydro_gen",
  `2044` = "solar_pv_gen",
  `2045` = "solar_thermal_gen",
  `2046` = "renewable_thermal_gen",
  `2047` = "diesel_gen",
  `2048` = "gas_turbine_gen",
  `2049` = "vapor_turbine_gen",
  `2050` = "auxiliary_generation_gen",
  `2051` = "cogeneration_waste_gen", # includes generation, non-renewable and renewable waste
  
  # Other hydro indicators (esios)
  `2065` = "pumped_hydro_gen", # Reliable but only data from 2025 onward
  `2066` = "turbine_hydro_gen",
  `2067` = "conventional_hydro_gen",
  `2078` = "pumped_hydro_gen_v2", # Also reliable but only data from 2025 onward
  `2079` = "turbine_hydro_gen_v2",
  `2080` = "conventional_hydro_gen_v2",
  `1` = "programmed_hydro_ugh", # These 3 are not very reliable
  `2` = "programmed_hydro_no_ugh",
  `3` = "programmed_pumped_hydro",
  
  # Other hydro indicators (entsoe)
  `B10` = "pumped_hydro_consumption_entsoe_mw",
  `B11` = "run_of_river_hydro_entsoe_mw",
  `B12` = "conventional_hydro_entsoe_mw",
  
  # Disaggregated cogeneration and waste (peninsular data)
  `10039` = "pen_cogeneration_gen",
  `10040` = "pen_nonrenewable_waste_gen",
  `10062` = "pen_renewable_waste_gen",

  # Nuclear and hydro self-reported available capacity
  `472` = "hydro_self_reported_cap",
  `474` = "nuclear_self_reported_cap", 

  # Batteries (only data for 18-31 November 2024)
  `2198` = "batteries_discharge",
  `2199` = "batteries_charge",

  # (Bilateral flows with the peninsula; 15 min data since 2022, but can retrieve hourly avg)
  `10207` = "net_pen_interconnection_france", 
  `10208` = "net_pen_interconnection_portugal",
  `10209` = "net_pen_interconnection_morocco",
  `10210` = "net_pen_interconnection_andorra", # I have removed Andorra since it does not work
  `2043` = "net_international_flows", 

  # (Peninsular data only; from 2024-11-01 onwards) 
  `10462` = "renewable_energy_curtailment" 
)

# Since we are retrieving several variables for 5 years of hourly data, the most efficient way is not too long 
# nested for loops with 5-10 variables and looping for every year in 2020:2024.

# Explained code with total demand 
# Until perform, these 12 lines of codes are just to define the URL 
demand_example <- request(base_url_esios) %>%  
  req_url_path_append("indicators", "1201") %>% # change id_num for the indicator of interest 
  req_headers(
    Accept = "application/json; application/vnd.esios-api-v1+json",
    `Content-Type` = "application/json",
    `x-api-key` = APIKey_esios
  ) %>% 
  req_url_query(
    start_date = "2020-01-01T00:00:00Z", # May try smaller periods to begin with
    end_date = "2021-01-01T00:00:00Z",
    time_trunc = "hour", 
    time_agg = "avg" # Data usually comes in MW every 5 mins, so we want to get the hourly average
  ) %>% 
  req_perform() %>% # This line is to actually perform the query
  resp_body_json() %>% # Obtain the json body of the httr request
  
  # Below, for other indicators we have to make sure that the way the json are structured is the same, otherwise edit parameters
  pluck("indicator", "values") %>% # Aim is to get the values, usually stored within the json list into indicators, and then values. Check it! 
  map_dfr(as_tibble) %>% # To convert all the lists of values into a tibble (more flexible than a data frame, but almost the same)
  mutate(time_long = ymd_hms(datetime), demand_mw = value) %>% # edit variables to make them more readable
  select(time_long, demand_mw) # Keep only time and value columns. 



# ===== 1. Demand indicators (hourly) ===== 

demand_ids <- as.character(c(1201:1207, 2037))

demand_data <- list()
years <- 2020:2024

for (id in demand_ids) {
  yearly_data <- list()
  
  for (year in years) {
    
    print(paste0("Retrieving ", id_to_name[[id]], " for year ", year))
    
    start_date <- paste0(year, "-01-01T00:00:00Z")
    end_date <- paste0(year, "-12-31T23:00:00Z")
    
    request <- request(base_url_esios) %>% 
      req_url_path_append("indicators", id) %>% 
      req_headers(
        Accept = "application/json; application/vnd.esios-api-v1+json",
        `Content-Type` = "application/json",
        `x-api-key` = APIKey_esios
      ) %>% 
      req_url_query(
        start_date = start_date,
        end_date = end_date,
        time_trunc = "hour",
        time_agg = "avg"
      ) %>% 
      req_perform() %>% 
      resp_body_json() %>% 
      pluck("indicator", "values") %>% 
      map_dfr(as_tibble) %>% 
      transmute(
        time_long = ymd_hms(datetime),
        !!paste0(id_to_name[[id]], "_mw") := value
      )
    
    yearly_data[[as.character(year)]] <- request
  }
  
  demand_data[[id]] <- bind_rows(yearly_data)
}

demand_data_raw <- reduce(demand_data, full_join, by = "time_long")

demand_dataset <- demand_data_raw %>% 
  mutate(sum_disaggregated_demand_mw = demand_less1kV_mw + demand_1kV_14kV_mw + demand_14kV_36kV_mw + 
           demand_36kV_72.5kV_mw + demand_72.5kV_145kV_mw + demand_145kV_220kV_mw + demand_more220kV_mw,
         gap_obs_demand_mw = total_national_demand_mw - sum_disaggregated_demand_mw)

# Check missing values
# There are 170 missings for total_national_demand, and around 2200 missings for each disaggregated demand, 
# since data is only available 8 months prior to extraction (therefore, until Aug 2024)
# 145-220kV is the most affected, with over 4000 missing values
colSums(is.na(demand_dataset))

# write_xlsx(demand_dataset, "demand_raw_data.xlsx")



# ===== 2. Installed capacity (monthly) ===== 

installed_cap_ids <- as.character(c(1475:1491))

installed_capacity_data <- list()
years <- 2020:2024

for (id in installed_cap_ids) {
  tech_data <- list()
  
  for (year in years) {
    print(paste0("Retrieving ", id_to_name[[id]], " for year ", year))
    
    start_date <- paste0(year, "-01-01T00:00:00Z")
    end_date <- paste0(year+1, "-01-01T00:00:00Z")
    
    data <- request(base_url_esios) %>%
      req_url_path_append("indicators", id) %>%
      req_headers(
        Accept = "application/json; application/vnd.esios-api-v1+json",
        `Content-Type` = "application/json",
        `x-api-key` = APIKey_esios
      ) %>%
      req_url_query(
        start_date = start_date,
        end_date = end_date,
        time_trunc = "month"
      ) %>%
      req_perform() %>%
      resp_body_json() %>%
      pluck("indicator", "values") %>%
      map_dfr(as_tibble) %>%
      mutate(year_month = floor_date(ymd_hms(datetime), unit = "month")) %>%  # force capacity to be monthly
      group_by(year_month) %>%
      summarise(!!paste0(id_to_name[[id]]) := sum(value, na.rm = TRUE), .groups = "drop")
    
    
    tech_data[[as.character(year)]] <- data
  }
  
  installed_capacity_data[[id]] <- bind_rows(tech_data)
}

installed_cap_dataset <- reduce(installed_capacity_data, full_join, by = "year_month")

# Save them in the order in which we are interested
# We are dismising the fuel_gas_cap (8 MW) and hydro_wind_cap (11 MW) since are negligible and not used in the model 
installed_cap_dataset <- installed_cap_dataset %>% 
  select(year_month, coal_cap, combined_cycle_cap, gas_turbine_cap, vapor_turbine_cap, diesel_cap,
         nuclear_cap, hydro_cap, pumped_hydro_cap, 
         solar_pv_cap, solar_thermal_cap, wind_cap, other_renewables_cap, 
         cogeneration_cap, nonrenewable_waste_cap, renewable_waste_cap) %>%
  mutate(cogeneration_waste_cap = cogeneration_cap + nonrenewable_waste_cap + renewable_waste_cap)       

# write_xlsx(installed_cap_dataset, "installed_cap_raw_data.xlsx")

# For the purposes of our dataset, we may have to create 1 row per each hour (think of repercussions for the MC simulations in the model)

## Alternative hourly_df (not developed in this code) ##
hourly_df <- data.frame(
  time_long = seq(
    from = as.POSIXct("2020-01-01 00:00:00", tz = "UTC"),
    to = as.POSIXct("2024-12-31 23:00:00", tz = "UTC"),
    by = "hour")) %>%
  mutate(year_month_day = as.Date(time_long),
         year_month = format(time_long, "%Y-%m"),
         year = format(time_long, "%Y"))

## Alternative hourly_df (not developed in this code) ##

start_time <- as.POSIXct("2020-01-01 00:00:00", tz = "UTC")
end_time <- as.POSIXct("2024-12-31 23:59:59", tz = "UTC")
full_time_sequence <- seq(from = start_time, to = end_time, by = "month")
full_time_df <- data.frame(time_long = full_time_sequence)

# Add the monthly data (assuming it also has a year-month column) 
# Create a matching column in your installed_cap_dataset
installed_cap_dataset <- installed_cap_dataset %>%
  mutate(year_month = seq(from = as.Date("2020-01-01"),
                          by = "month",
                          length.out = 60))

start_time <- as.POSIXct("2020-01-01 00:00:00", tz = "UTC")
end_time <- as.POSIXct("2025-01-01 00:00:00", tz = "UTC")
full_time_sequence <- seq(from = start_time, to = end_time, by = "hour")
full_time_df <- data.frame(time_long = full_time_sequence)

# Convert time_long to year_month format in full_time_df
full_time_df$year_month <- (format(full_time_df$time_long, "%Y-%m"))
installed_cap_dataset$year_month <- format(installed_cap_dataset$year_month, "%Y-%m")

# Join the monthly data to the hourly data
hourly_installed_cap_dataset <- full_time_df %>%
  left_join(installed_cap_dataset, by = "year_month") %>% 
  select(-year_month)

# Check missing values
colSums(is.na(hourly_installed_cap_dataset))

# write_xlsx(hourly_installed_cap_dataset, "hourly_installed_cap_dataset.xlsx")



# ===== 3.1 Real time Generation (hourly) ===== 

generation_ids <- as.character(c(2038:2042, 2044:2051))

real_time_generation_data <- list()
years <- 2020:2024

for (id in generation_ids) {
  yearly_data <- list()
  
  for (year in years) {
    
    print(paste0("Retrieving ", id_to_name[[id]], " for year ", year))
    
    start_date <- paste0(year, "-01-01T00:00:00Z")
    end_date <- paste0(year, "-12-31T23:00:00Z")
  
    request <- request(base_url_esios) %>% 
      req_url_path_append("indicators", id) %>% 
      req_headers(
        Accept = "application/json; application/vnd.esios-api-v1+json",
        `Content-Type` = "application/json",
        `x-api-key` = APIKey_esios
      ) %>% 
      req_url_query(
        start_date = start_date,
        end_date = end_date,
        time_trunc = "hour",
        time_agg = "avg"
      ) %>% 
      req_perform() %>% 
      resp_body_json() %>% 
      pluck("indicator", "values") %>% 
      map_dfr(as_tibble) %>% 
      transmute(
        time_long = ymd_hms(datetime),
        !!paste0(id_to_name[[id]], "_mw") := value
      )
    
    yearly_data[[as.character(year)]] <- request
  }
  
  real_time_generation_data[[id]] <- bind_rows(yearly_data)
}

generation_data_raw <- reduce(real_time_generation_data, full_join, by = "time_long")

generation_dataset <- generation_data_raw

# Check missing values
colSums(is.na(generation_dataset))

start_time <- as.POSIXct("2020-01-01 00:00:00", tz = "UTC")
end_time <- as.POSIXct("2024-12-31 23:59:59", tz = "UTC")
full_time_sequence <- seq(from = start_time, to = end_time, by = "hour")
full_time_df <- data.frame(time_long = full_time_sequence)

generation_dataset <- full_time_df %>%
  left_join(generation_dataset, by = "time_long")

# We now have 170 missing values for the generation dataset.
colSums(is.na(generation_dataset))



# ===== 3.2 Trying to improve waste and cogeneration indicators (hourly) ===== 

# I have found peninsular data for cogeneration, nonrenewable and renewable waste. 
# We need geo_agg parameter, which does not work in the previous loop, and loop over months
cogen_waste_separate_ids <- as.character(c(10039, 10040, 10062))

cogen_waste_separate_data <- list()
years <- 2020:2024
months <- 1:12

for (id in cogen_waste_separate_ids) {
  monthly_data <- list()
  
  for (year in years) {
    for (month in months) {
      print(paste0("Retrieving ", id_to_name[[id]], " for ", year, " ", month))
      
      start_date <- paste0(year, "-", month, "-01T00:00:00Z")
      end_date <- ceiling_date(as.Date(start_date), "month") 
      
      request <- request(base_url_esios) %>% 
        req_url_path_append("indicators", id) %>% 
        req_headers(
          Accept = "application/json; application/vnd.esios-api-v1+json",
          `Content-Type` = "application/json",
          `x-api-key` = APIKey_esios
        ) %>% 
        req_url_query(
          start_date = start_date,
          end_date = end_date,
          geo_agg = "sum"
        ) %>% 
        req_perform() %>% 
        resp_body_json() %>% 
        pluck("indicator", "values") %>% 
        map_dfr(as_tibble) %>% 
        group_by(datetime) %>% 
        summarise(value = mean(value), .groups = "drop") %>% 
        transmute(
          time_long = ymd_hms(datetime),
          !!paste0(id_to_name[[id]], "_mw") := value
        )
      monthly_data[[paste0(year, "-", sprintf("%02d", month))]] <- request
      
    }
  }
  
  cogen_waste_separate_data[[id]] <- bind_rows(monthly_data)
}

cogen_waste_separate_data_raw <- reduce(cogen_waste_separate_data, full_join, by = "time_long")

full_dataset <- read_xlsx("full_dataset.xlsx") %>% 
  select(-pen_cogeneration_gen_mw, -pen_nonrenewable_waste_gen_mw, -pen_renewable_waste_gen_mw)


# This should be already fixed in the code, but the first time I ran the code it downloaded twice the first hour of every month
#cogen_waste_separate_data_raw <- cogen_waste_separate_data_raw %>%
#  distinct(time_long, .keep_all = TRUE)

# cogen_waste_separate_data_raw <- cogen_waste_separate_data_raw[-nrow(cogen_waste_separate_data_raw), ]

colSums(is.na(cogen_waste_separate_data_raw))

# Merge the cogeneration and waste data into the main generation dataset
generation_dataset <- generation_dataset %>%
  left_join(cogen_waste_separate_data_raw, by = "time_long") 



# ===== 3.3 Trying to improve hydro indicators - esios (hourly) ===== 

# I have found additional indicators that may distinguish between pumped hydro and conventional hydro
# We need geo_agg parameter, which does not work in the previous loop, and loop over months
hydro_gen_old <- read_xlsx("data_version2.xlsx") %>% 
  filter(year !=2023 & year!= 2024) %>% 
  select(time_long, hydro_gen_mwh)


new_hydro_ids <- as.character(c(2065:2067)) 
new_hydro_2_ids <- as.character(c(2078:2080)) 
old_hydro <- as.character(2042)
planned_hydro <- as.character(c(1:3)) 

new_hydro_data <- list()
years <- 2025#:2024
months <- 1:4

for (id in planned_hydro) {
  monthly_data <- list()
  
  for (year in years) {
    for (month in months) {
      print(paste0("Retrieving ", id_to_name[[id]], " for ", year, " ", month))
      
      start_date <- paste0(year, "-", month, "-01T00:00:00Z")
      end_date <- ceiling_date(as.Date(start_date), "month") 
      
      request <- request(base_url_esios) %>% 
        req_url_path_append("indicators", id) %>% 
        req_headers(
          Accept = "application/json; application/vnd.esios-api-v1+json",
          `Content-Type` = "application/json",
          `x-api-key` = APIKey_esios
        ) %>% 
        req_url_query(
          start_date = start_date,
          end_date = end_date,
          time_trunc = "hour",
          time_agg = "avg",
          geo_agg = "sum"
        ) %>% 
        req_perform() %>% 
        resp_body_json() %>% 
        pluck("indicator", "values") %>% 
        map_dfr(as_tibble) %>% 
        group_by(tz_time) %>% 
        summarise(value = mean(value), .groups = "drop") %>% 
        transmute(
          time_long = ymd_hms(tz_time),
          !!paste0(id_to_name[[id]], "_mw") := value
        )
      monthly_data[[paste0(year, "-", sprintf("%02d", month))]] <- request
      
    }
  }
  
  new_hydro_data[[id]] <- bind_rows(monthly_data)
}

# new_hydro_data_raw <- reduce(new_hydro_data, full_join, by = "time_long")
# new_hydro_data_raw2 <- reduce(new_hydro_data, full_join, by = "time_long") 
# old_hydro <- reduce(new_hydro_data, full_join, by = "time_long")
planned_hydro <- reduce(new_hydro_data, full_join, by = "time_long")

start_time <- as.POSIXct("2025-01-01 00:00:00", tz = "UTC")
end_time <- as.POSIXct("2025-04-30 23:00:00", tz = "UTC")

time_long <- as.data.frame(seq(from = start_time, to = end_time, by = "hour"))
colnames(time_long) <- "time_long"

# Merge all esios hydro indicators to check which ones are reliable
full_hydro_2025 <- time_long %>%
  left_join(old_hydro) %>% 
  left_join(new_hydro_data_raw) %>% 
  left_join(new_hydro_data_raw2) %>% 
  left_join(planned_hydro) %>% 
  mutate(total_programmed_hydro_not_pumped = programmed_hydro_ugh_mw + programmed_hydro_no_ugh_mw,
         total_programmed_hydro = programmed_hydro_ugh_mw + programmed_hydro_no_ugh_mw + programmed_pumped_hydro_mw,
         total_hydro_v1_not_pumped = turbine_hydro_gen_mw + conventional_hydro_gen_mw,
         total_hydro_v2_not_pumped = turbine_hydro_gen_v2_mw + conventional_hydro_gen_v2_mw,
         total_hydro_v1 = pumped_hydro_gen_mw + turbine_hydro_gen_mw + conventional_hydro_gen_mw,
         total_hydro_v2 = pumped_hydro_gen_v2_mw + turbine_hydro_gen_v2_mw + conventional_hydro_gen_v2_mw) %>% 
  select(time_long, pumped_hydro_gen_mw, pumped_hydro_gen_v2_mw, programmed_pumped_hydro_mw,
         turbine_hydro_gen_mw, turbine_hydro_gen_v2_mw,
         conventional_hydro_gen_mw, conventional_hydro_gen_v2_mw, programmed_hydro_ugh_mw, programmed_hydro_no_ugh_mw, 
         old_hydro_indicator = hydro_gen_mw, 
         total_programmed_hydro_not_pumped, total_hydro_v1_not_pumped, total_hydro_v2_not_pumped,
         total_programmed_hydro, total_hydro_v1, total_hydro_v2,
         old_hydro_indicator_again = hydro_gen_mw)


colnames(full_hydro_2025)

# write_xlsx(full_hydro_2025, "all_esios_hydro_indicators_2025.xlsx")


# ===== 3.4 Trying to improve hydro indicators - entso-e (hourly) ===== 

# Let's try with entsoe data:
# As with the imports and exports indicator to France, at a certain moment in time data changes from 60min frequency to 15 min
# In this case it is between May 23rd and 24th of 2022

entsoe_hydro_ids <- c("B10", "B11", "B12")
entsoe_hydro_60 <- list()
years <- 2020:2022

Sys.setenv(CURL_SSL_BACKEND = "openssl") 

for (id in entsoe_hydro_ids) {
  yearly_data <- list()
  
  for (year in years) {
    message("Getting ", id_to_name[[id]], " in ", year, " (60min data)")
    
    start_date <- paste0(year, "01010000")
    end_date <- if (year == 2022) paste0(year, "05230100") else paste0(year + 1, "01010000")
    
    response <- request(base_url_entsoe) %>%
      req_url_query(
        securityToken = APIKey_entsoe,
        documentType = "A75",
        processType = "A16",
        in_Domain = "10YES-REE------0",
        periodStart = start_date,
        periodEnd = end_date,
        psrType = id
      ) %>%
      req_perform()
    
    request_xml <- resp_body_xml(response)
    
    ns <- c(ns = "urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0")
    
    quantities <- xml_find_all(request_xml, ".//ns:Point/ns:quantity", ns) %>% xml_double()

    yearly_data[[as.character(year)]] <- quantities
  }
  
  indicator <- id_to_name[[id]]
  entsoe_hydro_60[[indicator]] <- unlist(yearly_data)
}

# Merging the 60-min data (more complex than with esios data since I am not retrieving times tamp here)

# Step 1: Merge each indicator's yearly data into one vector
entsoe_hydro_60_merged <- list()

for (indicator in names(entsoe_hydro_60)) {
  merged_data <- unlist(entsoe_hydro_60[[indicator]], use.names = FALSE)
  entsoe_hydro_60_merged[[indicator]] <- merged_data
}

# Step 2: Build the datetime sequence
# Assumption: All indicators cover exactly the same time span
total_points <- length(entsoe_hydro_60_merged[[1]])  # Use any indicator

start_datetime <- ymd_hms("2020-01-01 00:00:00")  # Adjust if needed
datetime <- start_datetime + hours(0:(total_points-1))

# Step 3: Build a single dataframe with datetime + all 3 indicators
entsoe_hydro_60_data <- tibble(
  time_long = datetime,
  pumped_hydro_consumption_entsoe_mw = entsoe_hydro_60_merged[["pumped_hydro_consumption_entsoe_mw"]],
  run_of_river_hydro_entsoe_mw = entsoe_hydro_60_merged[["run_of_river_hydro_entsoe_mw"]],
  conventional_hydro_entsoe_mw = entsoe_hydro_60_merged[["conventional_hydro_entsoe_mw"]]
  )

entsoe_hydro_60_data <- entsoe_hydro_60_data %>% slice(1:(n() - 3))

hydro_gen_old <- read_xlsx("data_version2.xlsx") %>% 
  filter(year !=2023 & year!= 2024) %>% 
  select(time_long, hydro_gen_mwh)

compare_hydro_ids <- entsoe_hydro_60_data %>% 
  left_join(hydro_gen_old) %>% 
  mutate(total_hydro_not_pumped = run_of_river_hydro_entsoe_mw + conventional_hydro_entsoe_mw,
         total_hydro1 = total_hydro_not_pumped + pumped_hydro_consumption_entsoe_mw,
         total_hydro2 = total_hydro_not_pumped - pumped_hydro_consumption_entsoe_mw,
         dif_hydro = hydro_gen_mwh - total_hydro2)
  
colnames(compare_hydro_ids)


# Repeat the same process for the 15 minute data
# From the entso-e webpage we also know that:

# Until 2022:
# Pumped Storage actual Consumption is included in “Hydro Pumped Storage”, so in B10
# Pumped Storage actual Generation is included in “Hydro Water Reservoir”, so in B12

# From 12/12/2022:
# Pumped Storage Actual Consumption and Generation is showed separately in “Hydro Pumped Storage”

entsoe_hydro_ids <- c("B10", "B11", "B12")
entsoe_hydro_15_until_20221212 <- list()

Sys.setenv(CURL_SSL_BACKEND = "openssl") 

for (id in entsoe_hydro_ids) {
  message("Getting ", id_to_name[[id]], " in 2022 (15min data)")
  
  start_date <- "202205230000"
  end_date <- if (id == "B10") "202212112330" else "202212120000"
  
  response <- request(base_url_entsoe) %>%
    req_url_query(
      securityToken = APIKey_entsoe,
      documentType = "A75",
      processType = "A16",
      in_Domain = "10YES-REE------0",
      periodStart = start_date,
      periodEnd = end_date,
      psrType = id
    ) %>%
    req_perform()
  
  request_xml <- resp_body_xml(response)
  
  ns <- c(ns = "urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0")
  
  quantities <- xml_find_all(request_xml, ".//ns:Point/ns:quantity", ns) %>% xml_double()
  
  indicator <- id_to_name[[id]]
  entsoe_hydro_15_until_20221212[[indicator]] <- quantities
}

# Create a function to convert 15-minute data to 1-hour data (not sure if can directly retrieve hourly data from entso-E)
average_every_4 <- function(x) {
  n <- length(x)
  m <- floor(n / 4)
  means <- sapply(1:m, function(i) mean(x[((i - 1) * 4 + 1):(i * 4)], na.rm = TRUE))
  return(means)
}

# Step 1: Merge each indicator's yearly data into one vector
entsoe_hydro_15_merged_1 <- list()

for (indicator in names(entsoe_hydro_15_until_20221212)) {
  raw_15min_data <- unlist(entsoe_hydro_15_until_20221212[[indicator]], use.names = FALSE)
  hourly_data <- average_every_4(raw_15min_data)
  entsoe_hydro_15_merged_1[[indicator]] <- hourly_data
}

# Step 2: Build the datetime sequence
# Assumption: All indicators cover exactly the same time span
total_points <- length(entsoe_hydro_15_merged_1[[1]])  # Use any indicator

start_datetime <- ymd_hms("2022-05-23 00:00:00")  
datetime <- start_datetime + hours(0:(total_points - 1))

for (indicator in names(entsoe_hydro_15_merged_1)) {
  cat(indicator, "\n")
  cat("  Raw 15min length: ", length(entsoe_hydro_15_until_20221212[[indicator]]), "\n")
  cat("  Hourly length:    ", length(entsoe_hydro_15_merged_1[[indicator]]), "\n\n")
}

# Step 3: Build a single dataframe with datetime + all 3 indicators
entsoe_hydro_15_data_1 <- tibble(
  time_long = datetime,
  pumped_hydro_consumption_entsoe_mw = entsoe_hydro_15_merged_1[["pumped_hydro_consumption_entsoe_mw"]],
  run_of_river_hydro_entsoe_mw = entsoe_hydro_15_merged_1[["run_of_river_hydro_entsoe_mw"]],
  conventional_hydro_entsoe_mw = entsoe_hydro_15_merged_1[["conventional_hydro_entsoe_mw"]]
)


# As mentioned above, since 2022/12/12 pumped hydro has separate indicators for consumption and generation. 
# Whenever a request is made, the first half corresponds to pumped hydro generation, and the second to pumped hydro consumption.
# To make things easy, I will first retrieve data for B11 and B12, and then do specific runs of code for each year for pumped hydro

entsoe_hydro_ids <- c("B11", "B12")
entsoe_hydro_15_not_pumped_since_20221212 <- list()
years <- 2022:2024

Sys.setenv(CURL_SSL_BACKEND = "openssl") 

for (id in entsoe_hydro_ids) {
  yearly_data <- list()
  
  for (year in years) {
    message("Getting ", id_to_name[[id]], " in ", year, " (60min data)")
    
    start_date <- if (year == 2022) paste0(year, "12120000") else paste0(year, "01010000")
    end_date <- if (year == 2024) paste0(year+1, "01010200") else paste0(year+1, "01010000")
    
    response <- request(base_url_entsoe) %>%
      req_url_query(
        securityToken = APIKey_entsoe,
        documentType = "A75",
        processType = "A16",
        in_Domain = "10YES-REE------0",
        periodStart = start_date,
        periodEnd = end_date,
        psrType = id
      ) %>%
      req_perform()
    
    request_xml <- resp_body_xml(response)
    
    ns <- c(ns = "urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0")
    
    quantities <- xml_find_all(request_xml, ".//ns:Point/ns:quantity", ns) %>% xml_double()
    
    yearly_data[[as.character(year)]] <- quantities
  }
  
  indicator <- id_to_name[[id]]
  entsoe_hydro_15_not_pumped_since_20221212[[indicator]] <- unlist(yearly_data)
}

# Step 1: Merge each indicator's yearly data into one vector
entsoe_hydro_15_merged_2 <- list()

for (indicator in names(entsoe_hydro_15_not_pumped_since_20221212)) {
  raw_15min_data <- unlist(entsoe_hydro_15_not_pumped_since_20221212[[indicator]], use.names = FALSE)
  hourly_data <- average_every_4(raw_15min_data)
  entsoe_hydro_15_merged_2[[indicator]] <- hourly_data
}

# Step 2: Build the datetime sequence
# Assumption: All indicators cover exactly the same time span
total_points <- length(entsoe_hydro_15_merged_2[[1]])  # Use any indicator

start_datetime <- ymd_hms("2022-12-12 00:00:00")  
datetime <- start_datetime + hours(0:(total_points - 1))

for (indicator in names(entsoe_hydro_15_merged_2)) {
  cat(indicator, "\n")
  cat("  Raw 15min length: ", length(entsoe_hydro_15_not_pumped_since_20221212[[indicator]]), "\n")
  cat("  Hourly length:    ", length(entsoe_hydro_15_merged_2[[indicator]]), "\n\n")
}

# Step 3: Build a single dataframe with datetime + all 3 indicators
entsoe_hydro_15_data_2 <- tibble(
  time_long = datetime,
  run_of_river_hydro_entsoe_mw = entsoe_hydro_15_merged_2[["run_of_river_hydro_entsoe_mw"]],
  conventional_hydro_entsoe_mw = entsoe_hydro_15_merged_2[["conventional_hydro_entsoe_mw"]]
)


# Finally, I get data on pumped hydro. I do each request separately to then split the list into 2

entsoe_pumped_hydro_since_20221212 <- tibble()

start_dates <- c("202212120000", "202301010000", "202401010000")
end_dates <- c("202301010000", "202401010100", "202501010100")

for (i in seq_along(start_dates)) {
  message("Processing period ", i, ": ", start_dates[i], " to ", end_dates[i])
  
  response <- request(base_url_entsoe) %>%
    req_url_query(
      securityToken = APIKey_entsoe,
      documentType = "A75",
      processType = "A16",
      in_Domain = "10YES-REE------0",
      periodStart = start_dates[i],
      periodEnd = end_dates[i],
      psrType = "B10"
    ) %>%
    req_perform()
  
  request_xml <- resp_body_xml(response)
  
  ns <- c(ns = "urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0")
  
  quantities <- xml_find_all(request_xml, ".//ns:Point/ns:quantity", ns) %>% xml_double()
  
  n <- length(quantities)
  half <- n / 2
  
  generation_vec <- quantities[1:half]
  consumption_vec <- quantities[(half + 1):n]
  
  generation_avg <- average_every_4(generation_vec)
  consumption_avg <- average_every_4(consumption_vec)
  
  len <- min(length(generation_avg), length(consumption_avg))
  
  start_datetime <- ymd_hm(start_dates[i])
  time_long <- start_datetime + hours(0:(len - 1))
  
  df_period <- tibble(
    time_long = time_long,
    pumped_hydro_gen_entsoe_mw = generation_avg[1:len],
    pumped_hydro_consumption_entsoe_mw = consumption_avg[1:len]
  )
  
  entsoe_pumped_hydro_since_20221212 <- bind_rows(entsoe_pumped_hydro_since_20221212, df_period)
}

# Now, time to merge !!
# Note that the column of pumped_hydro_consumption_mw was present since the beginning while the other one appears only in the end.
# Therefore, I first add an empty column for pumped_hydro_gen_entsoe_mw

all_entsoe_hydro_60_data <- entsoe_hydro_60_data %>% 
  rbind(entsoe_hydro_15_data_1) %>% 
  mutate(pumped_hydro_gen_entsoe_mw = 0)

all_entsoe_hydro_15_data <- entsoe_hydro_15_data_2 %>% 
  left_join(entsoe_pumped_hydro_since_20221212) 

all_entsoe_hydro_data <- rbind(all_entsoe_hydro_60_data, all_entsoe_hydro_15_data)

colSums(is.na(all_entsoe_hydro_data))

hydro_gen_old <- read_xlsx("data_version2.xlsx") %>% 
  select(time_long, hydro_gen_mwh)

compare_hydro_entsoe <- all_entsoe_hydro_data %>% 
  left_join(hydro_gen_old) %>% 
  mutate(total_hydro_not_pumped = run_of_river_hydro_entsoe_mw + conventional_hydro_entsoe_mw,
         total_hydro1 = total_hydro_not_pumped + pumped_hydro_consumption_entsoe_mw + pumped_hydro_gen_entsoe_mw,
         total_hydro2 = total_hydro_not_pumped - pumped_hydro_consumption_entsoe_mw + pumped_hydro_gen_entsoe_mw,
         dif_hydro = hydro_gen_mwh - total_hydro2)

# Merge the cogeneration and waste data into the main generation dataset
generation_dataset <- generation_dataset %>%
  left_join(all_entsoe_hydro_data, by = "time_long") 

# ====== 3.5 Order the columns the generation dataset ======

generation_dataset <- generation_dataset %>% 
  select(time_long, coal_gen_mw, combined_cycle_gen_mw, gas_turbine_gen_mw, vapor_turbine_gen_mw, diesel_gen_mw,
         nuclear_gen_mw, hydro_gen_mw, conventional_hydro_entsoe_mw, run_of_river_hydro_entsoe_mw,
         pumped_hydro_gen_entsoe_mw, pumped_hydro_consumption_entsoe_mw,
         solar_pv_gen_mw, solar_thermal_gen_mw, wind_gen_mw,
         renewable_thermal_gen_mw, auxiliary_generation_gen_mw,
         pen_cogeneration_gen_mw, pen_nonrenewable_waste_gen_mw, pen_renewable_waste_gen_mw, cogeneration_waste_gen_mw) %>%  
  mutate(total_pen_cog_waste_gen_mw = pen_cogeneration_gen_mw + pen_nonrenewable_waste_gen_mw + pen_renewable_waste_gen_mw,
         gap_data_cogen_waste_gen_mw = cogeneration_waste_gen_mw - total_pen_cog_waste_gen_mw,
         total_generation_mw = coal_gen_mw + combined_cycle_gen_mw + gas_turbine_gen_mw + vapor_turbine_gen_mw + diesel_gen_mw +
           nuclear_gen_mw + hydro_gen_mw + solar_pv_gen_mw + solar_thermal_gen_mw + wind_gen_mw +
           renewable_thermal_gen_mw + total_pen_cog_waste_gen_mw + auxiliary_generation_gen_mw)

# Check missing values
colSums(is.na(generation_dataset))

# For our purposes, I save all the installed capacity and generation data into the same dataset
ins_cap_gen_dataset <- hourly_installed_cap_dataset %>% 
  left_join(generation_dataset) %>% 
  mutate(solar_pv_cap_factor = solar_pv_gen_mw / solar_pv_cap,
         solar_thermal_cap_factor = solar_thermal_gen_mw / solar_thermal_cap,
         wind_cap_factor = wind_gen_mw / wind_cap) %>% 
  mutate(across(starts_with("coal_gen_mw"):starts_with("pen_renewable_waste_gen_mw"), 
                ~ . / total_generation_mw, 
                .names = "share_{.col}")) %>% 
  rename_with(~ str_replace(., "_mw$", ""), 
    .cols = starts_with("share_"))

# Using PNIEC taxonomy (see details in the Excel) we can compute renewable and non-renewable shares
ins_cap_gen_dataset <- ins_cap_gen_dataset %>% 
  mutate(renewable_share = share_hydro_gen + share_solar_pv_gen + share_solar_thermal_gen + 
           share_wind_gen + share_renewable_thermal_gen + share_pen_renewable_waste_gen,
         nonrenewable_share = share_coal_gen + share_combined_cycle_gen + share_gas_turbine_gen + 
           share_vapor_turbine_gen + share_diesel_gen + share_nuclear_gen + share_auxiliary_generation_gen +
           share_pen_cogeneration_gen + share_pen_nonrenewable_waste_gen)


colSums(is.na(ins_cap_gen_dataset))

# write_xlsx(ins_cap_gen_dataset, "ins_cap_gen_raw_data.xlsx")


# ===== 4.1 Imports and exports entso-e indicators (hourly) ===== 

bilateral_flows_data <- list()

# Spain and neighboring countries (ENTSO-E domain codes)
# For now just France and Portugal, since data for Morocco does not work.

# Getting this data is a little bit tricky, since although Portugal has hourly data throughout the 5 years, 
# France changes to 15-min granularity the 26th of March. Therefore, we must take this into account. 
# To make thinks easy, I just get each data separately, and then merge it all together.

pair_ESP_POR <- c(
  "10YES-REE------0", # Spain
  "10YPT-REN------W"  # Portugal
)
names(pair_ESP_POR) <- c("Spain", "Portugal")

imp_exp_ESP_POR <- list()
years <- 2020:2024

for (year in years) {
  for (i in 1:2) {
    
    if (i == 1) {
      out_Domain <- pair_ESP_POR["Spain"]
      in_Domain <- pair_ESP_POR["Portugal"]
      flow_type <- "Exports from Spain to Portugal"
    } else {
      out_Domain <- pair_ESP_POR["Portugal"]
      in_Domain <- pair_ESP_POR["Spain"]
      flow_type <- "Exports from Portugal to Spain"
    }
    
    print(paste0("Getting ", flow_type, " in ", year))
    
    start_date <- paste0(year, "01010000")
    end_date <- paste0(year + 1, "01010000")
    
    response <- request(base_url_entsoe) %>%
      req_url_query(
        securityToken = APIKey_entsoe,
        documentType = "A11",
        in_Domain = in_Domain,
        out_Domain = out_Domain,
        periodStart = start_date,
        periodEnd = end_date
      ) %>%
      req_perform()
    
    request_xml <- resp_body_xml(response)
    
    ns <- xml_ns(request_xml)
    ns <- c(ns, ns = "urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:0")
    
    flow_data <- xml_find_all(request_xml, ".//ns:Point/ns:quantity", ns) %>%
      xml_double()
    
    flow_name <- paste(year, names(pair_ESP_POR)[which(pair_ESP_POR == out_Domain)], 
                       "to", names(pair_ESP_POR)[which(pair_ESP_POR == in_Domain)])
    imp_exp_ESP_POR[[flow_name]] <- flow_data
  }
}

# Separate export and import vectors
exp_POR_list <- imp_exp_ESP_POR[grep("Spain to Portugal", names(imp_exp_ESP_POR))]
imp_POR_list <- imp_exp_ESP_POR[grep("Portugal to Spain", names(imp_exp_ESP_POR))]

# Combine them into single vectors (respecting year order)
exp_POR <- unlist(exp_POR_list, use.names = FALSE)
imp_POR <- unlist(imp_POR_list, use.names = FALSE)

# Create the final data frame
bilateral_flows_ESP_POR <- data.frame(
  time_long = hourly_dates,
  imp_POR_mw = imp_POR,
  exp_POR_mw = exp_POR,
  net_imp_POR_mw = imp_POR - exp_POR
)

# For France we have to make two separate queries, one before the data changed to 15-min and one after that
pair_ESP_FRA <- c(
  "10YES-REE------0", # Spain
  "10YFR-RTE------C"  # France
)
names(pair_ESP_FRA) <- c("Spain", "France")

imp_exp_ESP_FRA_60 <- list()
years <- 2020:2023

for (year in years) {
  for (i in 1:2) {
    
    if (i == 1) {
      out_Domain <- pair_ESP_FRA["Spain"]
      in_Domain <- pair_ESP_FRA["France"]
      flow_type <- "Exports from Spain to France"
    } else {
      out_Domain <- pair_ESP_FRA["France"]
      in_Domain <- pair_ESP_FRA["Spain"]
      flow_type <- "Exports from France to Spain"
    }
    
    print(paste0("Getting ", flow_type, " in ", year, " (60min data)"))
    
    start_date <- paste0(year, "01010000")
    end_date <- paste0(year + 1, "01010000")
    
    if (year == 2023) {
      start_date <- paste0(year, "01010000")
      end_date <- paste0(year, "03270000") # In the last 5 days of March 2023, the data already changes to 15 min 
    }
    
    response <- request(base_url_entsoe) %>%
      req_url_query(
        securityToken = APIKey_entsoe,
        documentType = "A11",
        in_Domain = in_Domain,
        out_Domain = out_Domain,
        periodStart = start_date,
        periodEnd = end_date
      ) %>%
      req_perform()
    
    request_xml <- resp_body_xml(response)
    
    ns <- xml_ns(request_xml)
    ns <- c(ns, ns = "urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:0")
    
    flow_data <- xml_find_all(request_xml, ".//ns:Point/ns:quantity", ns) %>%
      xml_double()
    
    flow_name <- paste(year, names(pair_ESP_FRA)[which(pair_ESP_FRA == out_Domain)], 
                       "to", names(pair_ESP_FRA)[which(pair_ESP_FRA == in_Domain)])
    imp_exp_ESP_FRA_60[[flow_name]] <- flow_data
  }
}

# Calculate the length of data in FRA_60
length_FRA_60 <- sum(sapply(imp_exp_ESP_FRA_60, length)) / 2  # divide by 2 because each year has imports and exports

# Subset hourly_dates accordingly
hourly_dates_FRA_60 <- hourly_dates[1:length_FRA_60]

# Separate export and import vectors
exp_FRA_60_list <- imp_exp_ESP_FRA_60[grep("Spain to France", names(imp_exp_ESP_FRA_60))]
imp_FRA_60_list <- imp_exp_ESP_FRA_60[grep("France to Spain", names(imp_exp_ESP_FRA_60))]

# Combine them into single vectors (respecting year order)
exp_FRA_60 <- unlist(exp_FRA_60_list, use.names = FALSE)
imp_FRA_60 <- unlist(imp_FRA_60_list, use.names = FALSE)

# Check lengths against the corresponding timestamp vector
stopifnot(length(exp_FRA_60) == length(hourly_dates_FRA_60))
stopifnot(length(imp_FRA_60) == length(hourly_dates_FRA_60))

# Create the data frame
bilateral_flows_ESP_FRA_60 <- data.frame(
  time_long = hourly_dates_FRA_60,
  imp_FRA_mw = imp_FRA_60,
  exp_FRA_mw = exp_FRA_60,
  net_imp_FRA_mw = imp_FRA_60 - exp_FRA_60
)

# And now retrieve the data that is stored in 15 minute format
imp_exp_ESP_FRA_15 <- list()
years <- 2023:2024

for (year in years) {
  for (i in 1:2) {
    
    if (i == 1) {
      out_Domain <- pair_ESP_FRA["Spain"]
      in_Domain <- pair_ESP_FRA["France"]
      flow_type <- "Exports from Spain to France"
    } else {
      out_Domain <- pair_ESP_FRA["France"]
      in_Domain <- pair_ESP_FRA["Spain"]
      flow_type <- "Exports from France to Spain"
    }
    
    print(paste0("Getting ", flow_type, " in ", year, " (15 min data)"))
    
    start_date <- paste0(year, "01010000")
    end_date <- paste0(year + 1, "01010000")
    
    if (year == 2023) {
      start_date <- paste0(year, "03270000") # In the last 5 days of March 2023, the data already changes to 15 min 
      end_date <- paste0(year + 1, "01010000") 
    }
    
    response <- request(base_url_entsoe) %>%
      req_url_query(
        securityToken = APIKey_entsoe,
        documentType = "A11",
        in_Domain = in_Domain,
        out_Domain = out_Domain,
        periodStart = start_date,
        periodEnd = end_date
      ) %>%
      req_perform()
    
    request_xml <- resp_body_xml(response)
    
    ns <- xml_ns(request_xml)
    ns <- c(ns, ns = "urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:0")
    
    flow_data <- xml_find_all(request_xml, ".//ns:Point/ns:quantity", ns) %>%
      xml_double()
    
    flow_name <- paste(year, names(pair_ESP_FRA)[which(pair_ESP_FRA == out_Domain)], 
                       "to", names(pair_ESP_FRA)[which(pair_ESP_FRA == in_Domain)])
    imp_exp_ESP_FRA_15[[flow_name]] <- flow_data
  }
}

# Use the function to convert 15-minute data to 1-hour data (not sure if can directly retrieve hourly data from entso-E)
imp_exp_ESP_FRA_15 <- lapply(imp_exp_ESP_FRA_15, average_every_4)

# The rest is for FRA_15
hourly_dates_FRA_15 <- hourly_dates[(length_FRA_60 + 1):length(hourly_dates)]

# Separate export and import vectors
exp_FRA_15_list <- imp_exp_ESP_FRA_15[grep("Spain to France", names(imp_exp_ESP_FRA_15))]
imp_FRA_15_list <- imp_exp_ESP_FRA_15[grep("France to Spain", names(imp_exp_ESP_FRA_15))]

# Combine them into single vectors (respecting year order)
exp_FRA_15 <- unlist(exp_FRA_15_list, use.names = FALSE)
imp_FRA_15 <- unlist(imp_FRA_15_list, use.names = FALSE)

# Create the data frame
bilateral_flows_ESP_FRA_15 <- data.frame(
  time_long = hourly_dates_FRA_15,
  imp_FRA_mw = imp_FRA_15,
  exp_FRA_mw = exp_FRA_15,
  net_imp_FRA_mw = imp_FRA_15 - exp_FRA_15
)

bilateral_flows_ESP_FRA <- rbind(bilateral_flows_ESP_FRA_60, bilateral_flows_ESP_FRA_15)


# Getting data from Morocco has been very challenging, so we follow an indirect approach. 
# Since international flows of Spain are mainly with France, Portugal and Morocco (Andorra is negligible),
# We attribute to Morocco the difference between the net flows and those in Spain and Portugal

# Therefore, we need to start by retrieving data of net international flows (indicator 2043)
# This is, we are back to using esios API

net_flows_data <- list()
years <- 2020:2024

for (year in years) {
  print(paste0("Retrieving net international flows for year ", year))
  
  start_date = paste0(year, "-01-01T00:00:00Z")
  end_date <- paste0(year, "-12-31T23:00:00Z")
  
  request <- request(base_url_esios) %>%
    req_url_path_append("indicators", 2043) %>%
    req_headers(
      Accept = "application/json; application/vnd.esios-api-v1+json",
      `Content-Type` = "application/json",
      `x-api-key` = APIKey_esios
    ) %>%
    req_url_query(
      start_date = start_date,
      end_date = end_date,
      time_trunc = "hour",
      time_agg = "avg",
    ) %>%
    req_perform() %>%
    resp_body_json() %>%
    pluck("indicator", "values") %>%
    map_dfr(as_tibble) %>%
    transmute(
      time_long = ymd_hms(datetime),
      total_net_flows_mw = value
    )
  
  net_flows_data[[as.character(year)]] <- request
}

net_flows_data_raw <- bind_rows(net_flows_data) %>% arrange(time_long)

colSums(is.na(net_flows_data_raw))

start_time <- as.POSIXct("2020-01-01 00:00:00", tz = "UTC")
end_time <- as.POSIXct("2024-12-31 23:59:59", tz = "UTC")
full_time_sequence <- seq(from = start_time, to = end_time, by = "hour")
full_time_df <- data.frame(time_long = full_time_sequence)

net_flows_dataset <- full_time_df %>%
  left_join(net_flows_data_raw, by = "time_long") 

colSums(is.na(net_flows_dataset))


# Combine all the flow data into 1 dataset
bilateral_flows_dataset <- bilateral_flows_ESP_FRA %>% 
  left_join(bilateral_flows_ESP_POR, by = "time_long")

# After many many attempts to merge by time_long, fixing the time zones, etc. the best way to achieve correct alignment of datasets is simple cbind
net_flows_vector <- net_flows_dataset %>% 
  select(-time_long)

bilateral_flows_dataset <- cbind(bilateral_flows_dataset, net_flows_vector)

# Check missing values
colSums(is.na(bilateral_flows_dataset))  

# We assume Morocco flows to be equivalent to the difference between total net flows and net flows with Portugal and France
# The data we have retrieved is always net (in the sense that every hour there's just either imports or exports to each interconnection).

# Total net flows = POR net flows + FRA net flows + MOR net flows
# MOR net flows = Total net flows - POR net flows - FRA net flows
# MOR net flows are exports if net flows are negative, and imports if positive

bilateral_flows_dataset <- bilateral_flows_dataset %>% 
  mutate(net_imp_MOR_mw = total_net_flows_mw - net_imp_POR_mw - net_imp_FRA_mw)

bilateral_flows_dataset$imp_MOR_mw <- ifelse(bilateral_flows_dataset$net_imp_MOR_mw > 0, bilateral_flows_dataset$net_imp_MOR_mw, 0)
bilateral_flows_dataset$exp_MOR_mw <- ifelse(bilateral_flows_dataset$net_imp_MOR_mw > 0, 0, abs(bilateral_flows_dataset$net_imp_MOR_mw))

# Order the columns of the dataset
bilateral_flows_dataset <- bilateral_flows_dataset %>%
  select(time_long, imp_FRA_mw, exp_FRA_mw, net_imp_FRA_mw, imp_POR_mw, exp_POR_mw, net_imp_POR_mw, imp_MOR_mw, exp_MOR_mw, net_imp_MOR_mw)


# ===== 4.2 Imports and exports esios indicators (hourly) ===== 

# As an alternative measure, we use new indicator found in esios
interconnections_ids <- as.character(c(10207:10209))

interconnections_data <- list()
years <- 2020:2024

for (id in interconnections_ids) {
  yearly_data <- list()
  
  for (year in years) {
    
    print(paste0("Retrieving ", id_to_name[[id]], " for year ", year))
    
    start_date <- paste0(year, "-01-01T00:00:00Z")
    end_date <- paste0(year, "-12-31T23:00:00Z")
    
    request <- request(base_url_esios) %>% 
      req_url_path_append("indicators", id) %>% 
      req_headers(
        Accept = "application/json; application/vnd.esios-api-v1+json",
        `Content-Type` = "application/json",
        `x-api-key` = APIKey_esios
      ) %>% 
      req_url_query(
        start_date = start_date,
        end_date = end_date,
        time_trunc = "hour",
        time_agg = "avg"
      ) %>% 
      req_perform() %>% 
      resp_body_json() %>% 
      pluck("indicator", "values") %>% 
      map_dfr(as_tibble) %>% 
      transmute(
        time_long = ymd_hms(datetime),
        !!paste0(id_to_name[[id]], "_mw") := value
      )
    
    yearly_data[[as.character(year)]] <- request
  }
  
  interconnections_data[[id]] <- bind_rows(yearly_data)
}

interconnections_data_raw <- reduce(interconnections_data, full_join, by = "time_long")

interconnections_dataset <- interconnections_data_raw 

colSums(is.na(interconnections_dataset))

# Check if the separate indicators are close to matching the total net flows
interconnections_dataset <- interconnections_data_raw %>% 
  mutate(total_pen_interconnections_mw = net_pen_interconnection_france_mw + net_pen_interconnection_portugal_mw + 
           net_pen_interconnection_morocco_mw,
         gap_obs_interconnections_mw = net_international_flows_mw - total_pen_interconnections_mw)

# Check missing values
colSums(is.na(interconnections_dataset))

# Again, perform cbind to merge datasets
interconnections_dataset <- interconnections_dataset %>% 
  select(-time_long)

imp_exp_dataset <- cbind(bilateral_flows_dataset, interconnections_dataset)

colSums(is.na(imp_exp_dataset))

imp_exp_dataset <- imp_exp_dataset %>% 
  mutate(gap_ind_flows_france = net_imp_FRA_mw - net_pen_interconnection_france_mw,
         gap_ind_flows_portugal = net_imp_POR_mw - net_pen_interconnection_portugal_mw,
         gap_ind_flows_morocco = net_imp_MOR_mw - net_pen_interconnection_morocco_mw) %>%
  select(time_long, imp_FRA_mw, exp_FRA_mw, net_imp_FRA_mw, net_pen_interconnection_france_mw, gap_ind_flows_france,
          imp_POR_mw, exp_POR_mw, net_imp_POR_mw, net_pen_interconnection_portugal_mw, gap_ind_flows_portugal,
          imp_MOR_mw, exp_MOR_mw, net_imp_MOR_mw, net_pen_interconnection_morocco_mw, gap_ind_flows_morocco,
          net_international_flows_mw, total_pen_interconnections_mw,
          gap_obs_interconnections_mw)

# Check missing values
colSums(is.na(imp_exp_dataset))          

write_xlsx(imp_exp_dataset, "imports_exports_raw_data.xlsx")

# ===== 5. Electricity prices (hourly) ===== 

spot_price_data <- list()
years <- 2020:2024

for (year in years) {
  print(paste0("Retrieving spot price for year ", year))
  
  start_date <- paste0(year, "-01-01T00:00:00Z")
  end_date <- paste0(year, "-12-31T23:00:00Z")
  
  request <- request(base_url_esios) %>%
    req_url_path_append("indicators", 600) %>%
    req_headers(
      Accept = "application/json; application/vnd.esios-api-v1+json",
      `Content-Type` = "application/json",
      `x-api-key` = APIKey_esios
    ) %>%
    req_url_query(
      start_date = start_date,
      end_date = end_date,
      time_trunc = "hour",
      time_agg = "avg",
      `geo_ids[]` = 3
    ) %>%
    req_perform() %>%
    resp_body_json() %>%
    pluck("indicator", "values") %>%
    map_dfr(as_tibble) %>%
    transmute(
      time_long = ymd_hms(datetime),
      spot_price_eur_mwh = value
    )
  
  spot_price_data[[as.character(year)]] <- request
}

spot_price_dataset <- bind_rows(spot_price_data) %>% arrange(time_long)

# Check missing values
colSums(is.na(spot_price_dataset))  

# write_xlsx(spot_price_dataset, "spot_price_data.xlsx")



# ===== 6 Self-declared capacity factors of hydro and nuclear ===== 

HydroNuc_cap_factors_ids <- as.character(c(472, 474))

HydroNuc_available_cap_data <- list()
years <- 2020:2024
months <- 1:12

for (id in HydroNuc_cap_factors_ids) {
  monthly_data <- list()
  
  for (year in years) {
    for (month in months) {
      
      print(paste0("Retrieving ", id_to_name[[id]], " for year ", year, " and month ", month))
      
      start_date <- paste0(year, "-", month, "-01T00:00:00Z")
      end_date <- ceiling_date(as.Date(start_date), "month") 
      
      request <- request(base_url_esios) %>% 
        req_url_path_append("indicators", id) %>% 
        req_headers(
          Accept = "application/json; application/vnd.esios-api-v1+json",
          `Content-Type` = "application/json",
          `x-api-key` = APIKey_esios
        ) %>% 
        req_url_query(
          start_date = start_date,
          end_date = end_date,
          geo_agg = "sum",
        ) %>% 
        req_perform() %>% 
        resp_body_json() %>% 
        pluck("indicator", "values") %>% 
        map_dfr(as_tibble) %>% 
        group_by(datetime) %>% 
        summarise(value = mean(value), .groups = "drop") %>% 
        transmute(
          time_long = ymd_hms(datetime),
          !!paste0(id_to_name[[id]], "_mw") := value
        )
      
      monthly_data[[paste0(year, "-", sprintf("%02d", month))]] <- request
      
    }
  }
  
  HydroNuc_available_cap_data[[id]] <- bind_rows(monthly_data)
}

hydronuc_available_cap <- reduce(HydroNuc_available_cap_data, full_join, by = "time_long")

# Check missing values
colSums(is.na(hydronuc_available_cap))  

hydronuc_available_cap_dataset <- full_time_df %>%
  left_join(hydronuc_available_cap, by = "time_long") 



# ===== 7. Merging the full dataset ===== 

# Once we have everything, merge all them. To avoid problems, I cbind all the data to demand_dataset

spot_price_data_merge <- spot_price_dataset %>% select(-time_long)
hourly_installed_cap_data_merge <- hourly_installed_cap_dataset %>% select(-time_long)
generation_data_merge <- generation_dataset %>% select(-time_long)
imp_exp_data_merge <- imp_exp_dataset %>% select(-time_long)
hydronuc_available_cap_data_merge <- hydronuc_available_cap_dataset %>% select(-time_long)

full_dataset <- cbind(demand_dataset, spot_price_data_merge, hourly_installed_cap_data_merge, 
                      generation_data_merge, imp_exp_data_merge, hydronuc_available_cap_data_merge)

# And create more readable time columns
full_dataset <- full_dataset %>%
  mutate(
    year = year(time_long),
    month = month(time_long),
    day = day(time_long),
    hour = hour(time_long)
  ) %>%
  select(time_long, year, month, day, hour, spot_price_eur_mwh, everything())

write_xlsx(full_dataset, "full_dataset.xlsx")

# Clean all objects which we do not need anymore

keep_vars <- c(
  "base_url_esios", "APIKey_esios", "clean_indicators", "id_to_name", "demand_ids", "demand_dataset", 
  "installed_cap_ids", "ins_cap_gen_dataset", "installed_cap_dataset", "hourly_installed_cap_dataset", 
  "generation_ids", "generation_dataset", "generation_data_raw", "base_url_entsoe", "APIKey_entsoe", 
  "pair_ESP_POR", "bilateral_flows_ESP_POR", "pair_ESP_FRA", "bilateral_flows_ESP_FRA", "net_flows_dataset", 
  "net_flows_data_raw", "interconnections_data_raw", "interconnections_dataset", "imp_exp_dataset",
  "HydroNuc_available_cap_dataset", "bilateral_flows_dataset", "spot_price_dataset", 
  "cogen_waste_separate_data", "cogen_waste_separate_data_raw", "full_dataset" 
)

rm(list = setdiff(ls(), keep_vars))

