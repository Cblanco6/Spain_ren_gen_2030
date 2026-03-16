rm(list=ls())
library(tidyverse)
library(readxl)
library(writexl)

setwd("/Users/pauorive23/Desktop/Màster BSE/Thesis/")

hourly_data <- read_xlsx("Data/data_version3.xlsx")
fixed_data <- read_xlsx("Data/fixed_data_euros.xlsx")

summary_cap_year <- hourly_data %>% 
  select(year, ends_with("cap_mw")) %>% 
  group_by(year) %>% 
  summarize(across(ends_with("cap_mw"), mean, na.rm = TRUE))

summary_fuel_prices_year <- hourly_data %>% 
  select(year, starts_with("cost")) %>% 
  group_by(year) %>% 
  summarize(across(starts_with("cost"), mean, na.rm = TRUE))

summary_eu_ets_prices_year <- hourly_data %>% 
  select(year, eu_ets_price_eur_tco2) %>% 
  group_by(year) %>% 
  summarize(eu_ets_price_eur_tco2 = mean(eu_ets_price_eur_tco2))

summary_costs_year <- cbind(summary_fuel_prices_year, summary_eu_ets_prices_year)

avg_cost_coal_2023 <- summary_costs_year[4,2]
avg_cost_natural_gas_2023 <- summary_costs_year[4,3]
avg_cost_diesel_2023 <- summary_costs_year[4,4]
avg_cost_uranium_2023 <- summary_costs_year[4,5]
avg_cost_eu_ets_2023 <- summary_costs_year[4,7]

avg_cost_coal_2024 <- summary_costs_year[5,2]
avg_cost_natural_gas_2024 <- summary_costs_year[5,3]
avg_cost_diesel_2024 <- summary_costs_year[5,4]
avg_cost_uranium_2024 <- summary_costs_year[5,5]
avg_cost_eu_ets_2024 <- summary_costs_year[5,7]

fixed_data$coal = ifelse(fixed_data$technology == "coal", 1, 0)
fixed_data$natural_gas = ifelse(fixed_data$technology %in% c("combined_cycle", "gas_turbine", "vapor_turbine", "cogeneration"), 1, 0)
fixed_data$diesel = ifelse(fixed_data$technology == "diesel_engine", 1, 0)
fixed_data$nuclear = ifelse(fixed_data$technology == "nuclear", 1, 0)

fixed_data_mc <- fixed_data %>%
  mutate(avg_fuel_cost_2023 = case_when(
    coal == 1 ~ avg_cost_coal_2023 / efficiency,
    natural_gas == 1 ~ avg_cost_natural_gas_2023 / efficiency,
    diesel == 1 ~ avg_cost_diesel_2023 / efficiency,
    nuclear == 1 ~ avg_cost_uranium_2023 / efficiency,
    TRUE ~ 0  # for non-fuel-based technologies
  ), avg_fuel_cost_2024 = case_when(
    coal == 1 ~ avg_cost_coal_2024 / efficiency,
    natural_gas == 1 ~ avg_cost_natural_gas_2024 / efficiency,
    diesel == 1 ~ avg_cost_diesel_2024 / efficiency,
    nuclear == 1 ~ avg_cost_uranium_2024 / efficiency,
    TRUE ~ 0  # for non-fuel-based technologies
  ),
  avg_marginal_cost_2023 = var_om_eur_mwh + avg_fuel_cost_2023 + direct_e_tco2_mwh * avg_cost_eu_ets_2023,
  avg_marginal_cost_2024 = var_om_eur_mwh + avg_fuel_cost_2024 + direct_e_tco2_mwh * avg_cost_eu_ets_2024
)

fixed_data_mc <- fixed_data_mc %>% arrange(avg_marginal_cost_2024)

write_xlsx(fixed_data_mc, "Data/fixed_data_mc.xlsx")

