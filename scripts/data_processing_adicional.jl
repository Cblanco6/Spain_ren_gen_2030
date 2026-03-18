# creo temporalmente este archivo para almacenar de momento
# aspectos que tenían que ir en el script de data processing 
# pero que por alguna razón lo pusimos al principio del modelo



# estos son los datasets que usabamos
hourly_data = CSV.read(string(dirpath, "Data/data_version3.csv"), DataFrame)
fixed_data = CSV.read(string(dirpath, "Data/fixed_data_euros.csv"), DataFrame)
projection_deltas = CSV.read(string(dirpath, "Data/projection_deltas_low_demand.csv"), DataFrame)

# Major fixes to the data
# 1. Make sure all generation and cap factors are non-negative
hourly_data.solar_thermal_gen_mwh .= ifelse.(hourly_data.solar_thermal_gen_mwh .< 0, 0, hourly_data.solar_thermal_gen_mwh)
hourly_data.solar_thermal_cap_factor .= ifelse.(hourly_data.solar_thermal_cap_factor .< 0, 0, hourly_data.solar_thermal_cap_factor)

# 2. Set minimum price to be 0.5 such that demand functions are well defined
hourly_data.spot_price_eur_mwh .= ifelse.(hourly_data.spot_price_eur_mwh .<= 0.5, 0.5, hourly_data.spot_price_eur_mwh)

# 3. Set all quantities to be in GWh/GW
hourly_data[!,Between(:residential_demand_mwh,:batteries_gen_mwh)] = hourly_data[!,Between(:residential_demand_mwh,:batteries_gen_mwh)] ./ 1000.0 # Convert from MWh to GWh
hourly_data[!,Between(:imports_France_mwh,:net_flows_Morocco_mwh)] = hourly_data[!,Between(:imports_France_mwh,:net_flows_Morocco_mwh)] ./ 1000.0 # Convert from MWh to GWh
hourly_data[!,Between(:eff_65_Average,:eff_85_Average)] = hourly_data[!,Between(:eff_65_Average,:eff_85_Average)] ./ 1000.0 # Convert from MWh to GWh

# 4. Set capacity in fixed_data to be in GW
fixed_data.avg_cap_2024 .= fixed_data.avg_cap_2024 ./ 1000.0 # Convert from MW to GW