function load_scenario_params(path::String, scenario_name::String)
    df = CSV.read(path, DataFrame)
    row = df[df.scenario .== scenario_name, :]
    @assert nrow(row) == 1 "Escenario '$scenario_name' no encontrado o duplicado"
    return NamedTuple(pairs(eachrow(row)[1]))
end


# ===== Monte Carlo Simulation set up =====
# Pre-setup phase (run once)
baseline_years = [2023, 2024]
variables_to_draw = [
    "residential_demand_mwh", "commercial_demand_mwh", "industrial_demand_mwh", 
    "coal_cap_mw", "combined_cycle_cap_mw", "gas_turbine_cap_mw", "vapor_turbine_cap_mw", "cogeneration_cap_mw", "diesel_cap_mw", 
    "nonrenewable_waste_cap_mw", "nuclear_cap_mw", "conventional_hydro_cap_mw", "run_of_river_hydro_cap_mw", "pumped_hydro_cap_mw", 
    "solar_pv_cap_mw", "solar_thermal_cap_mw", "wind_cap_mw", "other_renewable_cap_mw", "renewable_waste_cap_mw", "batteries_cap_mw",
    "cost_coal_eur_mwh", "cost_gas_eur_mwh", "cost_diesel_eur_mwh", "cost_uranium_eur_mwh", "eu_ets_price_eur_tco2",
    "imp_fra_cap_mw", "exp_fra_cap_mw", "imp_por_cap_mw", "exp_por_cap_mw", "imp_mor_cap_mw", "exp_mor_cap_mw"
]

# Pre-compute sampling data
sampling_data = Dict{String, Tuple{Vector{Float64}, Weights}}()
for var in variables_to_draw
    subset = projection_deltas[projection_deltas.variable .== var, :]
    sampling_data[var] = (subset.delta, Weights(subset.weight))
end

# Function to create distributions based on sampling data
function continuous_sample_delta(deltas::Vector{Float64}, weights::Weights, var::String)

    # Special case for coal capacity (100% sure of phase out)
    if var == "coal_cap_mw"
        return deltas[1]
    end

    # Discrete sampling for interconnectors capacity
    if startswith(var, "imp_") || startswith(var, "exp_")
        normalized_weights = weights ./ sum(weights)
        idx = sample(1:length(deltas), Weights(normalized_weights))
        return deltas[idx]
    end

    mu = mean(deltas)
    sigma = std(deltas)

    small_std_threshold = 0.05

    # For those that we do not have information, # we use a small standard deviation 
    if mu == 0 && sigma == 0
        std_dev = 0.05
        return rand(Normal(0.0, std_dev))
    end

    if sigma < small_std_threshold
        # Normal approx for small spread
        return rand(Normal(mu, sigma))
    else
        # KDE with reduced bandwidth for tighter distribution
        # Default bandwidth is approximately 1.06 * std(data) * n^(-1/5)
        # We'll use a smaller factor to reduce spread
        bandwidth = 0.75 * std(deltas) * length(deltas)^(-1/5)
        kde_est = KernelDensity.kde(deltas, weights=weights, bandwidth=bandwidth)
        
        # Sample from KDE by inverse transform sampling
        x_vals = kde_est.x
        pdf_vals = kde_est.density
        
        # Normalize PDF values
        pdf_vals = pdf_vals ./ sum(pdf_vals)
        cdf_vals = cumsum(pdf_vals)

        # Sample with bounds checking
        u = rand()
        idx = searchsortedfirst(cdf_vals, u)
        sampled_val = x_vals[clamp(idx, 1, length(x_vals))]
        
        # Optional: Further constrain extreme values
        min_val = minimum(deltas) - 0.1 * abs(minimum(deltas))
        max_val = maximum(deltas) + 0.1 * abs(maximum(deltas))
        return clamp(sampled_val, min_val, max_val)
    end
end

# Pre-allocate containers
all_delta_draws = DataFrame(
    year_random_draw = Int[],
    day_start = Int[],
    delta_residential_demand_mwh = Float64[], 
    delta_commercial_demand_mwh = Float64[],
    delta_industrial_demand_mwh = Float64[],
    delta_coal_cap_mw = Float64[],
    delta_combined_cycle_cap_mw = Float64[],
    delta_gas_turbine_cap_mw = Float64[],
    delta_vapor_turbine_cap_mw = Float64[],
    delta_cogeneration_cap_mw = Float64[],
    delta_diesel_cap_mw = Float64[],
    delta_nonrenewable_waste_cap_mw = Float64[],
    delta_nuclear_cap_mw = Float64[],
    delta_conventional_hydro_cap_mw = Float64[],
    delta_run_of_river_hydro_cap_mw = Float64[],
    delta_pumped_hydro_cap_mw = Float64[],
    delta_solar_pv_cap_mw = Float64[],
    delta_solar_thermal_cap_mw = Float64[],
    delta_wind_cap_mw = Float64[],
    delta_other_renewable_cap_mw = Float64[],
    delta_renewable_waste_cap_mw = Float64[],
    delta_batteries_cap_mw = Float64[],
    delta_cost_coal_eur_mwh = Float64[],
    delta_cost_gas_eur_mwh = Float64[],
    delta_cost_diesel_eur_mwh = Float64[],
    delta_cost_uranium_eur_mwh = Float64[],
    delta_eu_ets_price_eur_tco2 = Float64[],
    delta_imp_fra_cap_mw = Float64[],
    delta_exp_fra_cap_mw = Float64[],
    delta_imp_por_cap_mw = Float64[],
    delta_exp_por_cap_mw = Float64[],
    delta_imp_mor_cap_mw = Float64[],
    delta_exp_mor_cap_mw = Float64[]   
)

mean_values_new_data = DataFrame(
    residential_demand_mwh = Float64[],
    commercial_demand_mwh = Float64[],
    industrial_demand_mwh = Float64[],
    coal_cap_mw = Float64[],
    combined_cycle_cap_mw = Float64[],
    gas_turbine_cap_mw = Float64[],
    vapor_turbine_cap_mw = Float64[],
    cogeneration_cap_mw = Float64[],
    diesel_cap_mw = Float64[],
    nonrenewable_waste_cap_mw = Float64[],
    nuclear_cap_mw = Float64[],
    conventional_hydro_cap_mw = Float64[],
    run_of_river_hydro_cap_mw = Float64[],
    pumped_hydro_cap_mw = Float64[],
    solar_pv_cap_mw = Float64[],
    solar_thermal_cap_mw = Float64[],
    wind_cap_mw = Float64[],
    other_renewable_cap_mw = Float64[],
    renewable_waste_cap_mw = Float64[],
    batteries_cap_mw = Float64[],
    cost_coal_eur_mwh = Float64[],
    cost_gas_eur_mwh = Float64[],
    cost_diesel_eur_mwh = Float64[],
    cost_uranium_eur_mwh = Float64[],
    eu_ets_price_eur_tco2 = Float64[],
)

selected_results =  DataFrame(
  iteration = Int[],
  avg_price = Float64[],
  max_price = Float64[],
  std_price = Float64[],
  total_demand = Float64[],
  total_generation = Float64[],
  combined_cycle_gen = Float64[],
  cogeneration_gen = Float64[],
  nuclear_gen = Float64[],
  other_non_renewable_gen = Float64[],
  conventional_hydro_gen = Float64[],
  pumped_hydro_gen = Float64[],
  solar_pv_gen = Float64[],
  solar_thermal_gen = Float64[],
  wind_gen = Float64[],
  batteries_gen = Float64[],
  other_renewable_gen = Float64[],
  total_imports = Float64[],
  total_exports = Float64[],
  consumer_surplus = Float64[],
  producer_surplus = Float64[],
  net_welfare = Float64[],
  min_share_renewable_gen = Float64[],
  mean_share_renewable_gen = Float64[],
  median_share_renewable_gen = Float64[],
  max_share_renewable_gen = Float64[],
  min_non_renewable_gen = Float64[],
  lifecycle_emissions = Float64[],
  direct_emissions = Float64[],
  curt_solar_pv = Float64[],
  curt_solar_thermal = Float64[],
  curt_wind = Float64[],
)

hourly_new_results =  DataFrame(
  iteration = Int[],
  price1 = Float64[], price2 = Float64[], price3 = Float64[], price4 = Float64[], price5 = Float64[], price6 = Float64[], price7 = Float64[], price8 = Float64[],
    price9 = Float64[], price10 = Float64[], price11 = Float64[], price12 = Float64[], price13 = Float64[], price14 = Float64[], price15 = Float64[], price16 = Float64[],
    price17 = Float64[], price18 = Float64[], price19 = Float64[], price20 = Float64[], price21 = Float64[], price22 = Float64[], price23 = Float64[], price24 = Float64[],
    battout1 = Float64[], battout2 = Float64[], battout3 = Float64[], battout4 = Float64[], battout5 = Float64[], battout6 = Float64[], battout7 = Float64[], battout8 = Float64[],
    battout9 = Float64[], battout10 = Float64[], battout11 = Float64[], battout12 = Float64[], battout13 = Float64[], battout14 = Float64[], battout15 = Float64[], battout16 = Float64[],
    battout17 = Float64[], battout18 = Float64[], battout19 = Float64[], battout20 = Float64[], battout21 = Float64[], battout22 = Float64[], battout23 = Float64[], battout24 = Float64[],
    phout1 = Float64[], phout2 = Float64[], phout3 = Float64[], phout4 = Float64[], phout5 = Float64[], phout6 = Float64[], phout7 = Float64[], phout8 = Float64[],
    phout9 = Float64[], phout10 = Float64[], phout11 = Float64[], phout12 = Float64[], phout13 = Float64[], phout14 = Float64[], phout15 = Float64[], phout16 = Float64[],
    phout17 = Float64[], phout18 = Float64[], phout19 = Float64[], phout20 = Float64[], phout21 = Float64[], phout22 = Float64[], phout23 = Float64[], phout24 = Float64[],
    emissions1 = Float64[], emissions2 = Float64[], emissions3 = Float64[], emissions4 = Float64[], emissions5 = Float64[], emissions6 = Float64[], emissions7 = Float64[], emissions8 = Float64[], 
    emissions9 = Float64[], emissions10 = Float64[], emissions11 = Float64[], emissions12 = Float64[], emissions13 = Float64[], emissions14 = Float64[], emissions15 = Float64[], emissions16 = Float64[],
    emissions17 = Float64[], emissions18 = Float64[], emissions19 = Float64[], emissions20 = Float64[], emissions21 = Float64[], emissions22 = Float64[], emissions23 = Float64[], emissions24 = Float64[],
    rengenshare1 = Float64[], rengenshare2 = Float64[], rengenshare3 = Float64[], rengenshare4 = Float64[], rengenshare5 = Float64[], rengenshare6 = Float64[],
    rengenshare7 = Float64[], rengenshare8 = Float64[], rengenshare9 = Float64[], rengenshare10 = Float64[], rengenshare11 = Float64[], rengenshare12 = Float64[],
    rengenshare13 = Float64[], rengenshare14 = Float64[], rengenshare15 = Float64[], rengenshare16 = Float64[], rengenshare17 = Float64[], rengenshare18 = Float64[],
    rengenshare19 = Float64[], rengenshare20 = Float64[], rengenshare21 = Float64[], rengenshare22 = Float64[], rengenshare23 = Float64[], rengenshare24 = Float64[],
)

monthly_new_results = DataFrame(
  iteration = Int[],
  price1 = Float64[], price2 = Float64[], price3 = Float64[], price4 = Float64[], price5 = Float64[], price6 = Float64[], 
  price7 = Float64[], price8 = Float64[], price9 = Float64[], price10 = Float64[], price11 = Float64[], price12 = Float64[],
  battout1 = Float64[], battout2 = Float64[], battout3 = Float64[], battout4 = Float64[], battout5 = Float64[], battout6 = Float64[],
  battout7 = Float64[], battout8 = Float64[], battout9 = Float64[], battout10 = Float64[], battout11 = Float64[], battout12 = Float64[],
  phout1 = Float64[], phout2 = Float64[], phout3 = Float64[], phout4 = Float64[], phout5 = Float64[], phout6 = Float64[],
  phout7 = Float64[], phout8 = Float64[], phout9 = Float64[], phout10 = Float64[], phout11 = Float64[], phout12 = Float64[],
  emissions1 = Float64[], emissions2 = Float64[], emissions3 = Float64[], emissions4 = Float64[], emissions5 = Float64[], emissions6 = Float64[],
  emissions7 = Float64[], emissions8 = Float64[], emissions9 = Float64[], emissions10 = Float64[], emissions11 = Float64[], emissions12 = Float64[],
  rengenshare1 = Float64[], rengenshare2 = Float64[], rengenshare3 = Float64[], rengenshare4 = Float64[], rengenshare5 = Float64[], rengenshare6 = Float64[],
  rengenshare7 = Float64[], rengenshare8 = Float64[], rengenshare9 = Float64[], rengenshare10 = Float64[], rengenshare11 = Float64[], rengenshare12 = Float64[],
)

function calculate_hourly_averages(data::Vector{Float64}, hours_per_day::Int=24)
    # Make sure the vector length is divisible by hours_per_day
    @assert length(data) % hours_per_day == 0 "Data length must be divisible by hours_per_day"
    
    num_days = length(data) ÷ hours_per_day
    hourly_averages = zeros(Float64, hours_per_day)

    for hour in 1:hours_per_day
        # Get values for this specific hour across all days
        hour_values = [data[hour + (day-1)*hours_per_day] for day in 1:num_days]        
        # Calculate the average
        hourly_averages[hour] = mean(hour_values)
    end

    return hourly_averages
end

function calculate_monthly_averages(data::Vector{Float64}, month_indices::Vector{Int})
    # Check that we have month data for each hour
    @assert length(data) == length(month_indices) "Data and month_indices must be same length"
    
    monthly_sums = zeros(Float64, 12)
    monthly_counts = zeros(Int, 12)
    
    for i in 1:length(data)
        month = month_indices[i]
        if 1 <= month <= 12  # Ensure valid month index
            monthly_sums[month] += data[i]
            monthly_counts[month] += 1
        end
    end
    
    # Calculate averages (handle case where some months might not have data)
    monthly_averages = zeros(Float64, 12)
    for m in 1:12
        if monthly_counts[m] > 0
            monthly_averages[m] = monthly_sums[m] / monthly_counts[m]
        else
            # If no data for this month, use NaN or some placeholder
            monthly_averages[m] = NaN
        end
    end

    return monthly_averages
end

# ===== Monte Carlo Simulation Loop =====
num_iterations = 5000

for i in 201:num_iterations
    @printf("Running iteration %d of %d\n", i, num_iterations)
    year_random_draw = rand(baseline_years)
    
    new_data_full_year = DataFrame()  
    append!(new_data_full_year, hourly_data[hourly_data.year .== year_random_draw, :])

    # Now define new_data to be only 7 days of each month
    day_start = rand(1:21)
    new_data = filter(row -> row.day >= day_start && row.day < day_start + 7, new_data_full_year)
    
    delta_draws = Dict{String, Float64}()
    interconnectors_delta = Vector{Float64}(undef, 6)  
    
    # Fast sampling
    for var in variables_to_draw
        deltas, weights = sampling_data[var]
        delta_draws[var] = continuous_sample_delta(deltas, weights, var) # continuous version
        
        # Only modify new_data if it's not an interconnector variable
        if !startswith(var, "imp_") && !startswith(var, "exp_")
            new_data[!, var] .*= (1 + delta_draws[var])
        end
    end

    # Adjust conventional hydro variables
    delta_conventional_hydro = delta_draws["conventional_hydro_cap_mw"]
    new_data.conventional_hydro_cap_mw .*= (1 + delta_conventional_hydro)

    # Adjust interconnectors
    interconnectors_delta[1] = delta_draws["imp_fra_cap_mw"]
    interconnectors_delta[2] = delta_draws["exp_fra_cap_mw"]
    interconnectors_delta[3] = delta_draws["imp_por_cap_mw"]
    interconnectors_delta[4] = delta_draws["exp_por_cap_mw"]
    interconnectors_delta[5] = delta_draws["imp_mor_cap_mw"]
    interconnectors_delta[6] = delta_draws["exp_mor_cap_mw"]

    # Modify the column fixed_data.avg_cap_2024 for row coal to be the mean of new_data.coal_cap_mw
    fixed_data_new = DataFrame()  
    append!(fixed_data_new, fixed_data)    
    @views begin
        fixed_data.avg_cap_2024[fixed_data.technology .== "coal"] .= mean(new_data.coal_cap_mw)
        fixed_data.avg_cap_2024[fixed_data.technology .== "combined_cycle"] .= mean(new_data.combined_cycle_cap_mw)
        fixed_data.avg_cap_2024[fixed_data.technology .== "gas_turbine"] .= mean(new_data.gas_turbine_cap_mw)
        fixed_data.avg_cap_2024[fixed_data.technology .== "vapor_turbine"] .= mean(new_data.vapor_turbine_cap_mw)
        fixed_data.avg_cap_2024[fixed_data.technology .== "cogeneration"] .= mean(new_data.cogeneration_cap_mw)
        fixed_data.avg_cap_2024[fixed_data.technology .== "diesel"] .= mean(new_data.diesel_cap_mw)
        fixed_data.avg_cap_2024[fixed_data.technology .== "nonrenewable"] .= mean(new_data.nonrenewable_waste_cap_mw)
        fixed_data.avg_cap_2024[fixed_data.technology .== "nuclear"] .= mean(new_data.nuclear_cap_mw)
        fixed_data.avg_cap_2024[fixed_data.technology .== "conventional_hydro"] .= mean(new_data.conventional_hydro_cap_mw)
        fixed_data.avg_cap_2024[fixed_data.technology .== "run_of_river_hydro"] .= mean(new_data.run_of_river_hydro_cap_mw)
        fixed_data.avg_cap_2024[fixed_data.technology .== "pumped_hydro"] .= mean(new_data.pumped_hydro_cap_mw)
        fixed_data.avg_cap_2024[fixed_data.technology .== "solar_pv"] .= mean(new_data.solar_pv_cap_mw)
        fixed_data.avg_cap_2024[fixed_data.technology .== "solar_thermal"] .= mean(new_data.solar_thermal_cap_mw)
        fixed_data.avg_cap_2024[fixed_data.technology .== "wind"] .= mean(new_data.wind_cap_mw)
        fixed_data.avg_cap_2024[fixed_data.technology .== "other_renewable"] .= mean(new_data.other_renewable_cap_mw)
        fixed_data.avg_cap_2024[fixed_data.technology .== "renewable_waste"] .= mean(new_data.renewable_waste_cap_mw)
        fixed_data.avg_cap_2024[fixed_data.technology .== "battery"] .= mean(new_data.batteries_cap_mw)
    end

    # Dispatch the electricity market with the new data
    results = dispatch_electricity_market(new_data, fixed_data_new, interconnectors_delta, loss_factor = 0.02)

    push!(all_delta_draws, (
            year_random_draw = year_random_draw,
            day_start = day_start,
            delta_residential_demand_mwh = delta_draws["residential_demand_mwh"],
            delta_commercial_demand_mwh = delta_draws["commercial_demand_mwh"],
            delta_industrial_demand_mwh = delta_draws["industrial_demand_mwh"],
            delta_coal_cap_mw = delta_draws["coal_cap_mw"],
            delta_combined_cycle_cap_mw = delta_draws["combined_cycle_cap_mw"],
            delta_gas_turbine_cap_mw = delta_draws["gas_turbine_cap_mw"],
            delta_vapor_turbine_cap_mw = delta_draws["vapor_turbine_cap_mw"],
            delta_cogeneration_cap_mw = delta_draws["cogeneration_cap_mw"],
            delta_diesel_cap_mw = delta_draws["diesel_cap_mw"],
            delta_nonrenewable_waste_cap_mw = delta_draws["nonrenewable_waste_cap_mw"],
            delta_nuclear_cap_mw = delta_draws["nuclear_cap_mw"],
            delta_conventional_hydro_cap_mw = delta_draws["conventional_hydro_cap_mw"],
            delta_run_of_river_hydro_cap_mw = delta_draws["run_of_river_hydro_cap_mw"],
            delta_pumped_hydro_cap_mw = delta_draws["pumped_hydro_cap_mw"],
            delta_solar_pv_cap_mw = delta_draws["solar_pv_cap_mw"],
            delta_solar_thermal_cap_mw = delta_draws["solar_thermal_cap_mw"],
            delta_wind_cap_mw = delta_draws["wind_cap_mw"],
            delta_other_renewable_cap_mw = delta_draws["other_renewable_cap_mw"],
            delta_renewable_waste_cap_mw = delta_draws["renewable_waste_cap_mw"],
            delta_batteries_cap_mw = delta_draws["batteries_cap_mw"],
            delta_cost_coal_eur_mwh = delta_draws["cost_coal_eur_mwh"],
            delta_cost_gas_eur_mwh = delta_draws["cost_gas_eur_mwh"],
            delta_cost_diesel_eur_mwh = delta_draws["cost_diesel_eur_mwh"],
            delta_cost_uranium_eur_mwh = delta_draws["cost_uranium_eur_mwh"],
            delta_eu_ets_price_eur_tco2 = delta_draws["eu_ets_price_eur_tco2"],
            delta_imp_fra_cap_mw = delta_draws["imp_fra_cap_mw"],
            delta_exp_fra_cap_mw = delta_draws["exp_fra_cap_mw"],
            delta_imp_por_cap_mw = delta_draws["imp_por_cap_mw"],
            delta_exp_por_cap_mw = delta_draws["exp_por_cap_mw"],
            delta_imp_mor_cap_mw = delta_draws["imp_mor_cap_mw"],
            delta_exp_mor_cap_mw = delta_draws["exp_mor_cap_mw"],
    ))

    push!(mean_values_new_data, (
        residential_demand_mwh = mean(new_data.residential_demand_mwh),
        commercial_demand_mwh = mean(new_data.commercial_demand_mwh),
        industrial_demand_mwh = mean(new_data.industrial_demand_mwh),
        coal_cap_mw = mean(new_data.coal_cap_mw),
        combined_cycle_cap_mw = mean(new_data.combined_cycle_cap_mw),
        gas_turbine_cap_mw = mean(new_data.gas_turbine_cap_mw),
        vapor_turbine_cap_mw = mean(new_data.vapor_turbine_cap_mw),
        cogeneration_cap_mw = mean(new_data.cogeneration_cap_mw),
        diesel_cap_mw = mean(new_data.diesel_cap_mw),
        nonrenewable_waste_cap_mw = mean(new_data.nonrenewable_waste_cap_mw),
        nuclear_cap_mw = mean(new_data.nuclear_cap_mw),
        conventional_hydro_cap_mw = mean(new_data.conventional_hydro_cap_mw),
        run_of_river_hydro_cap_mw = mean(new_data.run_of_river_hydro_cap_mw),
        pumped_hydro_cap_mw = mean(new_data.pumped_hydro_cap_mw),
        solar_pv_cap_mw = mean(new_data.solar_pv_cap_mw),
        solar_thermal_cap_mw = mean(new_data.solar_thermal_cap_mw),
        wind_cap_mw = mean(new_data.wind_cap_mw),
        other_renewable_cap_mw = mean(new_data.other_renewable_cap_mw),
        renewable_waste_cap_mw = mean(new_data.renewable_waste_cap_mw),
        batteries_cap_mw = mean(new_data.batteries_cap_mw),
        cost_coal_eur_mwh = mean(new_data.cost_coal_eur_mwh),
        cost_gas_eur_mwh = mean(new_data.cost_gas_eur_mwh),
        cost_diesel_eur_mwh = mean(new_data.cost_diesel_eur_mwh),
        cost_uranium_eur_mwh = mean(new_data.cost_uranium_eur_mwh),
        eu_ets_price_eur_tco2 = mean(new_data.eu_ets_price_eur_tco2)
    ))

    push!(selected_results, (
        iteration = i,
        avg_price = results["avg_price"],
        max_price = results["max_price"],
        std_price = results["std_price"],
        total_demand = sum(results["total_demand"]) * 4.34524,
        total_generation = sum(results["total_generation"]) * 4.34524,
        combined_cycle_gen = sum(results["combined_cycle_gen"]) * 4.34524,
        cogeneration_gen = sum(results["cogeneration_gen"]) * 4.34524,
        nuclear_gen = sum(results["nuclear_gen"]) * 4.34524,
        other_non_renewable_gen = (sum(results["coal_gen"]) + sum(results["gas_turbine_gen"]) + sum(results["vapor_turbine_gen"]) 
            + sum(results["diesel_gen"]) + sum(results["non_renewable_waste_gen"])) * 4.34524,    
        conventional_hydro_gen = sum(results["conventional_hydro_gen"]) * 4.34524,
        pumped_hydro_gen = sum(results["pumped_hydro_gen"]) * 4.34524,
        solar_pv_gen = sum(results["solar_pv_gen"]) * 4.34524,
        solar_thermal_gen = sum(results["solar_thermal_gen"]) * 4.34524,
        wind_gen = sum(results["wind_gen"]) * 4.34524,
        batteries_gen = sum(results["battery_gen"]) * 4.34524,
        other_renewable_gen = (sum(results["run_of_river_hydro_gen"]) + sum(results["renewable_waste_gen"]) + sum(results["other_renewable_gen"])) * 4.34524,
        total_imports = (sum(results["imports_FRA"]) + sum(results["imports_POR"]) + sum(results["imports_MOR"])) * 4.34524,
        total_exports = (sum(results["exports_FRA"]) + sum(results["exports_POR"]) + sum(results["exports_MOR"])) * 4.34524,
        consumer_surplus = sum(results["consumer_surplus"]) * 4.34524,
        producer_surplus = sum(results["producer_surplus"]) * 4.34524,
        net_welfare = sum(results["net_welfare"]) * 4.34524,
        min_share_renewable_gen = minimum(results["share_renewable_gen"]),
        mean_share_renewable_gen = mean(results["share_renewable_gen"]),
        median_share_renewable_gen = median(results["share_renewable_gen"]),
        max_share_renewable_gen = maximum(results["share_renewable_gen"]),
        min_non_renewable_gen = mean(results["min_non_renewable_gen"]),
        lifecycle_emissions = sum(results["lifecycle_emissions"]),
        direct_emissions = sum(results["direct_emissions"]),
        curt_solar_pv = results["curtailment_solar_pv"],
        curt_solar_thermal = results["curtailment_solar_thermal"],
        curt_wind = results["curtailment_wind"]
    ))

    # Calculate hourly averages for different variables
    hourly_avg_prices = calculate_hourly_averages(results["price"])
    hourly_avg_batt_out = calculate_hourly_averages(results["battery_gen"])
    hourly_avg_ph_out = calculate_hourly_averages(results["pumped_hydro_gen"])
    hourly_avg_emissions = calculate_hourly_averages(results["direct_emissions"])
    hourly_avg_ren_share = calculate_hourly_averages(results["share_renewable_gen"])

    # Create a named tuple with all hourly values
    push!(hourly_new_results, (
        iteration = i,
        # Hourly prices (hours 1-24)
        price1 = hourly_avg_prices[1], price2 = hourly_avg_prices[2], price3 = hourly_avg_prices[3],
        price4 = hourly_avg_prices[4], price5 = hourly_avg_prices[5], price6 = hourly_avg_prices[6],
        price7 = hourly_avg_prices[7], price8 = hourly_avg_prices[8], price9 = hourly_avg_prices[9],
        price10 = hourly_avg_prices[10], price11 = hourly_avg_prices[11], price12 = hourly_avg_prices[12],
        price13 = hourly_avg_prices[13], price14 = hourly_avg_prices[14], price15 = hourly_avg_prices[15],
        price16 = hourly_avg_prices[16], price17 = hourly_avg_prices[17], price18 = hourly_avg_prices[18],
        price19 = hourly_avg_prices[19], price20 = hourly_avg_prices[20], price21 = hourly_avg_prices[21],
        price22 = hourly_avg_prices[22], price23 = hourly_avg_prices[23], price24 = hourly_avg_prices[24],
        
        # Hourly battery output (hours 1-24)
        battout1 = hourly_avg_batt_out[1], battout2 = hourly_avg_batt_out[2], battout3 = hourly_avg_batt_out[3],
        battout4 = hourly_avg_batt_out[4], battout5 = hourly_avg_batt_out[5], battout6 = hourly_avg_batt_out[6],
        battout7 = hourly_avg_batt_out[7], battout8 = hourly_avg_batt_out[8], battout9 = hourly_avg_batt_out[9],
        battout10 = hourly_avg_batt_out[10], battout11 = hourly_avg_batt_out[11], battout12 = hourly_avg_batt_out[12],
        battout13 = hourly_avg_batt_out[13], battout14 = hourly_avg_batt_out[14], battout15 = hourly_avg_batt_out[15],
        battout16 = hourly_avg_batt_out[16], battout17 = hourly_avg_batt_out[17], battout18 = hourly_avg_batt_out[18],
        battout19 = hourly_avg_batt_out[19], battout20 = hourly_avg_batt_out[20], battout21 = hourly_avg_batt_out[21],
        battout22 = hourly_avg_batt_out[22], battout23 = hourly_avg_batt_out[23], battout24 = hourly_avg_batt_out[24],
        
        # Hourly pumped hydro output (hours 1-24)
        phout1 = hourly_avg_ph_out[1], phout2 = hourly_avg_ph_out[2], phout3 = hourly_avg_ph_out[3],
        phout4 = hourly_avg_ph_out[4], phout5 = hourly_avg_ph_out[5], phout6 = hourly_avg_ph_out[6],
        phout7 = hourly_avg_ph_out[7], phout8 = hourly_avg_ph_out[8], phout9 = hourly_avg_ph_out[9],
        phout10 = hourly_avg_ph_out[10], phout11 = hourly_avg_ph_out[11], phout12 = hourly_avg_ph_out[12],
        phout13 = hourly_avg_ph_out[13], phout14 = hourly_avg_ph_out[14], phout15 = hourly_avg_ph_out[15],
        phout16 = hourly_avg_ph_out[16], phout17 = hourly_avg_ph_out[17], phout18 = hourly_avg_ph_out[18],
        phout19 = hourly_avg_ph_out[19], phout20 = hourly_avg_ph_out[20], phout21 = hourly_avg_ph_out[21],
        phout22 = hourly_avg_ph_out[22], phout23 = hourly_avg_ph_out[23], phout24 = hourly_avg_ph_out[24],
        
        # Hourly emissions (hours 1-24)
        emissions1 = hourly_avg_emissions[1], emissions2 = hourly_avg_emissions[2], emissions3 = hourly_avg_emissions[3],
        emissions4 = hourly_avg_emissions[4], emissions5 = hourly_avg_emissions[5], emissions6 = hourly_avg_emissions[6],
        emissions7 = hourly_avg_emissions[7], emissions8 = hourly_avg_emissions[8], emissions9 = hourly_avg_emissions[9],
        emissions10 = hourly_avg_emissions[10], emissions11 = hourly_avg_emissions[11], emissions12 = hourly_avg_emissions[12],
        emissions13 = hourly_avg_emissions[13], emissions14 = hourly_avg_emissions[14], emissions15 = hourly_avg_emissions[15],
        emissions16 = hourly_avg_emissions[16], emissions17 = hourly_avg_emissions[17], emissions18 = hourly_avg_emissions[18],
        emissions19 = hourly_avg_emissions[19], emissions20 = hourly_avg_emissions[20], emissions21 = hourly_avg_emissions[21],
        emissions22 = hourly_avg_emissions[22], emissions23 = hourly_avg_emissions[23], emissions24 = hourly_avg_emissions[24],
        
        # Hourly renewable generation share (hours 1-24)
        rengenshare1 = hourly_avg_ren_share[1], rengenshare2 = hourly_avg_ren_share[2], rengenshare3 = hourly_avg_ren_share[3],
        rengenshare4 = hourly_avg_ren_share[4], rengenshare5 = hourly_avg_ren_share[5], rengenshare6 = hourly_avg_ren_share[6],
        rengenshare7 = hourly_avg_ren_share[7], rengenshare8 = hourly_avg_ren_share[8], rengenshare9 = hourly_avg_ren_share[9],
        rengenshare10 = hourly_avg_ren_share[10], rengenshare11 = hourly_avg_ren_share[11], rengenshare12 = hourly_avg_ren_share[12],
        rengenshare13 = hourly_avg_ren_share[13], rengenshare14 = hourly_avg_ren_share[14], rengenshare15 = hourly_avg_ren_share[15],
        rengenshare16 = hourly_avg_ren_share[16], rengenshare17 = hourly_avg_ren_share[17], rengenshare18 = hourly_avg_ren_share[18],
        rengenshare19 = hourly_avg_ren_share[19], rengenshare20 = hourly_avg_ren_share[20], rengenshare21 = hourly_avg_ren_share[21],
        rengenshare22 = hourly_avg_ren_share[22], rengenshare23 = hourly_avg_ren_share[23], rengenshare24 = hourly_avg_ren_share[24]
    ))

    # Calculate monthly averages for different variables
    monthly_avg_prices = calculate_monthly_averages(results["price"], new_data.month)
    monthly_avg_batt_out = calculate_monthly_averages(results["battery_gen"], new_data.month)
    monthly_avg_ph_out = calculate_monthly_averages(results["pumped_hydro_gen"], new_data.month)
    monthly_avg_emissions = calculate_monthly_averages(results["direct_emissions"], new_data.month)
    monthly_avg_ren_share = calculate_monthly_averages(results["share_renewable_gen"], new_data.month)

    push!(monthly_new_results, (
        iteration = i,
        # Monthly prices (months 1-12)
        price1 = monthly_avg_prices[1], price2 = monthly_avg_prices[2], price3 = monthly_avg_prices[3],
        price4 = monthly_avg_prices[4], price5 = monthly_avg_prices[5], price6 = monthly_avg_prices[6],
        price7 = monthly_avg_prices[7], price8 = monthly_avg_prices[8], price9 = monthly_avg_prices[9],
        price10 = monthly_avg_prices[10], price11 = monthly_avg_prices[11], price12 = monthly_avg_prices[12],
        
        # Monthly battery output (months 1-12)
        battout1 = monthly_avg_batt_out[1], battout2 = monthly_avg_batt_out[2], battout3 = monthly_avg_batt_out[3],
        battout4 = monthly_avg_batt_out[4], battout5 = monthly_avg_batt_out[5], battout6 = monthly_avg_batt_out[6],
        battout7 = monthly_avg_batt_out[7], battout8 = monthly_avg_batt_out[8], battout9 = monthly_avg_batt_out[9],
        battout10 = monthly_avg_batt_out[10], battout11 = monthly_avg_batt_out[11], battout12 = monthly_avg_batt_out[12],
        
        # Monthly pumped hydro output (months 1-12)
        phout1 = monthly_avg_ph_out[1], phout2 = monthly_avg_ph_out[2], phout3 = monthly_avg_ph_out[3],
        phout4 = monthly_avg_ph_out[4], phout5 = monthly_avg_ph_out[5], phout6 = monthly_avg_ph_out[6],
        phout7 = monthly_avg_ph_out[7], phout8 = monthly_avg_ph_out[8], phout9 = monthly_avg_ph_out[9],
        phout10 = monthly_avg_ph_out[10], phout11 = monthly_avg_ph_out[11], phout12 = monthly_avg_ph_out[12],
        
        # Monthly emissions (months 1-12)
        emissions1 = monthly_avg_emissions[1], emissions2 = monthly_avg_emissions[2], emissions3 = monthly_avg_emissions[3],
        emissions4 = monthly_avg_emissions[4], emissions5 = monthly_avg_emissions[5], emissions6 = monthly_avg_emissions[6],
        emissions7 = monthly_avg_emissions[7], emissions8 = monthly_avg_emissions[8], emissions9 = monthly_avg_emissions[9],
        emissions10 = monthly_avg_emissions[10], emissions11 = monthly_avg_emissions[11], emissions12 = monthly_avg_emissions[12],
        
        # Monthly renewable generation share (months 1-12)
        rengenshare1 = monthly_avg_ren_share[1], rengenshare2 = monthly_avg_ren_share[2], rengenshare3 = monthly_avg_ren_share[3],
        rengenshare4 = monthly_avg_ren_share[4], rengenshare5 = monthly_avg_ren_share[5], rengenshare6 = monthly_avg_ren_share[6],
        rengenshare7 = monthly_avg_ren_share[7], rengenshare8 = monthly_avg_ren_share[8], rengenshare9 = monthly_avg_ren_share[9],
        rengenshare10 = monthly_avg_ren_share[10], rengenshare11 = monthly_avg_ren_share[11], rengenshare12 = monthly_avg_ren_share[12]
    ))
end

# ===== Save Results =====
# Remove the iteration column of all_delta_draws
combined_results = hcat(selected_results, mean_values_new_data, all_delta_draws)

# Save the main results to a CSV file
combined_results_path = string(dirpath, "Results/Baseline/combined_results.csv")
if isfile(combined_results_path)
    existing_combined_results = CSV.read(combined_results_path, DataFrame)
    existing_combined_results = vcat(existing_combined_results, combined_results)
    CSV.write(combined_results_path, existing_combined_results)
else
    CSV.write(combined_results_path, combined_results)
end

# Save hourly and monthly results
hourly_results_path = string(dirpath, "Results/Baseline/hourly_results.csv")
if isfile(hourly_results_path)
    existing_hourly_results = CSV.read(hourly_results_path, DataFrame)
    existing_hourly_results = vcat(existing_hourly_results, hourly_new_results)
    CSV.write(hourly_results_path, existing_hourly_results)
else
    CSV.write(hourly_results_path, hourly_new_results)
end

monthly_results_path = string(dirpath, "Results/Baseline/monthly_results.csv")
if isfile(monthly_results_path)
    existing_monthly_results = CSV.read(monthly_results_path, DataFrame)
    existing_monthly_results = vcat(existing_monthly_results, monthly_new_results)
    CSV.write(monthly_results_path, existing_monthly_results)
else
    CSV.write(monthly_results_path, monthly_new_results)
end
