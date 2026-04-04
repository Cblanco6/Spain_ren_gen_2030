# This script provides the auxiliary functions needed to make the Monte Carlo framework and the model work

# In particular, defines the following functions:

# 1. sample_time_window to select the 7-day window and baseline year of the dataset to be projected
# 2. build_deltas_dictionary to create a dictionary of the projection estimates and weights to each variable (run once)
# 3. sampling_procedure defines the sampling procedure for the projection  stimates
# 4. sample_deltas applies the sampling procedure to get a dictionary of draws for each iteration
# 5. apply_deltas! projects the sampled_window_data to 2030 by applying the delta draws
# 6. set_iteration_specific_parameters to define some parameters specific to each iteration hat are passed to the model
# 7. calculate_hourly_averages computes averages to retrieve hourly profiles of a selection of variables
# 8. calculate_monthly_averages computes averages to retrieve monthly profiles of a selection of variables
# 9. store_results! stores all the results from each iteration into different pre-defined containers



# ===== 1. Auxiliary function to sample a time window =====
# Instead of solving for an entire year, we solve for a randomly selected 7-day window each month.
# The idea is to randomly select a "mould" of historical data, which is then projected.
# This process ensures more variability on our moulds, so more robustness of results.
# This function creates the mentioned "mould" from historical data, which we call sampled_window_data.

function sample_time_window(
    historical_data::DataFrame,
    baseline_years::Vector{Int}
    )

    year = rand(baseline_years)
    day_start = rand(1:21)

    sampled_window_data = filter(row ->
        row.year == year &&
        row.day >= day_start &&
        row.day < day_start + 7,
        historical_data
    )

    return sampled_window_data, year, day_start
end



# ===== 2. Auxiliary function to pre-process the projection data =====
# We have gathered all available and reliable estimates on how key variables will evolve by 2030. 
# projection_deltas.csv contains those values computed as increments ("deltas")
# with respect to the values for each of those variables for the baseline years.  
# This function is run once and creates a dictionary with deltas and weights 
# for each variable, avoiding having to filter the projections_delta in each iteration.

function build_deltas_dictionary(
    projection_deltas::DataFrame,
    variables_to_draw::Vector{String}
    )
    
    deltas_dictionary = Dict{String, Tuple{Vector{Float64}, Weights}}()

    for var in variables_to_draw
        subset = projection_deltas[projection_deltas.variable .== var, :]
        deltas_dictionary[var] = (subset.delta, Weights(subset.weight))
    end
    
    return deltas_dictionary 
end


# The deltas_dictionary will be of the form: 
# "variable_name1" => ([v1_delta_1, v1_delta_2], Weights([v1_weight_1, v1_weight_2])),  
# "variable_name2" => ([v2_delta_1, v2_delta_2, v2_delta_3], Weights([v3_weight_1, v2_weight_2, v2_weight_3])), 
# ... 



# ===== 3. Auxiliary function to define the sampling process =====
# In each Monte Carlo Simulation we project a randomly selected subset of the   
# historical data ("mould") to simulate a possible realization of 2030 in Spain. 
# This function specifies how this sampling process is done for different kinds of variables.

function sampling_procedure(
    deltas::Vector{Float64}, 
    weights::Weights, 
    var::String
    )

    # ESTO QUIZÁS LO HEMOS DE QUITAR COMPLETAMENTE (ns si lo seguimos modelando)
    # Discrete sampling for interconnectors capacity
    if startswith(var, "imp_") || startswith(var, "exp_")
        normalized_weights = weights ./ sum(weights)
        idx = sample(1:length(deltas), Weights(normalized_weights))
        return deltas[idx]
    end

    # Define parameters for the rest of the variables we are projecting
    mu = mean(deltas)
    sigma = std(deltas)

    # For those that we do not have information, we use a small standard deviation 
    if mu == 0 && sigma == 0
        std_dev = 0.05
        return rand(Normal(0.0, std_dev))
    end

    # For those that have small spread, we sample from a normal distribution
    small_std_threshold = 0.05
    if sigma < small_std_threshold
        return rand(Normal(mu, sigma))

    # For those with larger spread, we use sample from a kernel density distribution
    else
        # Default bandwidth is approximately 1.06 * std(data) * n^(-1/5)
        # We'll use a smaller factor to reduce spread
        bandwidth = 0.75 * std(deltas) * length(deltas)^(-1/5)
        kde_est = KernelDensity.kde(deltas, weights = weights, bandwidth = bandwidth)
        
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
        
        # Further constrain extreme values such that no sample is 10% below/above the min/max data point
        min_val = minimum(deltas) - 0.1 * abs(minimum(deltas))
        max_val = maximum(deltas) + 0.1 * abs(maximum(deltas))
        return clamp(sampled_val, min_val, max_val)
    end
end



# ===== 4. Auxiliary function to sample a projection estimate for each variable to draw =====
# This function applies the sampling_procedure function to each of the variables in variables_to_draw.
# Returns another dictionary, delta draws, with the specific values to project the "mould" to 2030.

function sample_deltas(
    variables_to_draw::Vector{String},
    deltas_dictionary::Dict{String, Tuple{Vector{Float64}, Weights}},
    scenario::NamedTuple
)
    
    delta_draws = Dict{String, Float64}()

    for var in variables_to_draw

        # 1. Draw provisional delta
        deltas, weights = deltas_dictionary[var]
        delta_draw = sampling_procedure(deltas, weights, var)

        # 2. Apply scenario adjustments
        delta_adjusted = delta_draw

        # Coal phase-out
        if var == "coal_cap_mw" && scenario.coal_phase_out
            delta_adjusted = -1.0

        # Nuclear: avoid phase-out
        elseif var == "nuclear_cap_mw" && !scenario.nuclear_phase_out
            delta_adjusted = 0.0

        # Batteries scaling
        elseif var == "batteries_cap_mw"
            delta_adjusted *= scenario.battery_cap_multiplier

        # Renewables scaling 
        elseif occursin("_cap_mw", var) && startswith(var, "ren")
            delta_adjusted *= scenario.renewable_cap_multiplier
        end

        # 3. Store final delta
        delta_draws[var] = delta_adjusted
    end

    return delta_draws
end

# The delta_draws dictionary will be of the form: 
# "variable_name1" => v1_delta_draw,  
# "variable_name2" => v2_delta_draw, 
# ... 


# ===== 5. Auxiliary function to project the "mould" dataset to 2030  =====
# This function projects the "mould" (sampled_window_data) to 2030 by applying
# the sampling_procedure function to each of the variables in variables_to_draw.
# Returns the "hypothetical 2030 realization" to input into the model in each iteration.
# The ! at the end of the name is a Julia convention to signal that some of the arguments will be modified

function apply_deltas!(
    sampled_window_data::DataFrame,
    delta_draws::Dict{String, Float64}
    )
    for (var, delta) in delta_draws
        sampled_window_data[!, var] .*= (1 + delta)
    end
end

# ===== 6. Auxiliary function to define iteration-specific parameters =====
# Since the model is designed to be solved for many possible realizations of the future,
# some parameters shall be computed for each iteration (the input data will be different in each one).
# That is exactly what this function does, and returns a named tuple which is inputed into the model

function set_iteration_specific_parameters(
    projected::DataFrame,        # hourly projected data for 2030
    technical::NamedTuple,       # technical parameters shared across scenarios
    scenario::NamedTuple,        # scenario-specific parameters
   )

    T = nrow(projected)

    # Set minimum price to be 0.5 such that demand functions are well defined
    projected.spot_price_eur_gwh .= ifelse.(projected.spot_price_eur_gwh .<= 0.5, 0.5, projected.spot_price_eur_gwh)

    # Parameters defining domestic demand functions are re-computed in each simulation
    b_residential = technical.elas_residential * scenario.elas_anomaly * projected.residential_demand_gwh ./ projected.spot_price_eur_gwh
    b_commercial  = technical.elas_commercial  * scenario.elas_anomaly * projected.commercial_demand_gwh  ./ projected.spot_price_eur_gwh
    b_industrial  = technical.elas_industrial  * scenario.elas_anomaly * projected.industrial_demand_gwh  ./ projected.spot_price_eur_gwh

    a_residential = projected.residential_demand_gwh + b_residential .* projected.spot_price_eur_gwh
    a_commercial  = projected.commercial_demand_gwh  + b_commercial  .* projected.spot_price_eur_gwh
    a_industrial  = projected.industrial_demand_gwh  + b_industrial  .* projected.spot_price_eur_gwh

    # Hydro bundles for weekly allocation maximization
    bundle_size = 168   # number of hours in a week
    total_hours = nrow(projected)
    n_bundles   = div(total_hours, bundle_size)

    starts  = [1 + (w - 1) * bundle_size for w in 1:n_bundles]
    bundles = [s:s + bundle_size - 1 for s in starts[1:n_bundles]]

    hydro_min_weekly = [minimum(projected.conventional_hydro_gen_gwh[b]) for b in bundles]
    hydro_max_weekly = [maximum(projected.conventional_hydro_gen_gwh[b]) for b in bundles]

    hydro_min_hourly = zeros(Float64, total_hours)
    hydro_max_hourly = zeros(Float64, total_hours)

    for (w, b) in enumerate(bundles)
        hydro_min_hourly[b] .= hydro_min_weekly[w]
        hydro_max_hourly[b] .= hydro_max_weekly[w]
    end

    hydro_weekly_totals = [sum(projected.conventional_hydro_gen_gwh[b]) for b in bundles]

    # Run of river hydro: hour indices by seasonal production group
    hours_high_ror     = [t for t in 1:T if projected.month[t] in (1, 2, 12)]
    hours_med_high_ror = [t for t in 1:T if projected.month[t] in (3, 4, 5, 6)]
    hours_med_low_ror  = [t for t in 1:T if projected.month[t] in (7, 11)]
    hours_low_ror      = [t for t in 1:T if projected.month[t] in (8, 9, 10)]

    return (;
        a_residential, b_residential,
        a_commercial,  b_commercial,
        a_industrial,  b_industrial,
        n_bundles, bundles,
        hydro_min_hourly, hydro_max_hourly, hydro_weekly_totals,
        hours_high_ror, hours_med_high_ror, hours_med_low_ror, hours_low_ror
    )

end


# ===== 7. Auxiliary function to compute hourly averages of key results =====
# we are interested in studying the hourly profile of some key variables
# so we will compute hourly averages for each iteration on these variables

function calculate_hourly_averages(
    data::Vector{Float64}, 
    hours_per_day::Int=24
    )

    @assert length(data) % hours_per_day == 0 

    # turn input vector to a matrix of (24 × num_days)
    matrix_days_hours = reshape(data, hours_per_day, :)

    # compute averages per rows (hours) and return as a vector
    return vec(mean(matrix_days_hours, dims=2))
end

# ===== 8. Auxiliary function to compute monthly averages of key results =====
# we are interested in studying the monthly profile of some key variables
# so we will compute monthly averages for each iteration on these variables

function calculate_monthly_averages(
    data::Vector{Float64}, 
    hours_per_month_span::Int=168
    )
    
    @assert length(data) % hours_per_month_span == 0

    # turn input vector to a matrix of (168 × num_months)
    matrix_months_hours = reshape(data, hours_per_month_span, :)

    # compute averages per columns (months) and return as a vector
    return vec(mean(matrix_months_hours, dims=1))
end



# ===== 8. Auxiliary function to store results of each iteration =====
# This function defines how we store all the results in the loop 

function store_results!(
    iter::Int,
    scen::String,
    results::Dict,                 # the output of dispatch_electricity_market
    delta_draws::NamedTuple,      
    projected_data::DataFrame,   
    main_results::Dict,            # pre-allocated container to save main results
    hourly_profiles::Dict,         # pre-allocated container to save hourly profiles
    monthly_profiles::Dict,        # pre-allocated container to save monthly profiles
    delta_draws_container::Dict,   # pre-allocated container to save the specific delta draws for each iteration 
    inputs_realized::Dict          # pre-allocated container to save demand, capacity and costs inputed to the model in each iteration
    )

    # factor de anualización según número de filas del sample
    annual_factor = 365.25 * 24 / nrow(projected_data)

    # ----- main_results -----
    main_results[scen][iter] = (
        iteration = iter,

        # Prices
        avg_price = results["avg_price"],
        max_price = results["max_price"],
        min_price = results["min_price"],
        std_price = results["std_price"],

        # Total welfare
        consumer_surplus = sum(results["consumer_surplus"]) * annual_factor,
        producer_surplus = sum(results["producer_surplus"]) * annual_factor,
        total_cost       = sum(results["total_cost"])       * annual_factor,
        net_welfare      = sum(results["net_welfare"])      * annual_factor,

        # Total demand
        residential_demand = sum(results["residential_demand"]) * annual_factor,
        commercial_demand  = sum(results["commercial_demand"])  * annual_factor,
        industrial_demand  = sum(results["industrial_demand"])  * annual_factor,
        total_demand       = sum(results["total_demand"])       * annual_factor,

        # Total generation by technology
        coal_gen                  = sum(results["coal_gen"])                * annual_factor,
        combined_cycle_gen        = sum(results["combined_cycle_gen"])      * annual_factor,
        gas_turbine_gen           = sum(results["gas_turbine_gen"])         * annual_factor,
        vapor_turbine_gen         = sum(results["vapor_turbine_gen"])       * annual_factor,
        cogeneration_gen          = sum(results["cogeneration_gen"])        * annual_factor,
        diesel_gen                = sum(results["diesel_gen"])              * annual_factor,
        non_renewable_waste_gen   = sum(results["non_renewable_waste_gen"]) * annual_factor,
        nuclear_gen               = sum(results["nuclear_gen"])             * annual_factor,
        conventional_hydro_gen    = sum(results["conventional_hydro_gen"])  * annual_factor,
        run_of_river_hydro_gen    = sum(results["run_of_river_hydro_gen"])  * annual_factor,
        solar_pv_gen              = sum(results["solar_pv_gen"])            * annual_factor,
        solar_thermal_gen         = sum(results["solar_thermal_gen"])       * annual_factor,
        wind_gen                  = sum(results["wind_gen"])                * annual_factor,
        other_renewable_gen       = sum(results["other_renewable_gen"])     * annual_factor,
        renewable_waste_gen       = sum(results["renewable_waste_gen"])     * annual_factor,

        # Total storage flows
        pumped_hydro_pumping = sum(results["pumped_hydro_pumping"]) * annual_factor,
        pumped_hydro_out     = sum(results["pumped_hydro_out"])     * annual_factor,
        battery_charge       = sum(results["battery_charge"])       * annual_factor,
        battery_out          = sum(results["battery_out"])          * annual_factor,

        # Average and maximum storage stock
        mean_ph_stock        = mean(results["pumped_hydro_storage"]),
        mean_batt_stock      = mean(results["battery_storage"]),
        max_ph_stock         = maximum(results["pumped_hydro_storage"]),
        max_batt_stock       = maximum(results["battery_storage"]),

        # Aggregated total generation
        total_generation     = sum(results["total_generation"])  * annual_factor,
        renewable_gen        = sum(results["renewable_gen"])     * annual_factor,
        low_carbon_gen       = sum(results["low_carbon_gen"])    * annual_factor,
        non_renewable_gen    = sum(results["non_renewable_gen"]) * annual_factor,
        storage_out          = sum(results["storage_out"])       * annual_factor,
        share_ren_ph_in      = results["share_ren_ph_in"],
        share_ren_batt_in    = results["share_ren_batt_in"],

        # Statistical measures of aggregated generation
        share_renewable_gen  = sum(results["renewable_gen"])         / sum(results["total_generation"]),
        share_low_carbon_gen = sum(results["low_carbon_gen"])        / sum(results["total_generation"]),
        share_storage_out    = sum(results["storage_out"])           / sum(results["total_generation"]),
        share_min_non_ren    = sum(results["min_non_renewable_gen"]) / sum(results["total_generation"]),
        min_share_ren        = minimum(results["share_renewable_gen"]),
        max_share_ren        = maximum(results["share_renewable_gen"]),

        # Imports / exports
        imports_FRA = sum(results["imports_FRA"]) * annual_factor,
        imports_POR = sum(results["imports_POR"]) * annual_factor,
        imports_MOR = sum(results["imports_MOR"]) * annual_factor,
        exports_FRA = sum(results["exports_FRA"]) * annual_factor,
        exports_POR = sum(results["exports_POR"]) * annual_factor,
        exports_MOR = sum(results["exports_MOR"]) * annual_factor,

        # Emissions
        lifecycle_emissions = sum(results["lifecycle_emissions"]),
        direct_emissions    = sum(results["direct_emissions"]),

        # Curtailment
        curt_solar_pv      = results["curtailment_solar_pv"],
        curt_solar_thermal = results["curtailment_solar_thermal"],
        curt_wind          = results["curtailment_wind"]
    )

    # ----- hourly_profiles -----
    hourly_profiles[scen][iter] = (
        price     = calculate_hourly_averages(results["price"]),
        ph_in     = calculate_hourly_averages(results["pumped_hydro_pumping"]),
        ph_out    = calculate_hourly_averages(results["pumped_hydro_out"]),
        batt_in   = calculate_hourly_averages(results["battery_charge"]),
        batt_out  = calculate_hourly_averages(results["battery_out"]),        
        emissions = calculate_hourly_averages(results["direct_emissions"]),
        ren_share = calculate_hourly_averages(results["share_renewable_gen"]),
        lc_share  = calculate_hourly_averages(results["share_low_carbon_gen"])
    )

    # ----- monthly_profiles -----
    monthly_profiles[scen][iter] = (
        price     = calculate_monthly_averages(results["price"]),
        ph_in     = calculate_monthly_averages(results["pumped_hydro_pumping"]),
        ph_out    = calculate_monthly_averages(results["pumped_hydro_out"]),
        batt_in   = calculate_monthly_averages(results["battery_charge"]),
        batt_out  = calculate_monthly_averages(results["battery_out"]),     
        emissions = calculate_monthly_averages(results["direct_emissions"]),
        ren_share = calculate_monthly_averages(results["share_renewable_gen"]),
        lc_share  = calculate_monthly_averages(results["share_low_carbon_gen"])
    )

    # ----- delta_draws -----
    delta_draws_container[scen][iter] = delta_draws

    # ----- inputs_realized -----
    inputs_realized[scen][iter] = (
        iteration = iter,

        # Demand
        residential_demand_gwh = mean(projected_data.residential_demand_gwh),
        commercial_demand_gwh  = mean(projected_data.commercial_demand_gwh),
        industrial_demand_gwh  = mean(projected_data.industrial_demand_gwh),

        # Installed capacity
        coal_cap_gw               = mean(projected_data.coal_cap_gw),
        combined_cycle_cap_gw     = mean(projected_data.combined_cycle_cap_gw),
        gas_turbine_cap_gw        = mean(projected_data.gas_turbine_cap_gw),
        vapor_turbine_cap_gw      = mean(projected_data.vapor_turbine_cap_gw),
        cogeneration_cap_gw       = mean(projected_data.cogeneration_cap_gw),
        diesel_cap_gw             = mean(projected_data.diesel_cap_gw),
        nonrenewable_waste_cap_gw = mean(projected_data.nonrenewable_waste_cap_gw),
        nuclear_cap_gw            = mean(projected_data.nuclear_cap_gw),
        conventional_hydro_cap_gw = mean(projected_data.conventional_hydro_cap_gw),
        run_of_river_hydro_cap_gw = mean(projected_data.run_of_river_hydro_cap_gw),
        solar_pv_cap_gw           = mean(projected_data.solar_pv_cap_gw),
        solar_thermal_cap_gw      = mean(projected_data.solar_thermal_cap_gw),
        wind_cap_gw               = mean(projected_data.wind_cap_gw),
        other_renewable_cap_gw    = mean(projected_data.other_renewable_cap_gw),
        renewable_waste_cap_gw    = mean(projected_data.renewable_waste_cap_gw),
        pumped_hydro_turb_cap_gw  = mean(projected_data.pumped_hydro_cap_gw),
        batteries_cap_gw          = mean(projected_data.batteries_cap_gw),
        
        # Fuel and EU ETS costs
        cost_coal_eur_gwh     = mean(projected_data.cost_coal_eur_gwh),
        cost_gas_eur_gwh      = mean(projected_data.cost_gas_eur_gwh),
        cost_diesel_eur_gwh   = mean(projected_data.cost_diesel_eur_gwh),
        cost_uranium_eur_gwh  = mean(projected_data.cost_uranium_eur_gwh),
        eu_ets_price_eur_tco2 = mean(projected_data.eu_ets_price_eur_tco2)
    )

end