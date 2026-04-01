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



# ===== 1. Auxiliary function to sample a time window =====
# Instead of solving for an entire year, we solve for a randomly selected 7-day window each month.
# The idea is to randomly select a "mould" of historical data, which is then projected.
# This process ensures more variability on our moulds, so more robustness of results.
# This function creates the mentioned "mould" from historical data, which we call sampled_window_data.

function sample_time_window(
    hourly_data::DataFrame,
    baseline_years::Vector{Int}
    )

    year = rand(baseline_years)
    day_start = rand(1:21)

    sampled_window_data = filter(row ->
        row.year == year &&
        row.day >= day_start &&
        row.day < day_start + 7,
        hourly_data
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

    # Special case for coal capacity (100% sure of phase out)
    if var == "coal_cap_gw"
        return deltas[1]
    end

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
    deltas_dictionary::Dict{String, Tuple{Vector{Float64}, Weights}}
    )
    
    delta_draws = Dict{String, Float64}()

    for var in variables_to_draw
        # retive deltas and weights from the deltas_dictionary
        deltas, weights = deltas_dictionary[var]
        # apply sampling_procedure to draw a specific delta for each variable
        delta_draws[var] = sampling_procedure(deltas, weights, var)
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