# This script provides all the other auxiliary functions needed 
# to make the Monte Carlo framework and the model work


# follow the order of the MC loop
...



# ===== 1. Auxiliary function to sample a time window =====
# Instead of solving for an entire year each time, we solve for a randomly selected 
# 7-day window for each month. The idea is to take a "mould" (such subset of one of
# the defined baseline years), which is historical data, which will then be projected.
# This process ensures more variability on our moulds, and so more robustness of results.
# This function cerates the "mould" from historical data

function sample_time_window(hourly_data, baseline_years)
    year = rand(baseline_years)
    day_start = rand(1:21)

    data_year = hourly_data[hourly_data.year .== year, :]
    data_window = filter(row -> row.day >= day_start && row.day < day_start + 7, data_year)

    return data_window, year, day_start
end



# ===== 2. Auxiliary function to pre-process the projection data =====
# We have gathered several all reliable and available estimates on how key variables will
# evolve by 2030. projection_deltas.csv contains those values computed as increments 
# ("deltas") with respect to the values for each of those variables for the baseline years.  
# This function creates a dictionary which avoids having to filter the projections_delta 
# dataset for each variable that we are sampling a projection estimate

function build_sampling_data(projection_deltas, variables_to_draw)
    sampling_data = Dict{String, Tuple{Vector{Float64}, Weights}}()

    for var in variables_to_draw
        subset = projection_deltas[projection_deltas.variable .== var, :]
        sampling_data[var] = (subset.delta, Weights(subset.weight))
    end
    
    # This will be of the form: 
    # "variable_name" => ([delta1, delta2], Weights([weight1, weight2])), ... 
    return sampling_data 
end



# ===== 3. Auxiliary function to sample a projection estimate =====
# In each Monte Carlo Simulation we project a randomly selected subset of the   
# historical data to simulate a possible realization of 2030 in Spain. 
# This function specifies how to do this sample for different kinds of variables

function continuous_sample_delta(deltas::Vector{Float64}, weights::Weights, var::String)

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

    # Set parameters for the rest of the variables we are projecting
    mu = mean(deltas)
    sigma = std(deltas)

    # For those that we do not have information, # we use a small standard deviation 
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
        # KDE with reduced bandwidth for tighter distribution
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
        
        # Further constrain extreme values such that no sample is 10% belo/above the min/max data point
        min_val = minimum(deltas) - 0.1 * abs(minimum(deltas))
        max_val = maximum(deltas) + 0.1 * abs(maximum(deltas))
        return clamp(sampled_val, min_val, max_val)
    end
end



------------
# still to explain !!!

function sample_deltas(variables_to_draw, sampling_data)
    delta_draws = Dict{String, Float64}()

    for var in variables_to_draw
        deltas, weights = sampling_data[var]
        delta_draws[var] = continuous_sample_delta(deltas, weights, var)
    end

    return delta_draws
end


function apply_deltas!(df, delta_draws)
    for (var, delta) in delta_draws
        if !startswith(var, "imp_") && !startswith(var, "exp_")
            df[!, var] .*= (1 + delta)
        end
    end
end
------------------



# ===== xx. Auxiliary function to define iteration-specific parameters =====
# Since the model is designed to be solved for many possible realizations of the future
# some parameters shall be computed for each iteration (the input data will be different in each one)
# The result is a named tuple which is inputed into the model

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


# ===== xx. Auxiliary function to compute hourly averages of key results =====
# we are interested in studying the hourly profile of some key variables
# so we will compute hourly averages for each iteration on these variables

function calculate_hourly_averages(data::Vector{Float64}, hours_per_day::Int=24)
    @assert length(data) % hours_per_day == 0

    # turn input vector to a matrix of (24 × num_days)
    matrix_days_hours = reshape(data, hours_per_day, :)

    # compute averages per rows (hours) and return as a vector
    return vec(mean(matrix_days_hours, dims=2))
end

# ===== xx. Auxiliary function to compute monthly averages of key results =====
# we are interested in studying the monthly profile of some key variables
# so we will compute monthly averages for each iteration on these variables

function calculate_monthly_averages(data::Vector{Float64}, hours_per_month_span::Int=168)
    @assert length(data) % hours_per_month_span == 0

    # turn input vector to a matrix of (168 × num_months)
    matrix_months_hours = reshape(data, hours_per_month_span, :)

    # compute averages per columns (months) and return as a vector
    return vec(mean(matrix_months_hours, dims=1))
end