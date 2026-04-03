# Antiguo código de las MC simulations para el escenario baseline


# load the required libraries
using DataFrames
using CSV
using Statistics
using Distributions
using KernelDensity
using StatsBase
using Printf

# ===== Monte Carlo Set up =====

# define project location
project_root = dirname(@__DIR__)

# load the function that models the electricity market and the other auxiliary functions
include(joinpath(project_root, "scripts", "model_electricity_market.jl"))
include(joinpath(project_root, "scripts", "auxiliar_functions.jl"))

# load the fixed datasets
historical_data        = CSV.read(joinpath(project_root, "data", "historical_data.csv"), DataFrame)
technology_data        = CSV.read(joinpath(project_root, "data", "technology_data.csv"), DataFrame)
projection_deltas_data = CSV.read(joinpath(project_root, "data", "projection_deltas_data.csv"), DataFrame)

# define the scenarios
# FALTA NUCLEAR cap y batteries, lo retomamos luego!

# Revisar:
# 1. Anomalies tiene sentido en elasticity y hydro.
# 2. Sobre el resto: todo lo que es minimum o maximum generation constraints, así como efficiency parameters
#    en vez de dejar un valor a technical_params y un "anomaly" aquí, dejemos sólo un valor aquí 
#    (si cambian entre escenarios) o sólo el valor en technical_params (si no cambian entre escenarios)
# 3. He añadido 4 parámetros más de modo que aquí se centralize TODO lo que cambia entre escenarios

scenarios = DataFrame(
    scenario_name         = ["baseline",       "nuclear",      "optimistic",  "climate change"],
    
    elas_anomaly          = [1.0,             1.0,            2.0,            1.0],
    hydro_anomaly         = [1.0,             1.0,            1.0,            0.8],

    min_ccgas_gen_anomaly = [1.0,             1.0,            0.5,            1.0],
    min_cogen_gen_anomaly = [1.0,             1.0,            0.5,            1.0],
    min_oil_gen_anomaly   = [1.0,             1.0,            0.5,            1.0],
    other_ren_max_anomaly = [1.0,             1.0,            1.2,            1.0],
    ren_waste_max_anomaly = [1.0,             1.0,            1.2,            1.0],

    ph_eff_anomaly        = [1.0,             1.0,            1.025,          1.0],
    batt_eff_anomaly      = [1.0,             1.0,            1.025,          1.0],

    coal_phase_out        = [true,            true,           true,          true],
    nuclear_phase_out     = [true,            false,          true,          true],
    batt_cap_multiplier   = [1.0,             0.75,           1.25,           1.0],
    ren_cap_multiplier    = [1.0,             0.9,            1.1,            1.0]
)

scenario_names = scenarios.scenario_name
scenario_dict = Dict(scen => scenarios[i, :] for (i, scen) in enumerate(scenario_names))


# define monte carlo parameters

baseline_years = [2023, 2024]
variables_to_draw = [
    "residential_demand_gwh", "commercial_demand_gwh", "industrial_demand_gwh", 
    "coal_cap_gw", "combined_cycle_cap_gw", "gas_turbine_cap_gw", "vapor_turbine_cap_gw", "cogeneration_cap_gw", "diesel_cap_gw", 
    "nonrenewable_waste_cap_gw", "nuclear_cap_gw", "conventional_hydro_cap_gw", "run_of_river_hydro_cap_gw", "pumped_hydro_turbine_cap_gw", 
    "solar_pv_cap_gw", "solar_thermal_cap_gw", "wind_cap_gw", "other_renewable_cap_gw", "renewable_waste_cap_gw", "batteries_cap_gw",
    "cost_coal_eur_gwh", "cost_gas_eur_gwh", "cost_diesel_eur_gwh", "cost_uranium_eur_gwh", "eu_ets_price_eur_tco2",
]

# run build_deltas_dictionary once to create the dictionary
deltas_dictionary = build_deltas_dictionary(projection_deltas_data, variables_to_draw)



# ===== Monte Carlo Simulation Loop =====

num_iterations = 10000

# define containers of results
main_results      = Dict{String, Vector{NamedTuple}}()   
hourly_profiles   = Dict{String, Vector{NamedTuple}}()
monthly_profiles  = Dict{String, Vector{NamedTuple}}()
delta_draws       = Dict{String, Vector{NamedTuple}}()
inputs_realized   = Dict{String, Vector{NamedTuple}}()

for scen in scenario_names
    main_results[scen]     = Vector{NamedTuple}(undef, num_iterations)
    hourly_profiles[scen]  = Vector{NamedTuple}(undef, num_iterations)
    monthly_profiles[scen] = Vector{NamedTuple}(undef, num_iterations)
    delta_draws[scen]      = Vector{NamedTuple}(undef, num_iterations)
    inputs_realized[scen]  = Vector{NamedTuple}(undef, num_iterations)
end

for scen in scenario_names

    # define scenario-specific parameters
    scenario_params = scenario_dict[scen]  

    for iter in 1:num_iterations

        @printf("Scenario %s, iteration %d of %d\n", scen, iter, num_iterations)

        # 1. Sample time window from historical data
        sampled_window_data, year, day_start = sample_time_window(historical_data, baseline_years)

        # 2. Sample deltas for this iteration
        delta_draws_iter = sample_deltas(
            variables_to_draw,      # vector of variables to be scaled
            deltas_dictionary,      # dictionary with projection deltas per variable to draw
            scenario_params,        # scenario-specific parameters with specific rules to modify some of the delta draws
            )

        # 3. Apply deltas to the sampled data
        apply_deltas!(sampled_window_data, delta_draws_iter)

        # 4. Compute iteration-specific parameters
        iteration_params = compute_iteration_params(
            projected = sampled_window_data,    # hourly projected data for 2030
            technical = technical_params ,      # model calibration parameters shared across scenarios
            scenario  = scenario_params         # scenario-specific parameters
            )

        # 5. Solve the model
        results = dispatch_electricity_market(
            projected  = sampled_window_data,    # hourly projected data for 2030
            technology = technology_data,        # fixed technical and economic parameters by generation technology
            technical  = technical_params ,      # model calibration parameters shared across scenarios
            scenario   = scenario_params,        # scenario-specific parameters
            iteration  = iteration_params        # iteration-specific parameters
            )

        # 6. Store all the results 
        store_results!(
            # these are the id of each iteration run 
            scen, 
            iter,

            # these are the parameters needed to run the function
            results, 
            sampled_window_data, 
            delta_draws_iter,

            # these are the pre-allocated containers that are updated by the function
            main_results, 
            hourly_profiles, 
            monthly_profiles,
            delta_draws, 
            inputs_realized
        )

    end
end



# ===== Save Results =====

# These results will be saved into output/detailed_results, as it containes iteration-specific results
# Due to storage constraints, we cannot upload these into this repository, but if you want to see the detailed
# resutls, you can run the Monte Carlo simulations in your computer you will be capable to store them with this code

# For the paper purposes, another script generates the min results per iteration which are available in
# this repository at output/summarized_results


detailed_dir = joinpath(project_root, "output", "detailed_results")

for scen in scenario_names
    # main results
    CSV.write(joinpath(detailed_dir, "$(scen)_main_results.csv"), DataFrame(main_results[scen]))
    
    # hourly profiles
    CSV.write(joinpath(detailed_dir, "$(scen)_hourly_profiles.csv"), DataFrame(hourly_profiles[scen]))
    
    # monthly profiles
    CSV.write(joinpath(detailed_dir, "$(scen)_monthly_profiles.csv"), DataFrame(monthly_profiles[scen]))
    
    # delta draws
    CSV.write(joinpath(detailed_dir, "$(scen)_delta_draws.csv"), DataFrame(delta_draws[scen]))
    
    # demand/capacity/cost inputs
    CSV.write(joinpath(detailed_dir, "$(scen)_inputs_realized.csv"), DataFrame(inputs_realized[scen]))
end