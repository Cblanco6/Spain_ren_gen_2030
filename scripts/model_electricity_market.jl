# This script defines the function that simulates the Spanish electricity market

# load the required libraries
using DataFrames
using CSV
using Statistics
using JuMP
using Gurobi


# define technical parameters (model calibration)
technical = (
    elas_residential = 0.015,
    elas_commercial = 0.03,
    elas_industrial = 0.05,
    grid_loss_factor = 0.015,
    coal_min = 0.15,
    coal_max = 0.65,
    coal_ramp = 0.05,
    ccgt_min = 0.05,
    ccgt_max = 1.0,
    ccgt_ramp = 0.25,
    cogen_min = 0.15,
    cogen_max = 0.6,
    cogen_ramp = 0.1,
    diesel_min = 0.2,
    non_ren_waste_max = 0.6,
    non_ren_waste_ramp = 0.01,
    conv_hydro_ramp = 0.1,
    ror_lo_high = 0.3,
    ror_hi_high = 0.5,
    ror_lo_med_high = 0.2,
    ror_hi_med_high = 0.4,
    ror_lo_med_low = 0.15,
    ror_hi_med_low = 0.3,
    ror_lo_low = 0.1,
    ror_hi_low = 0.15,
    ror_ramp = 0.2,
    ph_storage_cap_gwh = 50.0,
    ph_roundtrip_eff = 0.75,
    batt_roundtrip_eff = 0.9,
    batt_decay = 0.001,
    batt_duration = 4.0,
    other_ren_min = 0.25,
    other_ren_max = 0.6,
    other_ren_ramp = 0.05,
    ren_waste_min = 0.0,
    ren_waste_max = 0.65,
    ren_waste_ramp = 0.05
)



# ===== Definition of the model: dispatch_electricity_market function =====
function dispatch_electricity_market(
    projected::DataFrame,        # hourly projected data for 2030
    technology::DataFrame,       # fixed technical and economic parameters by generation technology
    technical::NamedTuple,       # technical parameters shared across scenarios
    scenario::NamedTuple,        # scenario-specific parameters
    iteration::NamedTuple        # iteration-specific parameters
    )

    # Initialize the model solver
    model = Model(Gurobi.Optimizer)
    set_optimizer_attribute(model, "OutputFlag", 0)
    set_optimizer_attribute(model, "TimeLimit", 300)
    set_optimizer_attribute(model, "MIPGap", 0.03)

    # Set indices
    T = nrow(projected)
    years_solving = T / (365.25 * 24)
    I = nrow(technology)
    S = 3   # residential, commercial, industrial
    C = 3   # Portugal, France, Morocco

    # Define variables
    @variable(model, price[1:T] >= 0)
    @variable(model, demand[1:T, 1:S] >= 0)
    @variable(model, imports[1:T, 1:C] >= 0)
    @variable(model, exports[1:T, 1:C] >= 0)
    @variable(model, quantity[1:T, 1:I] >= 0)
    @variable(model, costs[1:T] >= 0)
    @variable(model, consumer_surplus[1:T])
    @variable(model, producer_revenue[1:T])
    @variable(model, running_costs[1:T] >= 0)
    @variable(model, fuel_costs[1:T] >= 0)
    @variable(model, emissions_costs[1:T] >= 0)
    @variable(model, direct_emissions[1:T] >= 0)
    @variable(model, lifecycle_emissions[1:T] >= 0)
    @variable(model, min_non_ren_gen[1:T] >= 0)
    @variable(model, ph_in[1:T] >= 0)
    @variable(model, ph_out[1:T] >= 0)
    @variable(model, ph_stock[1:T] >= 0)
    @variable(model, batt_in[1:T] >= 0)
    @variable(model, batt_out[1:T] >= 0)
    @variable(model, batt_stock[1:T] >= 0)

    # Objective: maximize social welfare
    @objective(model, Max, sum(consumer_surplus[t] + producer_revenue[t] - costs[t] for t in 1:T) / T)

    # Market clearing: generation + imports - exports = demand + storage charging
    @constraint(model, balance[t=1:T],
        sum(quantity[t,i] for i in 1:I) + sum(imports[t,c] for c in 1:C) - sum(exports[t,c] for c in 1:C)
        == (1 + technical.grid_loss_factor) * sum(demand[t,s] for s in 1:S) 
            + batt_in[t] 
            + ph_in[t])

    # Consumer surplus
    @constraint(model, [t=1:T],
        consumer_surplus[t] ==
            demand[t,1]^2 / (2 * iteration.b_residential[t])
            + demand[t,2]^2 / (2 * iteration.b_commercial[t])
            + demand[t,3]^2 / (2 * iteration.b_industrial[t]))

    # Producer revenue
    @constraint(model, [t=1:T],
        producer_revenue[t] ==
            (iteration.a_residential[t] - demand[t,1]) * demand[t,1] / iteration.b_residential[t]
            + (iteration.a_commercial[t]  - demand[t,2]) * demand[t,2] / iteration.b_commercial[t]
            + (iteration.a_industrial[t]  - demand[t,3]) * demand[t,3] / iteration.b_industrial[t])

    # Total costs (fixed + running)
    @constraint(model, [t=1:T],
        costs[t] == sum(technology.fixed_om_eur_gwy[i] * technology.avg_cap_year[i] * years_solving for i in 1:I) / T + running_costs[t])

    # Running costs
    @constraint(model, [t=1:T],
        running_costs[t] == sum(technology.var_om_eur_gwh[i] * quantity[t,i] for i in 1:I) + fuel_costs[t] + emissions_costs[t])

    # Fuel costs
    @constraint(model, [t=1:T],
        fuel_costs[t] ==
            projected.cost_coal_eur_gwh[t]      * quantity[t,1] / technology.efficiency[1]                # coal
            + sum(projected.cost_gas_eur_gwh[t] * quantity[t,j] / technology.efficiency[j] for j in 2:5)  # natural gas
            + projected.cost_diesel_eur_gwh[t]  * quantity[t,6] / technology.efficiency[6]                # diesel
            + projected.cost_uranium_eur_gwh[t] * quantity[t,8] / technology.efficiency[8])               # nuclear

    # Direct emissions (stored only, not used in objective)
    @constraint(model, [t=1:T],
        direct_emissions[t] == sum(technology.fossil_fuel[i] * quantity[t,i] * technology.direct_e_tco2_gwh[i] for i in 1:I))

    # Lifecycle emissions (stored only, not used in objective)
    @constraint(model, [t=1:T],
        lifecycle_emissions[t] == sum(technology.fossil_fuel[i] * quantity[t,i] * technology.lifecycle_e_tco2_gwh[i] for i in 1:I))

    # EU ETS costs
    @constraint(model, [t=1:T],
        emissions_costs[t] == sum(technology.fossil_fuel[i] * projected.eu_ets_price_eur_tco2[t] * quantity[t,i] * technology.direct_e_tco2_gwh[i] for i in 1:I))

    # Demand functions
    @constraint(model, [t=1:T], demand[t,1] == iteration.a_residential[t] - iteration.b_residential[t] * price[t])
    @constraint(model, [t=1:T], demand[t,2] == iteration.a_commercial[t]  - iteration.b_commercial[t]  * price[t])
    @constraint(model, [t=1:T], demand[t,3] == iteration.a_industrial[t]  - iteration.b_industrial[t]  * price[t])

    # Imports and exports (fixed to projected values)
    @constraint(model, [t=1:T], imports[t,1] == projected.imports_France_gwh[t])
    @constraint(model, [t=1:T], imports[t,2] == projected.imports_Portugal_gwh[t])
    @constraint(model, [t=1:T], imports[t,3] == projected.imports_Morocco_gwh[t])
    @constraint(model, [t=1:T], exports[t,1] == projected.exports_France_gwh[t])
    @constraint(model, [t=1:T], exports[t,2] == projected.exports_Portugal_gwh[t])
    @constraint(model, [t=1:T], exports[t,3] == projected.exports_Morocco_gwh[t])

    # ----- Output constraints -----
    
    # Non-renewable sources ----
    # Coal
    @constraint(model, [t=1:T], quantity[t,1] >= technical.coal_min * projected.coal_cap_gw[t])
    @constraint(model, [t=1:T], quantity[t,1] <= technical.coal_max * projected.coal_cap_gw[t])
    @constraint(model, [t=2:T], quantity[t,1] - quantity[t-1,1] >= -technical.coal_ramp * projected.coal_cap_gw[t])
    @constraint(model, [t=2:T], quantity[t,1] - quantity[t-1,1] <= +technical.coal_ramp * projected.coal_cap_gw[t])

    # Combined cycle gas
    @constraint(model, [t=1:T], quantity[t,2] >= technical.ccgt_min * projected.combined_cycle_cap_gw[t] * scenario.min_ccgas_gen_anomaly)
    @constraint(model, [t=1:T], quantity[t,2] <= technical.ccgt_max * projected.combined_cycle_cap_gw[t])
    @constraint(model, [t=2:T], quantity[t,2] - quantity[t-1,2] >= -technical.ccgt_ramp * projected.combined_cycle_cap_gw[t])
    @constraint(model, [t=2:T], quantity[t,2] - quantity[t-1,2] <= +technical.ccgt_ramp * projected.combined_cycle_cap_gw[t])

    # Other gas (simple modeling, not relevant)
    @constraint(model, [t=1:T], quantity[t,3] <= projected.gas_turbine_cap_gw[t])
    @constraint(model, [t=1:T], quantity[t,4] <= projected.vapor_turbine_cap_gw[t])

    # Cogeneration (most likely gas)
    @constraint(model, [t=1:T], quantity[t,5] >= technical.cogen_min * projected.cogeneration_cap_gw[t] * scenario.min_cogen_gen_anomaly)
    @constraint(model, [t=1:T], quantity[t,5] <= technical.cogen_max * projected.cogeneration_cap_gw[t])
    @constraint(model, [t=2:T], quantity[t,5] - quantity[t-1,5] >= -technical.cogen_ramp * projected.cogeneration_cap_gw[t])
    @constraint(model, [t=2:T], quantity[t,5] - quantity[t-1,5] <= +technical.cogen_ramp * projected.cogeneration_cap_gw[t])

    # Oil (minimum constraint reflects Canary Islands baseload)
    @constraint(model, [t=1:T], quantity[t,6] >= technical.diesel_min * projected.diesel_cap_gw[t] * scenario.min_oil_gen_anomaly)
    @constraint(model, [t=1:T], quantity[t,6] <= projected.diesel_cap_gw[t])

    # Non-renewable waste
    @constraint(model, [t=1:T], quantity[t,7] <= technical.non_ren_waste_max * projected.nonrenewable_waste_cap_gw[t])
    @constraint(model, [t=2:T], quantity[t,7] - quantity[t-1,7] >= -technical.non_ren_waste_ramp * projected.nonrenewable_waste_cap_gw[t])
    @constraint(model, [t=2:T], quantity[t,7] - quantity[t-1,7] <= +technical.non_ren_waste_ramp * projected.nonrenewable_waste_cap_gw[t])

    # Nuclear (fixed to self-reported availability)
    @constraint(model, [t=1:T], quantity[t,8] == projected.nuclear_cap_gw[t] * projected.nuclear_cap_factor[t])

    # Minimum non-renewable generation (used to track system flexibility floor)
    @constraint(model, [t=1:T], min_non_ren_gen[t] ==
        technical.coal_min             * projected.coal_cap_gw[t]
        + technical.ccgt_min           * projected.combined_cycle_cap_gw[t]     * scenario.min_ccgas_gen_anomaly
        + technical.cogen_min          * projected.cogeneration_cap_gw[t]       * scenario.min_cogen_gen_anomaly
        + technical.diesel_min         * projected.diesel_cap_gw[t]             * scenario.min_oil_gen_anomaly
        + technical.nonren_waste_min   * projected.nonrenewable_waste_cap_gw[t]
        + projected.nuclear_cap_gw[t]  * projected.nuclear_cap_factor[t])

    # Renewable sources ----
    # Conventional hydro (weekly allocation)
    @constraint(model, [t=1:T], quantity[t,9] >= iteration.hydro_min_hourly[t])
    @constraint(model, [t=1:T], quantity[t,9] <= iteration.hydro_max_hourly[t])
    @constraint(model, [w in 1:iteration.n_bundles], sum(quantity[t,9] for t in iteration.bundles[w]) <=  iteration.hydro_weekly_totals[w] * scenario.hydro_anomaly)
    @constraint(model, [t=2:T], quantity[t,9] - quantity[t-1,9] >= -technical.conv_hydro_ramp * projected.conventional_hydro_cap_gw[t])
    @constraint(model, [t=2:T], quantity[t,9] - quantity[t-1,9] <= +technical.conv_hydro_ramp * projected.conventional_hydro_cap_gw[t])

    # Run of river hydro (seasonal availability bounds)
    @constraint(model, [t in iteration.hours_high_ror],     scenario.hydro_anomaly * technical.ror_lo_high     * projected.run_of_river_hydro_cap_gw[t] <= quantity[t,10] <= scenario.hydro_anomaly * technical.ror_hi_high     * projected.run_of_river_hydro_cap_gw[t])
    @constraint(model, [t in iteration.hours_med_high_ror], scenario.hydro_anomaly * technical.ror_lo_med_high * projected.run_of_river_hydro_cap_gw[t] <= quantity[t,10] <= scenario.hydro_anomaly * technical.ror_hi_med_high * projected.run_of_river_hydro_cap_gw[t])
    @constraint(model, [t in iteration.hours_med_low_ror],  scenario.hydro_anomaly * technical.ror_lo_med_low  * projected.run_of_river_hydro_cap_gw[t] <= quantity[t,10] <= scenario.hydro_anomaly * technical.ror_hi_med_low  * projected.run_of_river_hydro_cap_gw[t])
    @constraint(model, [t in iteration.hours_low_ror],      scenario.hydro_anomaly * technical.ror_lo_low      * projected.run_of_river_hydro_cap_gw[t] <= quantity[t,10] <= scenario.hydro_anomaly * technical.ror_hi_low      * projected.run_of_river_hydro_cap_gw[t])
    @constraint(model, [t=2:T], quantity[t,10] - quantity[t-1,10] >= -technical.ror_ramp * projected.run_of_river_hydro_cap_gw[t])
    @constraint(model, [t=2:T], quantity[t,10] - quantity[t-1,10] <= +technical.ror_ramp * projected.run_of_river_hydro_cap_gw[t])

    # Solar PV, solar thermal, wind (capacity factor bounds)
    @constraint(model, [t=1:T], quantity[t,11] <= projected.solar_pv_cap_gw[t]      * projected.solar_pv_cap_factor[t])
    @constraint(model, [t=1:T], quantity[t,12] <= projected.solar_thermal_cap_gw[t] * projected.solar_thermal_cap_factor[t])
    @constraint(model, [t=1:T], quantity[t,13] <= projected.wind_cap_gw[t]          * projected.wind_cap_factor[t])

    # Other renewables
    @constraint(model, [t=1:T], quantity[t,14] >= technical.other_ren_min * projected.other_renewable_cap_gw[t])
    @constraint(model, [t=1:T], quantity[t,14] <= technical.other_ren_max * projected.other_renewable_cap_gw[t] * scenario.other_ren_max_anomaly)
    @constraint(model, [t=2:T], quantity[t,14] - quantity[t-1,14] >= -technical.other_ren_ramp * projected.other_renewable_cap_gw[t])
    @constraint(model, [t=2:T], quantity[t,14] - quantity[t-1,14] <= +technical.other_ren_ramp * projected.other_renewable_cap_gw[t])

    # Renewable waste
    @constraint(model, [t=1:T], quantity[t,15] >= technical.ren_waste_min * projected.renewable_waste_cap_gw[t])
    @constraint(model, [t=1:T], quantity[t,15] <= technical.ren_waste_max * projected.renewable_waste_cap_gw[t] * scenario.ren_waste_max_anomaly)
    @constraint(model, [t=2:T], quantity[t,15] - quantity[t-1,15] >= -technical.ren_waste_ramp * projected.renewable_waste_cap_gw[t])
    @constraint(model, [t=2:T], quantity[t,15] - quantity[t-1,15] <= +technical.ren_waste_ramp * projected.renewable_waste_cap_gw[t])

    # Storage technologies ----
    # Pumped hydro
    @constraint(model, ph_stock[1] == rand(Uniform(0.2, 0.8)) * technical.ph_storage_cap_gwh)
    @constraint(model, [t=2:T], ph_stock[t] <= technical.ph_storage_cap_gwh)
    @constraint(model, [t=2:T], 
        ph_stock[t] == ph_stock[t-1] 
        + sqrt(technical.ph_roundtrip_eff * scenario.ph_roundtrip_eff_anomaly) * ph_in[t-1] 
        - ph_out[t-1] / sqrt(technical.ph_roundtrip_eff * scenario.ph_roundtrip_eff_anomaly)) 
    @constraint(model, ph_stock[T] == ph_stock[1])
    @constraint(model, [t=1:T], ph_in[t]  <= projected.pumped_hydro_pump_cap_gw[t])
    @constraint(model, [t=1:T], ph_out[t] <= projected.pumped_hydro_turbine_cap_gw[t])
    @constraint(model, [t=1:T], quantity[t,16] == ph_out[t]) 

    # Batteries (4h duration)
    @constraint(model, batt_stock[1] == rand(Uniform(0.2, 0.8)) * technical.batt_duration * projected.batteries_cap_gw[1])
    @constraint(model, [t=2:T], batt_stock[t] <= technical.batt_duration * projected.batteries_cap_gw[t] / sqrt(technical.batt_roundtrip_eff * scenario.batt_roundtrip_eff_anomaly))
    @constraint(model, [t=2:T], 
        batt_stock[t] == (1 - technical.batt_decay) * batt_stock[t-1] 
        + sqrt(technical.batt_roundtrip_eff * scenario.batt_roundtrip_eff_anomaly) * batt_in[t-1] 
        - batt_out[t-1] / sqrt(technical.batt_roundtrip_eff * scenario.batt_roundtrip_eff_anomaly))
    @constraint(model, batt_stock[T] == batt_stock[1])
    @constraint(model, [t=1:T], batt_in[t]  <= projected.batteries_cap_gw[t])
    @constraint(model, [t=1:T], batt_out[t] <= projected.batteries_cap_gw[t])
    @constraint(model, [t=1:T], quantity[t,17] == batt_out[t])
    
    # Solve
    optimize!(model)
    status = JuMP.termination_status(model)

    if status == MOI.OPTIMAL
        # Scalar welfare results
        cons_surplus      = sum(JuMP.value.(consumer_surplus))
        prod_revenue      = sum(JuMP.value.(producer_revenue))
        total_cost        = sum(JuMP.value.(costs))
        prod_surplus      = prod_revenue - total_cost
        net_w             = cons_surplus + prod_surplus

        # Price
        price_vals        = JuMP.value.(price)
        min_p             = minimum(price_vals)
        avg_p             = mean(price_vals)
        max_p             = maximum(price_vals)
        std_p             = std(price_vals)

        # Generation by technology
        q_vals            = JuMP.value.(quantity)
        coal              = q_vals[:, 1]
        cc_gas            = q_vals[:, 2]
        gas_tur           = q_vals[:, 3]
        vapor_tur         = q_vals[:, 4]
        cogeneration      = q_vals[:, 5]
        diesel            = q_vals[:, 6]
        non_ren_w         = q_vals[:, 7]
        nuclear           = q_vals[:, 8]
        conv_hydro        = q_vals[:, 9]
        river_hydro       = q_vals[:, 10]
        solar_pv          = q_vals[:, 11]
        solar_t           = q_vals[:, 12]
        wind              = q_vals[:, 13]
        other_r           = q_vals[:, 14]
        ren_w             = q_vals[:, 15]
        pumped_hydro      = q_vals[:, 16]
        batteries         = q_vals[:, 17]

        # Aggregated generation
        non_ren_gen       = [sum(q_vals[t, 1:8])  for t in 1:T]
        ren_gen           = [sum(q_vals[t, 9:15]) for t in 1:T]
        low_c_gen         = [sum(q_vals[t, 8:15]) for t in 1:T]
        sto_out           = [sum(q_vals[t, 16:17]) for t in 1:T]
        total_gen         = ren_gen .+ non_ren_gen
        share_ren_gen     = ren_gen ./ total_gen
        share_lc_gen      = low_c_gen ./ total_gen
        share_sto         = sto_out ./ total_gen

        # Pumped hydro
        ph_in_vals        = JuMP.value.(ph_in)
        ph_stock_vals     = JuMP.value.(ph_stock)

        # Batteries
        batt_in_vals        = JuMP.value.(batt_in)
        batt_stock_vals     = JuMP.value.(batt_stock)

        # Minimum non-renewable generation (constraint variable)
        min_non_ren_vals  = JuMP.value.(min_non_ren_gen)

        # Demand
        d_vals            = JuMP.value.(demand)
        res_d             = d_vals[:, 1]
        com_d             = d_vals[:, 2]
        ind_d             = d_vals[:, 3]
        total_d           = [sum(d_vals[t, :]) for t in 1:T]

        # Imports and exports
        imp_vals          = JuMP.value.(imports)
        imp_fra           = imp_vals[:, 1]
        imp_por           = imp_vals[:, 2]
        imp_mor           = imp_vals[:, 3]

        exports_vals      = JuMP.value.(exports)
        exp_fra           = exports_vals[:, 1]
        exp_por           = exports_vals[:, 2]
        exp_mor           = exports_vals[:, 3]   

        # Emissions
        direct_e          = JuMP.value.(direct_emissions)
        life_e            = JuMP.value.(lifecycle_emissions)

        # Curtailment
        curt_solar_pv      = 1.0 - sum(q_vals[t,11] for t in 1:T) / sum(projected.solar_pv_cap_gw[t]      * projected.solar_pv_cap_factor[t]      for t in 1:T)
        curt_solar_thermal = 1.0 - sum(q_vals[t,12] for t in 1:T) / sum(projected.solar_thermal_cap_gw[t] * projected.solar_thermal_cap_factor[t] for t in 1:T)
        curt_wind          = 1.0 - sum(q_vals[t,13] for t in 1:T) / sum(projected.wind_cap_gw[t]          * projected.wind_cap_factor[t]          for t in 1:T)

        results = Dict(
            # Prices
            "price"                     => price_vals,
            "avg_price"                 => avg_p,
            "max_price"                 => max_p,
            "min_price"                 => min_p,
            "std_price"                 => std_p,

            # Welfare
            "consumer_surplus"          => cons_surplus,
            "producer_surplus"          => prod_surplus,
            "total_cost"                => total_cost,
            "net_welfare"               => net_w,

            # Demand
            "residential_demand"        => res_d,
            "commercial_demand"         => com_d,
            "industrial_demand"         => ind_d,
            "total_demand"              => total_d,

            # Generation by technology
            "coal_gen"                  => coal,
            "combined_cycle_gen"        => cc_gas,
            "gas_turbine_gen"           => gas_tur,
            "vapor_turbine_gen"         => vapor_tur,
            "cogeneration_gen"          => cogeneration,
            "diesel_gen"                => diesel,
            "non_renewable_waste_gen"   => non_ren_w,
            "nuclear_gen"               => nuclear,
            "conventional_hydro_gen"    => conv_hydro,
            "run_of_river_hydro_gen"    => river_hydro,
            "solar_pv_gen"              => solar_pv,
            "solar_thermal_gen"         => solar_t,
            "wind_gen"                  => wind,
            "other_renewable_gen"       => other_r,
            "renewable_waste_gen"       => ren_w,

            # Storage flows and stock
            "pumped_hydro_pumping"      => ph_in_vals,
            "pumped_hydro_out"          => pumped_hydro,
            "pumped_hydro_storage"      => ph_stock_vals,
            "battery_charge"            => batt_in_vals,
            "battery_out"               => batteries,
            "battery_storage"           => batt_stock_vals,

            # Aggregated generation
            "total_generation"          => total_gen,
            "renewable_gen"             => ren_gen,
            "low_carbon_gen"            => low_c_gen,
            "storage_out"               => sto_out,
            "non_renewable_gen"         => non_ren_gen,
            "min_non_renewable_gen"     => min_non_ren_vals,
            "share_renewable_gen"       => share_ren_gen,
            "share_low_carbon_gen"      => share_lc_gen,            

            # Imports / exports
            "imports_FRA"               => imp_fra,
            "imports_POR"               => imp_por,
            "imports_MOR"               => imp_mor,
            "exports_FRA"               => exp_fra,
            "exports_POR"               => exp_por,
            "exports_MOR"               => exp_mor,

            # Emissions
            "lifecycle_emissions"       => life_e,
            "direct_emissions"          => direct_e,

            # Curtailment
            "curtailment_solar_pv"      => curt_solar_pv,
            "curtailment_solar_thermal" => curt_solar_thermal,
            "curtailment_wind"          => curt_wind
        )

        # Post-process results to set small values to zero to ease comprehension of results
        threshold = 1e-3
        for (k, v) in results
            if isa(v, AbstractArray)
                results[k] = map(x -> abs(x) < threshold ? 0.0 : x, v)
            elseif isa(v, Number)
                results[k] = abs(v) < threshold ? 0.0 : v
            end
        end

    else
        # print status
        @warn "Optimization did not return an optimal solution. Status: $status"
        
        # Set all results to -1 such that the loop continues running
        results = Dict(
        # Prices
        "price"                     => fill(-1.0, T),
        "avg_price"                 => -1.0,
        "max_price"                 => -1.0,
        "min_price"                 => -1.0,
        "std_price"                 => -1.0,

        # Welfare
        "consumer_surplus"          => -1.0,
        "producer_surplus"          => -1.0,
        "total_cost"                => -1.0,
        "net_welfare"               => -1.0,

        # Demand
        "residential_demand"        => fill(-1.0, T),
        "commercial_demand"         => fill(-1.0, T),
        "industrial_demand"         => fill(-1.0, T),
        "total_demand"              => fill(-1.0, T),

        # Generation by technology
        "coal_gen"                  => fill(-1.0, T),
        "combined_cycle_gen"        => fill(-1.0, T),
        "gas_turbine_gen"           => fill(-1.0, T),
        "vapor_turbine_gen"         => fill(-1.0, T),
        "cogeneration_gen"          => fill(-1.0, T),
        "diesel_gen"                => fill(-1.0, T),
        "non_renewable_waste_gen"   => fill(-1.0, T),
        "nuclear_gen"               => fill(-1.0, T),
        "conventional_hydro_gen"    => fill(-1.0, T),
        "run_of_river_hydro_gen"    => fill(-1.0, T),
        "solar_pv_gen"              => fill(-1.0, T),
        "solar_thermal_gen"         => fill(-1.0, T),
        "wind_gen"                  => fill(-1.0, T),
        "other_renewable_gen"       => fill(-1.0, T),
        "renewable_waste_gen"       => fill(-1.0, T),
        
        # Storage flows and stock
        "pumped_hydro_pumping"      => fill(-1.0, T),
        "pumped_hydro_out"          => fill(-1.0, T),
        "pumped_hydro_storage"      => fill(-1.0, T),
        "battery_charge"            => fill(-1.0, T),
        "battery_out"               => fill(-1.0, T),
        "battery_storage"           => fill(-1.0, T),

        # Aggregated generation
        "total_generation"          => fill(-1.0, T),
        "renewable_gen"             => fill(-1.0, T),
        "low_carbon_gen"            => fill(-1.0, T),
        "storage_out"               => fill(-1.0, T),
        "non_renewable_gen"         => fill(-1.0, T),
        "min_non_renewable_gen"     => fill(-1.0, T),
        "share_renewable_gen"       => fill(-1.0, T),
        "share_low_carbon_gen"      => fill(-1.0, T),

        # Imports / exports
        "imports_FRA"               => fill(-1.0, T),
        "imports_POR"               => fill(-1.0, T),
        "imports_MOR"               => fill(-1.0, T),
        "exports_FRA"               => fill(-1.0, T),
        "exports_POR"               => fill(-1.0, T),
        "exports_MOR"               => fill(-1.0, T),

        # Emissions
        "lifecycle_emissions"       => fill(-1.0, T),
        "direct_emissions"          => fill(-1.0, T),
        
        # Curtailment
        "curtailment_solar_pv"      => -1.0,
        "curtailment_solar_thermal" => -1.0,
        "curtailment_wind"          => -1.0
        )
        
    end
    return results
end
