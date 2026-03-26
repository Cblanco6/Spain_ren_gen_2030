# This script defines the parameters and functions needed to run each iteration
# 1. Defines the general parameters
# 2. Defines a function to set iteration-specific parameters such as demand functions parameters
# 3. Defines the model that simulates the Spanish electricity market

# load the required libraries
using DataFrames
using CSV
using XLSX
using JuMP
using Plots
using Printf
using Statistics
using StatsBase
using GLM
using Gurobi
using Distributed
using Distributions
using KernelDensity

(quizás no es necesario!!)
# ===== Set dirpath to your working directory =====
# for us it works better adding a "/" at the end
dirpath = "path/to/your/directory/"


# ===== 1. Set general parameters =====
# Elasticities of demand functions (domestic, imports, exports)
# Values are lower than those usually found in the literature since otherwise the model becomes untractable
elas_residential = 0.015 
elas_commercial = 0.03
elas_industrial = 0.05

# creo que no definimos demand functions para imports y exports
elas_imports = 0.03
elas_exports = 0.03

# Pumped hydro parameters
eff_ph = 0.75
storage_cap_ph = 15.0   # no official figure for this; estiamtes range from 10-20 GW.

# Battery storage parameters
eff_batt = 0.95         # Same for charge and discharge
decay_batt = 0.001      # hourly self-loss ratio

# General grid loss factor
general_loss_factor = 0.015

# ===== 2. Auxiliary function to define iteration-specific parameters =====
# Since the model is designed to be solved for many possible realizations of the future,
# some parameters shall be computed for each iteration (since the input data will be different)

function set_iteration_specific_parameters(
    projected::DataFrame,        # hourly projected data for 2030
    )

    T = nrow(projected)

    # Set minimum price to be 0.5 such that demand functions are well defined
    projected.spot_price_eur_gwh .= ifelse.(projected.spot_price_eur_gwh .<= 0.5, 0.5, projected.spot_price_eur_gwh)

    # Parameters defining domestic demand functions are re-computed in each simulation
    # (pasarlo a GWh!)
    b_residential = elas_residential * projected.residential_demand_gwh ./ projected.spot_price_eur_gwh
    b_commercial = elas_commercial * projected.commercial_demand_gwh ./ projected.spot_price_eur_gwh
    b_industrial = elas_industrial * projected.industrial_demand_gwh ./ projected.spot_price_eur_gwh

    a_residential = projected.residential_demand_gwh + b_residential .* projected.spot_price_eur_gwh
    a_commercial = projected.commercial_demand_gwh + b_commercial .* projected.spot_price_eur_gwh
    a_industrial = projected.industrial_demand_gwh + b_industrial .* projected.spot_price_eur_gwh

    # Hydro bundles for weekly allocation maximization are also defined within the model
    bundle_size = 168   # number of hours in a week
    total_hours = size(projected, 1)
    n_bundles = div(total_hours, bundle_size)
    
    starts = [1 + (w - 1) * bundle_size for w in 1:n_bundles]
    #> [1, 169, 337, 505, ...]  (initial hours every week)

    bundles = [s:s + bundle_size - 1 for s in starts[1:n_bundles]]
    #> [1:168, 169:336, 337:504, ...]  (hour intervals for every week)

    # set lower and upper bound production levels per week
    hydro_min_weekly = [minimum(projected.conventional_hydro_gen_gwh[b]) for b in bundles]
    hydro_max_weekly = [maximum(projected.conventional_hydro_gen_gwh[b]) for b in bundles]

    # set the same minimum and maximum hourly production values for all hours of the week
    hydro_min_hourly = zeros(Float64, total_hours)
    hydro_max_hourly = zeros(Float64, total_hours)

    for (w, b) in enumerate(bundles)
        hydro_min_hourly[b] .= hydro_min_weekly[w]
        hydro_max_hourly[b] .= hydro_max_weekly[w]
    end

    # total hydro production per week (which the model then allocats per hour)
    hydro_weekly_totals = [sum(projected.conventional_hydro_gen_gwh[b]) for b in bundles]

    # define water availability for run of river hydro with a stylized definition of seasonality
    high_prod_months     = [t for t in 1:T if projected.month[t] in (1, 3, 12)]
    med_high_prod_months = [t for t in 1:T if projected.month[t] in (2, 4, 5, 6)]
    med_low_prod_months  = [t for t in 1:T if projected.month[t] in (7, 11)]
    low_prod_months      = [t for t in 1:T if projected.month[t] in (8, 9, 10)]

    ph_nat_in = projected.eff_75_Average

    return (;
        a_residential, b_residential,
        a_commercial,  b_commercial,
        a_industrial,  b_industrial,
        hydro_min_hourly, hydro_max_hourly, hydro_weekly_totals,
        ph_nat_in,
        high_prod_months, med_high_prod_months, med_low_prod_months, low_prod_months
    )

end


# ===== Definition of the model: dispatch_electricity_market function =====
function dispatch_electricity_market(
    projected::DataFrame,        # hourly projected data for 2030
    technology::DataFrame,       # fixed technical and economic parameters by generation technology
    technical::NamedTuple,       # technical parameters shared across scenarios
    scenario::NamedTuple,        # scenario-specific parameters
    iteration::NamedTuple,       # iteration-specific parameters
    years_solving::Float64 = 0.230137
)

    # initialize the model solver and parameters
    model = Model(Gurobi.Optimizer)
    set_optimizer_attribute(model, "OutputFlag", 0)  
    set_optimizer_attribute(model, "TimeLimit", 300) 
    set_optimizer_attribute(model, "MIPGap", 0.03)

    # set indices
    T = nrow(projected);
    I = nrow(fixed_data);
    S = 3 # we have 3 sectors: residential, commercial and industrial
    C = 3 # import/export flows from/to 3 countries (POR, FRA, MOR)

    # define variables that the model solves
    @variable(model, price[1:T] >= 0);
    @variable(model, demand[1:T, 1:S] >= 0);
    @variable(model, imports[1:T, 1:C] >= 0); 
    @variable(model, exports[1:T, 1:C] >= 0); 
    @variable(model, quantity[1:T, 1:I] >= 0); 
    @variable(model, costs[1:T] >= 0); 
    @variable(model, consumer_surplus[1:T]);
    @variable(model, producer_revenue[1:T]); 
    @variable(model, running_costs[1:T] >= 0);  
    @variable(model, fuel_costs[1:T] >= 0);
    @variable(model, emissions_costs[1:T] >= 0);
    @variable(model, direct_emissions[1:T] >= 0);  
    @variable(model, lifecycle_emissions[1:T] >= 0);
    @variable(model, min_non_ren_gen[1:T] >= 0); 
    @variable(model, ph_in[t=1:T] >= 0);
    @variable(model, ph_out[t=1:T] >= 0);
    @variable(model, ph_stock[1:T] >= 0);
    @variable(model, batt_in[t=1:T] >= 0);   
    @variable(model, batt_out[t=1:T] >= 0);   
    @variable(model, batt_stock[1:T] >= 0); 

    # Objective function: maximize social welfare 
    @objective(model, Max, sum(consumer_surplus[t] + producer_revenue[t] - costs[t] for t=1:T)/T);

    # Market Clearing: Generation + imports - exports = demand 
    @constraint(model, balance[t=1:T], sum(quantity[t,i] for i in 1:I) + sum(imports[t,c] for c in 1:C) - sum(exports[t,c] for c in 1:C) 
                                == (1 + loss_factor) * sum(demand[t,s] for s in 1:S) + batt_in[t] / eff_batt + ph_in[t] / eff_ph);

    # Definition of consumer surplus 
    @constraint(model, [t=1:T], 
        consumer_surplus[t] == 
            demand[t,1]^2/(2*b_residential[t]) 
            + demand[t,2]^2/(2*b_commercial[t]) 
            + demand[t,3]^2/(2*b_industrial[t]));

    # Definition of producer revenue 
    @constraint(model, [t=1:T], 
        producer_revenue[t] == 
            (a_residential[t] - demand[t,1]) * demand[t,1] / b_residential[t] 
            + (a_commercial[t] - demand[t,2]) * demand[t,2] / b_commercial[t]
            + (a_industrial[t] - demand[t,3]) * demand[t,3] / b_industrial[t]);

    # Definition of costs (fixed + running)   
    @constraint(model, [t=1:T],
        costs[t] == sum((fixed_data.fixed_om_eur_gwy[i] * fixed_data.avg_cap_2024[i] * years_solving) for i in 1:I) / T + running_costs[t]); 

    # Components of running costs    
    @constraint(model, [t=1:T],
        running_costs[t] == sum((fixed_data.var_om_eur_gwh[i] * quantity[t,i]) for i in 1:I) + fuel_costs[t] + emissions_costs[t]);    

    # Fuel costs    
    @constraint(model, [t=1:T],
        fuel_costs[t] == projected.cost_coal_eur_gwh[t] * quantity[t, 1] / fixed_data.efficiency[1] # for coal
                        + sum((projected.cost_gas_eur_gwh[t] * quantity[t,j] / fixed_data.efficiency[j]) for j in 2:5) # for natural gas
                        + projected.cost_diesel_eur_gwh[t] * quantity[t, 6] / fixed_data.efficiency[6] # for diesel
                        + projected.cost_uranium_eur_gwh[t] * quantity[t, 8] / fixed_data.efficiency[8]); # for nuclear
    
    # Lifecycle emissions (computed with lifecycle emissions factors – only to store the results)
    @constraint(model, [t=1:T],
        lifecycle_emissions[t] == sum((fixed_data.fossil_fuel[i] * quantity[t,i] * fixed_data.lifecycle_e_tco2_gwh[i]) for i in 1:I));    

    # Direct emissions (computed with direct emissions factors – only to store the results)
    @constraint(model, [t=1:T],
        direct_emissions[t] == sum((fixed_data.fossil_fuel[i] * quantity[t,i] * fixed_data.direct_e_tco2_gwh[i]) for i in 1:I));

    # EU ETS costs (computed with direct emissions)
    @constraint(model, [t=1:T],
        emissions_costs[t] == sum((fixed_data.fossil_fuel[i] * projected.eu_ets_price_eur_tco2[t] * quantity[t,i] * fixed_data.direct_e_tco2_gwh[i]) for i in 1:I));        
        
    # Definition of demand
    @constraint(model, [t=1:T], demand[t,1] == a_residential[t] - b_residential[t] * price[t]);
    @constraint(model, [t=1:T], demand[t,2] == a_commercial[t] - b_commercial[t] * price[t]);
    @constraint(model, [t=1:T], demand[t,3] == a_industrial[t] - b_industrial[t] * price[t]);
    
    # Definition of imports (fixed to observed values)    
    @constraint(model, [t=1:T], imports[t,1] == projected.imports_France_gwh[t]);            
    @constraint(model, [t=1:T], imports[t,2] == projected.imports_Portugal_gwh[t]);           
    @constraint(model, [t=1:T], imports[t,3] == projected.imports_Morocco_gwh[t]);

    # Definition of exports (fixed to observed values)    
    @constraint(model, [t=1:T], exports[t,1] == projected.exports_France_gwh[t]);           
    @constraint(model, [t=1:T], exports[t,2] == projected.exports_Portugal_gwh[t]);         
    @constraint(model, [t=1:T], exports[t,3] == projected.exports_Morocco_gwh[t]);
            
    # Output constraints 
    # NON-RENEWABLES: specific modeling for Nuclear and calibration of ramping costs for the thermal plants
    # Coal
    @constraint(model, [t=1:T], quantity[t,1] >= 0.15 * projected.coal_cap_gw[t]);
    @constraint(model, [t=1:T], quantity[t,1] <= 0.65 * projected.coal_cap_gw[t]); 
    @constraint(model, [t=2:T], quantity[t,1] - quantity[t-1,1] >= -0.05 * projected.coal_cap_gw[t]);
    @constraint(model, [t=2:T], quantity[t,1] - quantity[t-1,1] <= +0.05 * projected.coal_cap_gw[t]);
 
    # Combined cycle gas
    @constraint(model, [t=1:T], quantity[t,2] >= 0.05 * projected.combined_cycle_cap_gw[t]);
    @constraint(model, [t=1:T], quantity[t,2] <= projected.combined_cycle_cap_gw[t]);
    @constraint(model, [t=2:T], quantity[t,2] - quantity[t-1,2] >= -0.25 * projected.combined_cycle_cap_gw[t]);
    @constraint(model, [t=2:T], quantity[t,2] - quantity[t-1,2] <= +0.25 * projected.combined_cycle_cap_gw[t]); 

    # Other gas
    @constraint(model, [t=1:T], quantity[t,3] <= projected.gas_turbine_cap_gw[t]);
    @constraint(model, [t=1:T], quantity[t,4] <= projected.vapor_turbine_cap_gw[t]);

    # Cogeneration (most likely gas)
    @constraint(model, [t=1:T], quantity[t,5] >= 0.15 * projected.cogeneration_cap_gw[t]);
    @constraint(model, [t=1:T], quantity[t,5] <= 0.6 * projected.cogeneration_cap_gw[t]);
    @constraint(model, [t=2:T], quantity[t,5] - quantity[t-1,5] >= -0.1 * projected.cogeneration_cap_gw[t]);
    @constraint(model, [t=2:T], quantity[t,5] - quantity[t-1,5] <= +0.1 * projected.cogeneration_cap_gw[t]);
    
    # Oil
    @constraint(model, [t=1:T], quantity[t,6] >= 0.3 * projected.diesel_cap_gw[t]); # Minimum production in the Canary islands
    @constraint(model, [t=1:T], quantity[t,6] <= projected.diesel_cap_gw[t]);

    # Non-renewable waste
    @constraint(model, [t=1:T], quantity[t,7] <= 0.4 * projected.nonrenewable_waste_cap_gw[t]);
    @constraint(model, [t=2:T], quantity[t,7] - quantity[t-1,7] >= -0.01 * projected.nonrenewable_waste_cap_gw[t]);
    @constraint(model, [t=2:T], quantity[t,7] - quantity[t-1,7] <= +0.01 * projected.nonrenewable_waste_cap_gw[t]);  
    
    # Nuclear (set equal to the self-reported availability)
    @constraint(model, [t=1:T], quantity[t,8] == projected.nuclear_cap_gw[t] * projected.nuclear_cap_factor[t]);

    # 
    @constraint(model, [t=1:T], min_non_ren_gen[t] == 
                0.15 * projected.coal_cap_gw[t] 
                + 0.05 * projected.combined_cycle_cap_gw[t] 
                + 0.15 * projected.cogeneration_cap_gw[t]
                + 0.3 * projected.diesel_cap_gw[t]
                + 0.4 * projected.nonrenewable_waste_cap_gw[t]
                + projected.nuclear_cap_gw[t] * projected.nuclear_cap_factor[t]);

    # RENEWABLES:
    # Hydro: generation constraints based on thr weekly real generation
    @constraint(model, [t=1:T], quantity[t,9] >= hydro_min_hourly[t]);
    @constraint(model, [t=1:T], quantity[t,9] <= hydro_max_hourly[t]);
    @constraint(model, [w in 1:n_bundles], sum(quantity[t, 9] for t in bundles[w]) <= hydro_weekly_totals[w]);
    @constraint(model, [t=2:T], quantity[t,9] - quantity[t-1,9] >= -0.1 * projected.conventional_hydro_cap_gw[t]);
    @constraint(model, [t=2:T], quantity[t,9] - quantity[t-1,9] <= +0.1 * projected.conventional_hydro_cap_gw[t]);    

    # Run of river hydro (seasonality constraints linked to monthly historical production)
    @constraint(model, [t=2:T], quantity[t,10] - quantity[t-1,10] >= -0.2 * projected.run_of_river_hydro_cap_gw[t]);
    @constraint(model, [t=2:T], quantity[t,10] - quantity[t-1,10] <= +0.2 * projected.run_of_river_hydro_cap_gw[t]);
    @constraint(model, [t in iteration.high_prod_months], scenario.ror_lo_high * projected.run_of_river_hydro_cap_gw[t] <= quantity[t,10] <= scenario.ror_hi_high * projected.run_of_river_hydro_cap_gw[t])

    # Pumped hydro (works as a battery)
    @constraint(model, ph_stock[1] == 0.5 * storage_cap_ph); # Initial stock of pumped hydro is 50% of the capacity
    @constraint(model, [t=2:T], ph_stock[t] <= storage_cap_ph);
    @constraint(model, [t=2:T], ph_stock[t] == ph_stock[t-1] + eff_ph * ph_in[t-1] - ph_out[t-1] + ph_nat_in[t-1]);
    @constraint(model, [t=1:T], ph_out[t] <= 0.75 * projected.pumped_hydro_cap_gw[t]);
    @constraint(model, [t=1:T], ph_in[t] <= projected.pumped_hydro_cap_gw[t]);
    @constraint(model, [t=1:T], quantity[t,11] == ph_out[t]);

    # Solar PV, thermal and wind have capacity factors relative to availability
    @constraint(model, [t=1:T], quantity[t,12] <= projected.solar_pv_cap_gw[t] * projected.solar_pv_cap_factor[t]);
    @constraint(model, [t=1:T], quantity[t,13] <= projected.solar_thermal_cap_gw[t] * projected.solar_thermal_cap_factor[t]); 
    @constraint(model, [t=1:T], quantity[t,14] <= projected.wind_cap_gw[t] * projected.wind_cap_factor[t]);

    # Other renewables have basic capacity constraints
    @constraint(model, [t=1:T], quantity[t,15] >= 0.25 * projected.other_renewable_cap_gw[t]);
    @constraint(model, [t=1:T], quantity[t,15] <= 0.6 * projected.other_renewable_cap_gw[t]);
    @constraint(model, [t=2:T], quantity[t,15] - quantity[t-1,15] >= -0.05 * projected.other_renewable_cap_gw[t]);
    @constraint(model, [t=2:T], quantity[t,15] - quantity[t-1,15] <= +0.05 * projected.other_renewable_cap_gw[t]); 

    # Renewable waste
    @constraint(model, [t=1:T], quantity[t,16] <= 0.65 * projected.renewable_waste_cap_gw[t]); 
    @constraint(model, [t=2:T], quantity[t,16] - quantity[t-1,16] >= -0.05 * projected.renewable_waste_cap_gw[t]);
    @constraint(model, [t=2:T], quantity[t,16] - quantity[t-1,16] <= +0.05 * projected.renewable_waste_cap_gw[t]);  
    
    # Batteries (modeled as 4h batteries)
    @constraint(model, batt_stock[1] == 0.5 * projected.batteries_cap_gw[1]);
    @constraint(model, [t=2:T], batt_stock[t] <= projected.batteries_cap_gw[t]);
    @constraint(model, [t=2:T], batt_stock[t] == (1 - decay_batt) * batt_stock[t-1] + eff_batt * batt_in[t-1] - batt_out[t-1] / eff_batt);
    @constraint(model, [t=1:T], batt_out[t] <= 0.25 * projected.batteries_cap_gw[t]);
    @constraint(model, [t=1:T], batt_in[t] <= 0.25 * projected.batteries_cap_gw[t]);
    @constraint(model, [t=1:T], quantity[t,17] == batt_out[t]); 
     
    # Solving the model
    optimize!(model)
    status = JuMP.termination_status(model);

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
        pumped_hydro      = q_vals[:, 11]
        solar_pv          = q_vals[:, 12]
        solar_t           = q_vals[:, 13]
        wind              = q_vals[:, 14]
        other_r           = q_vals[:, 15]
        ren_w             = q_vals[:, 16]
        batt_gen          = q_vals[:, 17]

        # Aggregated generation
        non_ren_gen       = [sum(q_vals[t, 1:8])  for t in 1:T]
        ren_gen           = [sum(q_vals[t, 9:17]) for t in 1:T]
        total_gen         = ren_gen .+ non_ren_gen
        share_ren_gen     = ren_gen ./ total_gen

        # Pumped hydro storage
        ph_in_vals        = JuMP.value.(ph_in)
        ph_stock_vals     = JuMP.value.(ph_stock)

        # Minimum non-renewable generation (constraint variable)
        min_non_ren_vals  = JuMP.value.(min_non_ren_gen)
        share_min_non_ren = min_non_ren_vals ./ total_gen

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
        curt_solar_pv      = 1.0 - sum(q_vals[t,12] for t in 1:T) / sum(new_data.solar_pv_cap_gw[t]      * new_data.solar_pv_cap_factor[t]      for t in 1:T)
        curt_solar_thermal = 1.0 - sum(q_vals[t,13] for t in 1:T) / sum(new_data.solar_thermal_cap_gw[t] * new_data.solar_thermal_cap_factor[t] for t in 1:T)
        curt_wind          = 1.0 - sum(q_vals[t,14] for t in 1:T) / sum(new_data.wind_cap_gw[t]          * new_data.wind_cap_factor[t]          for t in 1:T)

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
            "pumped_hydro_gen"          => pumped_hydro,
            "pumped_hydro_pumping"      => ph_in_vals,
            "pumped_hydro_storage"      => ph_stock_vals,
            "solar_pv_gen"              => solar_pv,
            "solar_thermal_gen"         => solar_t,
            "wind_gen"                  => wind,
            "other_renewable_gen"       => other_r,
            "renewable_waste_gen"       => ren_w,
            "battery_gen"               => batt_gen,

            # Aggregated generation
            "total_generation"          => total_gen,
            "renewable_gen"             => ren_gen,
            "non_renewable_gen"         => non_ren_gen,
            "share_renewable_gen"       => share_ren_gen,
            "min_non_renewable_gen"     => share_min_non_ren,

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
        "pumped_hydro_gen"          => fill(-1.0, T),
        "pumped_hydro_pumping"      => fill(-1.0, T),
        "pumped_hydro_storage"      => fill(-1.0, T),
        "solar_pv_gen"              => fill(-1.0, T),
        "solar_thermal_gen"         => fill(-1.0, T),
        "wind_gen"                  => fill(-1.0, T),
        "other_renewable_gen"       => fill(-1.0, T),
        "renewable_waste_gen"       => fill(-1.0, T),
        "battery_gen"               => fill(-1.0, T),

        # Aggregated generation
        "total_generation"          => fill(-1.0, T),
        "renewable_gen"             => fill(-1.0, T),
        "non_renewable_gen"         => fill(-1.0, T),
        "share_renewable_gen"       => fill(-1.0, T),
        "min_non_renewable_gen"     => fill(-1.0, T),

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
