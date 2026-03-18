# This script defines the model that simulates the Spanish electricity market

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

# ===== Set dirpath to your working directory =====
# for us it works better adding a "/" at the end
dirpath = "path/to/your/directory/"


# ===== Set general parameters =====
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

# ===== Auxiliary function to define iteration-specific parameters =====
# Since the model is designed to be solved for many possible realizations of the future,
# some parameters shall be computed for each iteration (since the input data will be different)

function set_iteration_specific_parameters(hourly_data::DataFrame)

    # Parameters defining domestic demand functions are re-computed in each simulation
    # (pasarlo a GWh!)
    b_residential = elas_residential * hourly_data.residential_demand_mwh ./ hourly_data.spot_price_eur_mwh
    b_commercial = elas_commercial * hourly_data.commercial_demand_mwh ./ hourly_data.spot_price_eur_mwh
    b_industrial = elas_industrial * hourly_data.industrial_demand_mwh ./ hourly_data.spot_price_eur_mwh

    a_residential = hourly_data.residential_demand_mwh + b_residential .* hourly_data.spot_price_eur_mwh
    a_commercial = hourly_data.commercial_demand_mwh + b_commercial .* hourly_data.spot_price_eur_mwh
    a_industrial = hourly_data.industrial_demand_mwh + b_industrial .* hourly_data.spot_price_eur_mwh

    # Hydro bundles for weekly allocation maximization are also defined within the model
    bundle_size = 168   # number of hours in a week
    total_hours = size(hourly_data, 1)
    n_bundles = div(total_hours, bundle_size)
    
    starts = [1 + (w - 1) * bundle_size for w in 1:n_bundles]
    #> [1, 169, 337, 505, ...]  (initial hours every week)

    bundles = [s:s + bundle_size - 1 for s in starts[1:n_bundles]]
    #> [1:168, 169:336, 337:504, ...]  (hour intervals for every week)

    # set lower and upper bound production levels per week
    hydro_min_weekly = [minimum(hourly_data.conventional_hydro_gen_mwh[b]) for b in bundles]
    hydro_max_weekly = [maximum(hourly_data.conventional_hydro_gen_mwh[b]) for b in bundles]

    # set the same minimum and maximum hourly production values for all hours of the week
    hydro_min_hourly = zeros(Float64, total_hours)
    hydro_max_hourly = zeros(Float64, total_hours)

    for (w, b) in enumerate(bundles)
        hydro_min_hourly[b] .= hydro_min_weekly[w]
        hydro_max_hourly[b] .= hydro_max_weekly[w]
    end

    # total hydro production per week (which the model then allocats per hour)
    hydro_weekly_totals = [sum(hourly_data.conventional_hydro_gen_mwh[b]) for b in bundles]

    # define water availability for run of river hydro with a stylized definition of seasonality
    ror_lo = [ror_bounds(new_data.month[t])[1] for t in 1:T]
    ror_hi = [ror_bounds(new_data.month[t])[2] for t in 1:T]

    ph_nat_in = hourly_data.eff_75_Average

    return (;
        a_residential, b_residential,
        a_commercial,  b_commercial,
        a_industrial,  b_industrial,
        hydro_min_hourly, hydro_max_hourly, hydro_weekly_totals,
        ph_nat_in,
        ror_lo, ror_hi,
    )

end


# ===== Definition of the model: dispatch_electricity_market function =====
function dispatch_electricity_market(
    hourly_data::DataFrame, 
    fixed_data::DataFrame, 
    scenario_parameters::DataFrame,
    iteration_specific_params::NamedTuple,
    interconnectors_delta::Vector; # revisar si finalmente los usamos
    loss_factor::Float64 = general_loss_factor, 
    years_solving::Float64 = 0.230137
    )

    # initialize the model solver and parameters
    model = Model(Gurobi.Optimizer)
    set_optimizer_attribute(model, "OutputFlag", 0)  
    set_optimizer_attribute(model, "TimeLimit", 300) 
    set_optimizer_attribute(model, "MIPGap", 0.03)

    # set indices
    T = nrow(hourly_data);
    I = nrow(fixed_data);
    S = 3 # we have 3 sectors: residential, commercial and industrial
    C = 3 # import/export flows with 3 countries (POR, FRA, MOR)

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
    @variable(model, lifecycle_emissions[1:T] >= 0);
    @variable(model, direct_emissions[1:T] >= 0);  
    @variable(model, emissions_costs[1:T] >= 0);
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
        costs[t] == sum((fixed_data.fixed_om_eur_mwy[i] * fixed_data.avg_cap_2024[i] * years_solving) for i in 1:I) / T + running_costs[t]); 

    # Components of running costs    
    @constraint(model, [t=1:T],
        running_costs[t] == sum((fixed_data.var_om_eur_mwh[i] * quantity[t,i]) for i in 1:I) + fuel_costs[t] + emissions_costs[t]);    

    # Fuel costs    
    @constraint(model, [t=1:T],
        fuel_costs[t] == hourly_data.cost_coal_eur_mwh[t] * quantity[t, 1] / fixed_data.efficiency[1] # for coal
                        + sum((hourly_data.cost_gas_eur_mwh[t] * quantity[t,j] / fixed_data.efficiency[j]) for j in 2:5) # for natural gas
                        + hourly_data.cost_diesel_eur_mwh[t] * quantity[t, 6] / fixed_data.efficiency[6] # for diesel
                        + hourly_data.cost_uranium_eur_mwh[t] * quantity[t, 8] / fixed_data.efficiency[8]); # for nuclear
    
    # Lifecycle emissions (computed with lifecycle emissions factors – only to store the results)
    @constraint(model, [t=1:T],
        lifecycle_emissions[t] == sum((fixed_data.fossil_fuel[i] * quantity[t,i] * fixed_data.lifecycle_e_tco2_mwh[i]) for i in 1:I));    

    # Direct emissions (computed with direct emissions factors – only to store the results)
    @constraint(model, [t=1:T],
        direct_emissions[t] == sum((fixed_data.fossil_fuel[i] * quantity[t,i] * fixed_data.direct_e_tco2_mwh[i]) for i in 1:I));

    # EU ETS costs (computed with direct emissions)
    @constraint(model, [t=1:T],
        emissions_costs[t] == sum((fixed_data.fossil_fuel[i] * hourly_data.eu_ets_price_eur_tco2[t] * quantity[t,i] * fixed_data.direct_e_tco2_mwh[i]) for i in 1:I));        
        
    # Definition of demand
    @constraint(model, [t=1:T], demand[t,1] == a_residential[t] - b_residential[t] * price[t]);
    @constraint(model, [t=1:T], demand[t,2] == a_commercial[t] - b_commercial[t] * price[t]);
    @constraint(model, [t=1:T], demand[t,3] == a_industrial[t] - b_industrial[t] * price[t]);
    
    # Definition of imports    
    @constraint(model, [t=1:T], imports[t,1] == hourly_data.imports_France_mwh[t]);            
    @constraint(model, [t=1:T], imports[t,2] == hourly_data.imports_Portugal_mwh[t]);           
    @constraint(model, [t=1:T], imports[t,3] == hourly_data.imports_Morocco_mwh[t]);

    # Definition of exports    
    @constraint(model, [t=1:T], exports[t,1] == hourly_data.exports_France_mwh[t]);           
    @constraint(model, [t=1:T], exports[t,2] == hourly_data.exports_Portugal_mwh[t]);         
    @constraint(model, [t=1:T], exports[t,3] == hourly_data.exports_Morocco_mwh[t]);
            
    # Output constraints 
    # NON-RENEWABLES: specific modeling for Nuclear and calibration of ramping costs for the thermal plants
    @constraint(model, [t=1:T], quantity[t,1] >= 0.15 * hourly_data.coal_cap_mw[t]);
    @constraint(model, [t=1:T], quantity[t,1] <= 0.65 * hourly_data.coal_cap_mw[t]); 
    @constraint(model, [t=2:T], quantity[t,1] - quantity[t-1,1] >= -0.05 * hourly_data.coal_cap_mw[t]);
    @constraint(model, [t=2:T], quantity[t,1] - quantity[t-1,1] <= +0.05 * hourly_data.coal_cap_mw[t]);
 
    @constraint(model, [t=1:T], quantity[t,2] >= 0.05 * hourly_data.combined_cycle_cap_mw[t]);
    @constraint(model, [t=1:T], quantity[t,2] <= hourly_data.combined_cycle_cap_mw[t]);
    @constraint(model, [t=2:T], quantity[t,2] - quantity[t-1,2] >= -0.25 * hourly_data.combined_cycle_cap_mw[t]);
    @constraint(model, [t=2:T], quantity[t,2] - quantity[t-1,2] <= +0.25 * hourly_data.combined_cycle_cap_mw[t]); 

    @constraint(model, [t=1:T], quantity[t,3] <= hourly_data.gas_turbine_cap_mw[t]);
    @constraint(model, [t=1:T], quantity[t,4] <= hourly_data.vapor_turbine_cap_mw[t]);

    @constraint(model, [t=1:T], quantity[t,5] >= 0.15 * hourly_data.cogeneration_cap_mw[t]);
    @constraint(model, [t=1:T], quantity[t,5] <= 0.6 * hourly_data.cogeneration_cap_mw[t]);
    @constraint(model, [t=2:T], quantity[t,5] - quantity[t-1,5] >= -0.1 * hourly_data.cogeneration_cap_mw[t]);
    @constraint(model, [t=2:T], quantity[t,5] - quantity[t-1,5] <= +0.1 * hourly_data.cogeneration_cap_mw[t]);
    
    @constraint(model, [t=1:T], quantity[t,6] >= 0.3 * hourly_data.diesel_cap_mw[t]); # Minimum production in the Canary islands
    @constraint(model, [t=1:T], quantity[t,6] <= hourly_data.diesel_cap_mw[t]);

    @constraint(model, [t=1:T], quantity[t,7] <= 0.4 * hourly_data.nonrenewable_waste_cap_mw[t]);
    @constraint(model, [t=2:T], quantity[t,7] - quantity[t-1,7] >= -0.01 * hourly_data.nonrenewable_waste_cap_mw[t]);
    @constraint(model, [t=2:T], quantity[t,7] - quantity[t-1,7] <= +0.01 * hourly_data.nonrenewable_waste_cap_mw[t]);  
    
    # Nuclear is almost always constant, so we set it equal to the self-reported availability    
    @constraint(model, [t=1:T], quantity[t,8] == hourly_data.nuclear_cap_mw[t] * hourly_data.nuclear_cap_factor[t]);

    @constraint(model, [t=1:T], min_non_ren_gen[t] == 
                0.15 * hourly_data.coal_cap_mw[t] 
                + 0.05 * hourly_data.combined_cycle_cap_mw[t] 
                + 0.15 * hourly_data.cogeneration_cap_mw[t]
                + 0.3 * hourly_data.diesel_cap_mw[t]
                + 0.4 * hourly_data.nonrenewable_waste_cap_mw[t]
                + hourly_data.nuclear_cap_mw[t] * hourly_data.nuclear_cap_factor[t]);

    # RENEWABLES:
    # Hydro: generation constraints based on thr weekly real generation
    @constraint(model, [t=1:T], quantity[t,9] >= hydro_min_hourly[t]);
    @constraint(model, [t=1:T], quantity[t,9] <= hydro_max_hourly[t]);
    @constraint(model, [w in 1:n_bundles], sum(quantity[t, 9] for t in bundles[w]) <= hydro_weekly_totals[w]);
    @constraint(model, [t=2:T], quantity[t,9] - quantity[t-1,9] >= -0.1 * hourly_data.conventional_hydro_cap_mw[t]);
    @constraint(model, [t=2:T], quantity[t,9] - quantity[t-1,9] <= +0.1 * hourly_data.conventional_hydro_cap_mw[t]);    

    # Run of river hydro (seasonality constraints linked to monthly historical production)
    @constraint(model, [t=1:T], params.ror_lo[t] * cap[t] <= quantity[t,10] <= params.ror_hi[t] * cap[t])  
    @constraint(model, [t=2:T], quantity[t,10] - quantity[t-1,10] >= -0.2 * hourly_data.run_of_river_hydro_cap_mw[t]);
    @constraint(model, [t=2:T], quantity[t,10] - quantity[t-1,10] <= +0.2 * hourly_data.run_of_river_hydro_cap_mw[t]);

    # Pumped hydro (works as a battery)
    @constraint(model, ph_stock[1] == 0.5 * storage_cap_ph); # Initial stock of pumped hydro is 50% of the capacity
    @constraint(model, [t=2:T], ph_stock[t] <= storage_cap_ph);
    @constraint(model, [t=2:T], ph_stock[t] == ph_stock[t-1] + eff_ph * ph_in[t-1] - ph_out[t-1] + ph_nat_in[t-1]);
    @constraint(model, [t=1:T], ph_out[t] <= 0.75 * hourly_data.pumped_hydro_cap_mw[t]);
    @constraint(model, [t=1:T], ph_in[t] <= hourly_data.pumped_hydro_cap_mw[t]);
    @constraint(model, [t=1:T], quantity[t,11] == ph_out[t]);

    # Solar PV, thermal and wind have capacity factors relative to availability
    @constraint(model, [t=1:T], quantity[t,12] <= hourly_data.solar_pv_cap_mw[t] * hourly_data.solar_pv_cap_factor[t]);
    @constraint(model, [t=1:T], quantity[t,13] <= hourly_data.solar_thermal_cap_mw[t] * hourly_data.solar_thermal_cap_factor[t]); 
    @constraint(model, [t=1:T], quantity[t,14] <= hourly_data.wind_cap_mw[t] * hourly_data.wind_cap_factor[t]);

    # Other renewables have basic capacity constraints
    @constraint(model, [t=1:T], quantity[t,15] >= 0.25 * hourly_data.other_renewable_cap_mw[t]);
    @constraint(model, [t=1:T], quantity[t,15] <= 0.6 * hourly_data.other_renewable_cap_mw[t]);
    @constraint(model, [t=2:T], quantity[t,15] - quantity[t-1,15] >= -0.05 * hourly_data.other_renewable_cap_mw[t]);
    @constraint(model, [t=2:T], quantity[t,15] - quantity[t-1,15] <= +0.05 * hourly_data.other_renewable_cap_mw[t]); 

    @constraint(model, [t=1:T], quantity[t,16] <= 0.65 * hourly_data.renewable_waste_cap_mw[t]); 
    @constraint(model, [t=2:T], quantity[t,16] - quantity[t-1,16] >= -0.05 * hourly_data.renewable_waste_cap_mw[t]);
    @constraint(model, [t=2:T], quantity[t,16] - quantity[t-1,16] <= +0.05 * hourly_data.renewable_waste_cap_mw[t]);  
    
    # Batteries (modeled as 4h batteries)
    @constraint(model, batt_stock[1] == 0.5* hourly_data.batteries_cap_mw[1]);
    @constraint(model, [t=2:T], batt_stock[t] <= hourly_data.batteries_cap_mw[t]);
    @constraint(model, [t=2:T], batt_stock[t] == (1 - decay_batt) * batt_stock[t-1] + eff_batt * batt_in[t-1] - batt_out[t-1] / eff_batt);
    @constraint(model, [t=1:T], batt_out[t] <= 0.25 * hourly_data.batteries_cap_mw[t]);
    @constraint(model, [t=1:T], batt_in[t] <= 0.25 * hourly_data.batteries_cap_mw[t]);
    @constraint(model, [t=1:T], quantity[t,17] == batt_out[t]); 
          
    optimize!(model)
    
    status = JuMP.termination_status(model);

    if status == MOI.OPTIMAL
        cons_surplus = sum(JuMP.value.(consumer_surplus));
        prod_revenue = sum(JuMP.value.(producer_revenue));
        total_cost = sum(JuMP.value.(costs));
        prod_surplus = prod_revenue - total_cost;
        net_w = cons_surplus + prod_surplus;
        p = JuMP.value.(price);
        min_p = minimum(p); avg_p = mean(p); max_p = maximum(p); std_p = std(p);
        q = JuMP.value.(quantity);
        gen = [sum(q[t, :]) for t in 1:T];
        coal = q[:, 1]; cc_gas = q[:, 2]; gas_tur = q[:, 3]; vapor_tur = q[:, 4]; cogeneration = q[:, 5]; diesel = q[:, 6]; non_ren_w = q[:, 7]; nuclear = q[:, 8];
        conv_hydro = q[:, 9]; river_hydro = q[:, 10]; pumped_hydro = q[:, 11]; solar_pv = q[:, 12]; solar_t = q[:, 13]; wind = q[:, 14]; other_r = q[:, 15]; ren_w = q[:, 16]; batt_gen = q[:, 17];
        ph_in_vals  = JuMP.value.(ph_in); ph_stock_vals = JuMP.value.(ph_stock);
        non_ren_gen = [sum(q[t, 1:8]) for t in 1:T];
        ren_gen = [sum(q[t, 9:17]) for t in 1:T];
        total_gen = ren_gen + non_ren_gen;
        share_ren_gen = ren_gen ./ total_gen;
        min_share_ren_gen = minimum(ren_gen ./ total_gen); max_share_ren_gen = maximum(ren_gen ./ total_gen);
        mean_share_ren_gen = mean(ren_gen ./ total_gen); median_share_ren_gen = median(ren_gen ./ total_gen);
        imp = JuMP.value.(imports); 
        imp = JuMP.value.(imports); 
        imp_fra = imp[:, 1]; imp_por = imp[:, 2]; imp_mor = imp[:, 3]
        exp = JuMP.value.(exports);
        exp_fra = exp[:, 1]; exp_por = exp[:, 2]; exp_mor = imp[:, 3]
        d = JuMP.value.(demand);
        res_d = d[:, 1];
        com_d = d[:, 2];
        ind_d = d[:, 3];
        total_d = [sum(d[t, :]) for t in 1:T];
        min_non_ren_gen = JuMP.value.(min_non_ren_gen);
        share_min_non_ren_gen = min_non_ren_gen ./ total_gen;
        life_e = JuMP.value.(lifecycle_emissions);
        direct_e = JuMP.value.(direct_emissions);
        curt_solar_pv = 1.0 - sum(q[t,12] for t=1:T) / sum(hourly_data.solar_pv_cap_mw[t] * hourly_data.solar_pv_cap_factor[t] for t=1:T)
        curt_solar_thermal = 1.0 - sum(q[t,13] for t=1:T) / sum(hourly_data.solar_thermal_cap_mw[t] * hourly_data.solar_thermal_cap_factor[t] for t=1:T)
        curt_wind = 1.0 - sum(q[t,14] for t=1:T) / sum(hourly_data.wind_cap_mw[t] * hourly_data.wind_cap_factor[t] for t=1:T)

        results = Dict(
            "price" => p,
            "avg_price" => avg_p,
            "max_price" => max_p,
            "min_price" => min_p,
            "std_price" => std_p,
            "consumer_surplus" => cons_surplus,
            "producer_surplus" => prod_surplus,
            "total_cost" => total_cost,
            "net_welfare" => net_w,
            "residential_demand" => res_d,
            "commercial_demand" => com_d,
            "industrial_demand" => ind_d,
            "total_demand" => total_d,
            "generation" => gen, 
            "coal_gen" => coal,
            "combined_cycle_gen" => cc_gas,
            "gas_turbine_gen" => gas_tur,
            "vapor_turbine_gen" => vapor_tur,
            "cogeneration_gen" => cogeneration,
            "diesel_gen" => diesel,
            "non_renewable_waste_gen" => non_ren_w,
            "nuclear_gen" => nuclear,
            "conventional_hydro_gen" => conv_hydro,
            "run_of_river_hydro_gen" => river_hydro,
            "pumped_hydro_gen" => pumped_hydro,
            "pumped_hydro_pumping" => ph_in_vals,         
            "pumped_hydro_storage" => ph_stock_vals,
            "solar_pv_gen" => solar_pv,
            "solar_thermal_gen" => solar_t,
            "wind_gen" => wind,
            "other_renewable_gen" => other_r,
            "renewable_waste_gen" => ren_w,
            "battery_gen" => batt_gen,
            "imports_FRA" => imp_fra,
            "imports_POR" => imp_por,
            "imports_MOR" => imp_mor,
            "exports_FRA" => exp_fra,
            "exports_POR" => exp_por,
            "exports_MOR" => exp_mor, 
            "renewable_gen" => ren_gen,
            "non_renewable_generation" => non_ren_gen,
            "total_generation" => total_gen,
            "share_renewable_gen" => share_ren_gen,
            "min_non_renewable_gen" => share_min_non_ren_gen,
            "lifecycle_emissions" => life_e,
            "direct_emissions" => direct_e,
            "curtailment_solar_pv" => curt_solar_pv,
            "curtailment_solar_thermal" => curt_solar_thermal,
            "curtailment_wind" => curt_wind
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
        # Return a results dictionary with default values
        results = Dict(
        "price" => fill(-1.0, T),  # Vector of size T
        "avg_price" => -1.0,       # Single float
        "max_price" => -1.0,
        "min_price" => -1.0,
        "std_price" => -1.0,
        "consumer_surplus" => -1.0,
        "producer_surplus" => -1.0,
        "total_cost" => -1.0,
        "net_welfare" => -1.0,
        "residential_demand" => fill(-1.0, T),
        "commercial_demand" => fill(-1.0, T),
        "industrial_demand" => fill(-1.0, T),
        "total_demand" => fill(-1.0, T),
        "generation" => fill(-1.0, T),
        "coal_gen" => fill(-1.0, T),
        "combined_cycle_gen" => fill(-1.0, T),
        "gas_turbine_gen" => fill(-1.0, T),
        "vapor_turbine_gen" => fill(-1.0, T),
        "cogeneration_gen" => fill(-1.0, T),
        "diesel_gen" => fill(-1.0, T),
        "non_renewable_waste_gen" => fill(-1.0, T),
        "nuclear_gen" => fill(-1.0, T),
        "conventional_hydro_gen" => fill(-1.0, T),
        "run_of_river_hydro_gen" => fill(-1.0, T),
        "pumped_hydro_gen" => fill(-1.0, T),
        "pumped_hydro_pumping" => fill(-1.0, T),
        "pumped_hydro_storage" => fill(-1.0, T),
        "solar_pv_gen" => fill(-1.0, T),
        "solar_thermal_gen" => fill(-1.0, T),
        "wind_gen" => fill(-1.0, T),
        "other_renewable_gen" => fill(-1.0, T),
        "renewable_waste_gen" => fill(-1.0, T),
        "battery_gen" => fill(-1.0, T),
        "imports_FRA" => fill(-1.0, T),
        "imports_POR" => fill(-1.0, T),
        "imports_MOR" => fill(-1.0, T),
        "exports_FRA" => fill(-1.0, T),
        "exports_POR" => fill(-1.0, T),
        "exports_MOR" => fill(-1.0, T),
        "renewable_gen" => fill(-1.0, T),
        "non_renewable_generation" => fill(-1.0, T),
        "total_generation" => fill(-1.0, T),
        "share_renewable_gen" => fill(-1.0, T),
        "min_non_renewable_gen" => -1.0,
        "lifecycle_emissions" => fill(-1.0, T),
        "direct_emissions" => fill(-1.0, T),
        "curtailment_solar_pv" => -1.0,
        "curtailment_solar_thermal" => -1.0,
        "curtailment_wind" => -1.0
        )
    end
    return results
end
