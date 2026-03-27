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

# Edit dirpath making sure it ends with "/" at the end
dirpath = "/Users/marreguant/Documents/TFM_Cristobal_Tomas_Pau_v2/"


# ===== Load data  =====
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

# Define elasticities of demand functions (domestic, imports, exports)
e_residential = 0.015 # Mar values are x10 these!
e_commercial = 0.03
e_industrial = 0.05
e_imports = 0.03
e_exports = 0.03


# ===== Define dispatch_electricity_market function =====

function dispatch_electricity_market(hourly_data::DataFrame, fixed_data::DataFrame, interconnectors_delta::Vector; loss_factor::Float64 = 0.0, years_solving::Float64 = 0.230137)
        
    model = Model(Gurobi.Optimizer)

    set_optimizer_attribute(model, "OutputFlag", 0)  
    set_optimizer_attribute(model, "TimeLimit", 300) 
    set_optimizer_attribute(model, "MIPGap", 0.03)

    T = nrow(hourly_data);
    I = nrow(fixed_data);
    S = 3 # we have 3 sectors: residential, commercial and industrial
    C = 3 # import/export flows with 3 countries (POR, FRA, MOR)

    # Define parameters for domestic demand functions
    hourly_data.b_residential = e_residential * hourly_data.residential_demand_mwh ./ hourly_data.spot_price_eur_mwh
    hourly_data.b_commercial = e_commercial * hourly_data.commercial_demand_mwh ./ hourly_data.spot_price_eur_mwh
    hourly_data.b_industrial = e_industrial * hourly_data.industrial_demand_mwh ./ hourly_data.spot_price_eur_mwh

    hourly_data.a_residential = hourly_data.residential_demand_mwh + hourly_data.b_residential .* hourly_data.spot_price_eur_mwh
    hourly_data.a_commercial = hourly_data.commercial_demand_mwh + hourly_data.b_commercial .* hourly_data.spot_price_eur_mwh
    hourly_data.a_industrial = hourly_data.industrial_demand_mwh + hourly_data.b_industrial .* hourly_data.spot_price_eur_mwh

    # # Define parameters for imports and exports demand functions
    # hourly_data.b_imp_FRA = e_imports * hourly_data.imports_France_mwh ./ hourly_data.spot_price_eur_mwh
    # hourly_data.b_imp_POR = e_imports * hourly_data.imports_Portugal_mwh ./ hourly_data.spot_price_eur_mwh
    # hourly_data.b_imp_MOR = e_imports * hourly_data.imports_Morocco_mwh ./ hourly_data.spot_price_eur_mwh
    # hourly_data.b_exp_FRA = e_exports * hourly_data.exports_France_mwh ./ hourly_data.spot_price_eur_mwh
    # hourly_data.b_exp_POR = e_exports * hourly_data.exports_Portugal_mwh ./ hourly_data.spot_price_eur_mwh
    # hourly_data.b_exp_MOR = e_exports * hourly_data.exports_Morocco_mwh ./ hourly_data.spot_price_eur_mwh

    # # We have set b to be the mean since in every hour either imports or exports = 0
    # hourly_data.b_imp_FRA .= mean(hourly_data.b_imp_FRA)
    # hourly_data.b_imp_POR .= mean(hourly_data.b_imp_POR)
    # hourly_data.b_imp_MOR .= mean(hourly_data.b_imp_MOR)
    # hourly_data.b_exp_FRA .= mean(hourly_data.b_exp_FRA)
    # hourly_data.b_exp_POR .= mean(hourly_data.b_exp_POR)
    # hourly_data.b_exp_MOR .= mean(hourly_data.b_exp_MOR)

    # hourly_data.a_imp_FRA = hourly_data.imports_France_mwh - hourly_data.b_imp_FRA .* hourly_data.spot_price_eur_mwh
    # hourly_data.a_imp_POR = hourly_data.imports_Portugal_mwh - hourly_data.b_imp_POR .* hourly_data.spot_price_eur_mwh
    # hourly_data.a_imp_MOR = hourly_data.imports_Morocco_mwh - hourly_data.b_imp_MOR .* hourly_data.spot_price_eur_mwh
    # hourly_data.a_exp_FRA = hourly_data.exports_France_mwh + hourly_data.b_exp_FRA .* hourly_data.spot_price_eur_mwh
    # hourly_data.a_exp_POR = hourly_data.exports_Portugal_mwh + hourly_data.b_exp_POR .* hourly_data.spot_price_eur_mwh
    # hourly_data.a_exp_MOR = hourly_data.exports_Morocco_mwh + hourly_data.b_exp_MOR .* hourly_data.spot_price_eur_mwh

    # Hydro bundles for weekly allocation maximization
    bundle_size = 168
    total_hours = size(hourly_data, 1)
    n_bundles = div(total_hours, bundle_size)
    
    starts = [1 + (w - 1) * bundle_size for w in 1:n_bundles]
    bundles = [s:s + bundle_size - 1 for s in starts[1:n_bundles]]

    hydro_min_weekly = [minimum(hourly_data.conventional_hydro_gen_mwh[b]) for b in bundles]
    hydro_max_weekly = [maximum(hourly_data.conventional_hydro_gen_mwh[b]) for b in bundles]

    hydro_min_hourly = zeros(Float64, total_hours)
    hydro_max_hourly = zeros(Float64, total_hours)

    for (w, b) in enumerate(bundles)
    hydro_min_hourly[b] .= hydro_min_weekly[w]
    hydro_max_hourly[b] .= hydro_max_weekly[w]
    end

    hydro_weekly_totals = [sum(hourly_data.conventional_hydro_gen_mwh[b]) for b in bundles]

    # For run of river seasonality
    high_prod_months = [t for t in 1:T if hourly_data.month[t] in (1, 3, 12)]
    med_high_prod_months = [t for t in 1:T if hourly_data.month[t] in (2, 4, 5, 6)]
    med_low_prod_months = [t for t in 1:T if hourly_data.month[t] in (7, 11)]
    low_prod_months = [t for t in 1:T if hourly_data.month[t] in (8, 9, 10)]

    # Define pumped hydro parameters
    eff_ph = 0.75
    storage_cap_ph = 15.0 # no official number for this estiamtes range from 10-20 GW.
    ph_nat_in = hourly_data.eff_75_Average

    # Define battery parameters
    eff_batt = 0.95  # Same for charge and discharge
    decay_batt = 0.001  # hourly self-loss ratio

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
    # @variable(model, import_costs[1:T] >= 0);
    # @variable(model, export_revenues[1:T] >= 0);
    @variable(model, min_non_ren_gen[1:T] >= 0); 
    @variable(model, ph_in[t=1:T] >= 0);
    @variable(model, ph_out[t=1:T] >= 0);
    @variable(model, ph_stock[1:T] >= 0);
    @variable(model, batt_in[t=1:T] >= 0);   
    @variable(model, batt_out[t=1:T] >= 0);   
    @variable(model, batt_stock[1:T] >= 0); 

    # Objective function, maximize social welfare 
    @objective(model, Max, sum(consumer_surplus[t] + producer_revenue[t] - costs[t] for t=1:T)/T);

    # Market Clearing: Generation + imports - exports = demand 
    @constraint(model, balance[t=1:T], sum(quantity[t,i] for i in 1:I) + sum(imports[t,c] for c in 1:C) - sum(exports[t,c] for c in 1:C) 
                                == (1 + loss_factor) * sum(demand[t,s] for s in 1:S) + batt_in[t] / eff_batt + ph_in[t] / eff_ph);

    # Definition of consumer surplus (producer surplus + consumer surplus)
    @constraint(model, [t=1:T], 
        consumer_surplus[t] == 
            demand[t,1]^2/(2*hourly_data.b_residential[t]) 
            + demand[t,2]^2/(2*hourly_data.b_commercial[t]) 
            + demand[t,3]^2/(2*hourly_data.b_industrial[t]));

    # Definition of consumer surplus (producer surplus + consumer surplus)
    @constraint(model, [t=1:T], 
        producer_revenue[t] == 
            (hourly_data.a_residential[t] - demand[t,1]) * demand[t,1] / hourly_data.b_residential[t] 
            + (hourly_data.a_commercial[t] - demand[t,2]) * demand[t,2] / hourly_data.b_commercial[t]
            + (hourly_data.a_industrial[t] - demand[t,3]) * demand[t,3] / hourly_data.b_industrial[t]);

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
        
    # # Import costs
    # @constraint(model, [t=1:T],
    #     import_costs[t] == - hourly_data.a_imp_FRA[t]/hourly_data.b_imp_FRA[t]*imports[t,1] + imports[t,1]^2/(2 * hourly_data.b_imp_FRA[t])
    #                      - hourly_data.a_imp_POR[t]/hourly_data.b_imp_POR[t]*imports[t,2] + imports[t,2]^2/(2 * hourly_data.b_imp_POR[t]) 
    #                      - hourly_data.a_imp_MOR[t]/hourly_data.b_imp_MOR[t]*imports[t,3] + imports[t,3]^2/(2 * hourly_data.b_imp_MOR[t]));     

    # # Export revenues
    # @constraint(model, [t=1:T],
    #     export_revenues[t] ==  hourly_data.a_exp_FRA[t]/hourly_data.b_exp_FRA[t]*exports[t,1] - exports[t,1]^2/(2 * hourly_data.b_exp_FRA[t])
    #                         + hourly_data.a_exp_POR[t]/hourly_data.b_exp_POR[t]*exports[t,2] - exports[t,2]^2/(2 * hourly_data.b_exp_POR[t]) 
    #                         + hourly_data.a_exp_MOR[t]/hourly_data.b_exp_MOR[t]*exports[t,2] - exports[t,3]^2/(2 * hourly_data.b_exp_MOR[t]));

    # Definition of demand
    @constraint(model, [t=1:T], demand[t,1] == hourly_data.a_residential[t] - hourly_data.b_residential[t] * price[t]);
    @constraint(model, [t=1:T], demand[t,2] == hourly_data.a_commercial[t] - hourly_data.b_commercial[t] * price[t]);
    @constraint(model, [t=1:T], demand[t,3] == hourly_data.a_industrial[t] - hourly_data.b_industrial[t] * price[t]);
    
    # Definition of imports    
    @constraint(model, [t=1:T], imports[t,1] == hourly_data.imports_France_mwh[t]);            
    @constraint(model, [t=1:T], imports[t,2] == hourly_data.imports_Portugal_mwh[t]);           
    @constraint(model, [t=1:T], imports[t,3] == hourly_data.imports_Morocco_mwh[t]);

    # Definition of exports    
    @constraint(model, [t=1:T], exports[t,1] == hourly_data.exports_France_mwh[t]);           
    @constraint(model, [t=1:T], exports[t,2] == hourly_data.exports_Portugal_mwh[t]);         
    @constraint(model, [t=1:T], exports[t,3] == hourly_data.exports_Morocco_mwh[t]);
            
    # OUTPUT CONSTRAINTS 
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
    @constraint(model, [t in high_prod_months], 0.30 * hourly_data.run_of_river_hydro_cap_mw[t] <= quantity[t,10] <= 0.50 * hourly_data.run_of_river_hydro_cap_mw[t]);
    @constraint(model, [t in med_high_prod_months], 0.20 * hourly_data.run_of_river_hydro_cap_mw[t] <= quantity[t,10] <= 0.40 * hourly_data.run_of_river_hydro_cap_mw[t]);
    @constraint(model, [t in med_low_prod_months], 0.15 * hourly_data.run_of_river_hydro_cap_mw[t] <= quantity[t,10] <= 0.30 * hourly_data.run_of_river_hydro_cap_mw[t]);
    @constraint(model, [t in low_prod_months], 0.10 * hourly_data.run_of_river_hydro_cap_mw[t] <= quantity[t,10] <= 0.15 * hourly_data.run_of_river_hydro_cap_mw[t]);
    @constraint(model, [t=2:T], quantity[t,10] - quantity[t-1,10] >= -0.2 * hourly_data.run_of_river_hydro_cap_mw[t]);
    @constraint(model, [t=2:T], quantity[t,10] - quantity[t-1,10] <= +0.2 * hourly_data.run_of_river_hydro_cap_mw[t]);

    # Pumped hydro
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

    # # Contraints on interconnectors capacity
    # @constraint(model, [t=1:T], imports[t,1] <= 2.8 * (1 + interconnectors_delta[1]));
    # @constraint(model, [t=1:T], exports[t,1] <= 3.3 * (1 + interconnectors_delta[2]));
    
    # @constraint(model, [t=1:T], imports[t,2] <= 3.0 * (1 + interconnectors_delta[3]));
    # @constraint(model, [t=1:T], exports[t,2] <= 3.0 * (1 + interconnectors_delta[4]));

    # @constraint(model, [t=1:T], imports[t,3] <= 0.6 * (1 + interconnectors_delta[5]));
    # @constraint(model, [t=1:T], exports[t,3] <= 0.9 * (1 + interconnectors_delta[6]));  
          
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
