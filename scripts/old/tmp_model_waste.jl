# aquí muevo todo lo que creo que no debería ir en el script que define el modelo

# ========== las demand functions para imports e exports ==========
# # Define parameters for imports and exports demand functions
# hourly_data.b_imp_FRA = elas_imports * hourly_data.imports_France_mwh ./ hourly_data.spot_price_eur_mwh
# hourly_data.b_imp_POR = elas_imports * hourly_data.imports_Portugal_mwh ./ hourly_data.spot_price_eur_mwh
# hourly_data.b_imp_MOR = elas_imports * hourly_data.imports_Morocco_mwh ./ hourly_data.spot_price_eur_mwh
# hourly_data.b_exp_FRA = elas_exports * hourly_data.exports_France_mwh ./ hourly_data.spot_price_eur_mwh
# hourly_data.b_exp_POR = elas_exports * hourly_data.exports_Portugal_mwh ./ hourly_data.spot_price_eur_mwh
# hourly_data.b_exp_MOR = elas_exports * hourly_data.exports_Morocco_mwh ./ hourly_data.spot_price_eur_mwh

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


# esto estaba dentro del modelo

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

    
    # # Contraints on interconnectors capacity
    # @constraint(model, [t=1:T], imports[t,1] <= 2.8 * (1 + interconnectors_delta[1]));
    # @constraint(model, [t=1:T], exports[t,1] <= 3.3 * (1 + interconnectors_delta[2]));

    # @constraint(model, [t=1:T], imports[t,2] <= 3.0 * (1 + interconnectors_delta[3]));
    # @constraint(model, [t=1:T], exports[t,2] <= 3.0 * (1 + interconnectors_delta[4]));

    # @constraint(model, [t=1:T], imports[t,3] <= 0.6 * (1 + interconnectors_delta[5]));
    # @constraint(model, [t=1:T], exports[t,3] <= 0.9 * (1 + interconnectors_delta[6])); 