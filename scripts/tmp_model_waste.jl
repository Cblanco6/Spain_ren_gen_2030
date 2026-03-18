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
