# Scripts

*this is still just a draft*
*when we have the definitive version, number the scripts to make order clearer*

This folder contains the scripts needed to replicate the paper. 
The scripts defining the model and to run the Monte Carlo simulations are written in Julia, since we think is the most suited for this kind of optimization models. Data extraction, processing and analysis of results is done in R (Pau) or Python (Cristobal)

They are organized as follows:

1. Scripts to get the historical calibration data

These are non essential to replicate the paper as we provide the complete datasets, but show where the data was sourced from.
Note that both the [ESIOS](https://api.esios.ree.es/) and [ENTSO-e](https://transparencyplatform.zendesk.com/hc/en-us/articles/12845911031188-How-to-get-security-token) API request a valid personal token, which you can ask in the hyperlinks to each source.

*Cristobal to edit this when he is done*

-   11_list_all_indicators.R creates an Excel file with the code and description of all the indicators available in the ESIOS API saved in the data/ folder.
-   12_retrieve_api_data.R gets all the data from a selection of indicators
-   13_process_historical_data.R processes the dataset such that it is ready to input to the model for calibration an saves it to the data/ folder as a csv file.

2. Scripts to run the Monte Carlo Simulations

-   21_model_electricity_market.jl defines the model we have used to simulate the Spanish electricity market
-   22_auxiliary_functions.jl *(or 22_model_calibration.ipynb)* shows how the model replicates historical patterns  
-   23_monte_carlo_simulations.jl runs the Monte Carlo Simulations for each scenario and saves the results into the output/detaild_results folder as csv. files

These core scripts of the project work with the following datasets in data/

-   historical_data
-   technology_data
-   projection_deltas
-   technical_params
-   scenario_params

Forking this repository you can play adjusting the model and/or scenario-specific assumptions to test how the model responds to other assumptions.
Note that you will need a [Gurobi license](https://www.gurobi.com/academia/academic-program-and-licenses/) to run the model (free for students!). 
If you do, we would love to get your feedback!

3. Scrips to create the graphs and other outputs

-   31_historical_data_summary_statistics.R creates summary statistics and graphs of the historical data (2020-2024)
-   32_main_results.R creates the summary tables and graphs for the main results (distributions for generation and prices)
-   33_other_grpahs.R *(or 33_annex_graphs. R)* creates the rest of graphs

All graphs are saved into the output/graphs folder.
