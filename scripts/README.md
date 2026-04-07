# Scripts

*this is still just a draft*

This folder contains the scripts needed to replicate the paper. 
Some scripts are written in R (the preferred language of the authors) and other in julia, since (in principle) works better for the kind of model we define.

They are organized as follows:

1. Scripts to get the historical calibration data

These are non essential to replicate the paper as we provide the complete datasets, but show where the data was sourced from.
Note that both the [ESIOS](https://api.esios.ree.es/) and [ENTSO-e](https://transparencyplatform.zendesk.com/hc/en-us/articles/12845911031188-How-to-get-security-token) API request a valid personal token, which you can ask in the hyperlinks to each source.

-   11_list_all_indicators.R creates an Excel file with the code and description of all the indicators available in the ESIOS API saved in the data/ folder.
-   12_retrieve_api_data.R gets all the data from a selection of indicators
-   13_process_historical_data.R processes the dataset such that it is ready to input to the model for calibration an saves it to the data/ folder as a csv file.

2. Scripts to run the Monte Carlo Simulations

These are the core scripts of the project: the model and Monte Carlo simulations.
They work with the following datasets in data/

-   complete_dataset
-   fixed_data
-   projection_deltas
-   scenario_parameters

Forking this repository you can play with the model and/or scenarios to test how the model responds to other assumptions.
Note that you will need a [Gurobi license](https://www.gurobi.com/academia/academic-program-and-licenses/) to run the model. 
If you do, we would love to get your feedback!

-   21_electricity_market_model.jl defines the model we have used to simulate the Spanish electricity market
-   22_model_calibration.jl *(or 22_model_calibration.ipynb)* shows how the model replicates historical patterns  
-   23_run_monte_carlo.jl runs the Monte Carlo Simulations for each scenario and saves the results into the output/raw_results folder as csv. files

3. Scrips to create the graphs and other output

-   31_main_results_grpahs.R creates the graphs for the main results (distributions for prices and )
-   32_ins_cap_per_scenario.R creates the graphs that compare the installed capacity of each scenario per percentile of average renewable generation.
-   33_other_grpahs.R *(or 33_annex_graphs. R)* creates the rest of graphs
-   *3x_abatement_costs.R* runs a reduced-from model to compute the abatement cost of BESS and other technologies. 

All graphs are saved into the output/graphs folder.
