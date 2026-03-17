# Spain_ren_gen_2030

This is the repository for the paper: "Modeling Spain’s Power System Expansion: A Path Toward 81% Renewable Integration and Resilient Electrification by 2030" *(revisar el título una vez sea definitivo)* *published in ...* 

## Abstract (main results)

*(revisar una vez sea definitivo)* 

This study provides a high-resolution probabilistic assessment of Spain’s 2030 renewable electricity targets, as outlined in the 2024 update of the National Integrated Energy and Climate Plan (PNIEC).
Using a partial-equilibrium social planner model developed in JuMP (Julia) and 10,000 Monte Carlo iterations, we evaluate the system’s resilience under four strategic scenarios: Baseline, Nuclear, Optimistic, and Climate Change.

Our findings reveal a significant "implementation gap": the baseline scenario yields an average renewable share of 72.7%, meeting the official 81% target in only 0.8% of the iterations. Even under an Optimistic scenario with enhanced storage and demand flexibility, the success probability only reaches 19.3%. 
Furthermore, we identify a "Nuclear Paradox" as maintaining the nuclear fleet provides price
stability (€51.2/MWh) and lower emissions but technically hinders renewable penetration (67.8%) due to baseload inflexibility.

Economic analysis shows that battery energy storage systems (BESS) face high abatement costs (€416–1,025/tCO2), suggesting that merchant revenues alone are insufficient for deployment. 
We conclude that bridging the gap to 2030 requires an urgent policy shift toward capacity markets and aggressive demand-side response to manage the stochastic nature of a high-RES system.

## Methdology summary

*revisar*

We have developed a partial-equilibrium social planner model that replciates the functioning of the Spanish electricity market (under perfect competition). 
In particular, it maximizes social welfare by choosing the optimal electricity mix for each hour, subject to market clearing, capacity constraints and intertemporal linkages in production (ramp-up and ramp-down processes in thermal units, storage flows for batteries and pumped hydro, and almost continuous production for nuclear).
We have used 2020-2024 high-frecuency data to calibrate the model, including hourly generation, sectoral demand, electricity prices, fuel prices and EU ETS prices as well as montlhly installed capacity data and fixed and variable O&M cost estimates for up to 16 different electricity-generating technologies.

Since the purpose of the paper is to analyze the electricity system in 2030, we have gathered data on projections of installed capacity, electricity demand and commodity prices to that target year.
To handle the uncertainty on what will be the specific realization, we run 10,000 Monte Carlo simulations on 4 different scenarios, keeping track of the average renewable share in each iteration as well as other outcomes such as the total emissions, battery inflos and outflows, etcetera.

## Versions

The first version of the project was the Master Thesis I conducted at Barcelona School of Economics alongside Cristobal Blanco and Tomás Butelman. 
Cristobal and Pau continued working in the second version, polishing scripts, validating data collection, updating parts of the analysis, diving deeper into the policy implications and ultimately adapting the content to journal format. 

## How to navigate the repository

The repository follows standard practices: it contains three folders (data, scrpits and outputs) containing all the ingredients needed to replicate the paper.
Each folder contains its own README.md file with specific guidelines to navigate each folder. 

## Instructions to replicate the paper

...

## Limitations of the study

...

## Credits

*How to cite our work?*
...