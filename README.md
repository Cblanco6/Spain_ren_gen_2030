# Spain_ren_gen_2030

This is the repository for the paper: "Modeling Spain’s Power System Expansion: A Path Toward 81% Renewable Integration and Resilient Electrification by 2030" *(revisar el título una vez sea definitivo)* *published in ...* 

## Abstract

*(revisar una vez sea definitivo)* 

This study provides a high-resolution probabilistic assessment of Spain’s 2030 renewable electricity targets, as outlined in the 2024 update of the National Integrated Energy and Climate Plan (PNIEC).
Using a partial-equilibrium social planner model developed in JuMP (Julia) and 10,000 Monte Carlo iterations, we evaluate the system’s resilience under four strategic scenarios: Baseline, Nuclear, Optimistic, and Climate Change.

Our findings reveal a significant "implementation gap": the baseline scenario yields an average renewable share of 72.7%, meeting the official 81% target in only 0.8% of the iterations. Even under an Optimistic scenario with enhanced storage and demand flexibility, the success probability only reaches 19.3%. 
Furthermore, we identify a "Nuclear Paradox" as maintaining the nuclear fleet provides price
stability (€51.2/MWh) and lower emissions but technically hinders renewable penetration (67.8%) due to baseload inflexibility.

Economic analysis shows that battery energy storage systems (BESS) face high abatement costs (€416–1,025/tCO2), suggesting that merchant revenues alone are insufficient for deployment. 
We conclude that bridging the gap to 2030 requires an urgent policy shift toward capacity markets and aggressive demand-side response to manage the stochastic nature of a high-RES system.

## Methdology summary

We have developed a 

We develop a partial-equilibrium power system model
using the JuMP modeling language in Julia [13], solved with
Gurobi [8]. Following Reguant’s methodology for large-
scale renewable integration in the Iberian Peninsula [17],
the model simulates a social planner who aims to maximize
social welfare defined as the sum of consumer surplus (CS)
and producer revenue (PR) minus the total costs, by choosing
the optimal electricity mix for each hour, subject to market
clearing and capacity constraints

## Versions

The first version of the project was the Master Thesis I conducted at Barcelona School of Economics alongside Cristobal Blanco and Tomás Butelman. 
Cristobal and Pau continued working in the second version, polishing scripts, validating data collection, updating parts of the analysis, diving deeper into the policy implications and ultimately adapting the content to journal format. 

## How to navigate the repository

The repository follows standard practices: it contains three folders (data, scrpits and outputs) containing all the 


## Instructions to replicate the paper


## Limitations of the study

## Credits

*How to cite our work?*

Common structure:
Title
Motivation
Description of the technology used (and why)
Description of the process (and why we have done it this way)
Table of contents
Limitations
Challenges
Intended use
Credits
