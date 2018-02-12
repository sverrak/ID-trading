# Status: 
# Description: Initial implementation of the hydropower producer's trading problem

### ----------- External Packages -----------
from gurobipy import *
import time

### ----------- Model Initialization -----------
model = Model('hydro')


### ----------- System Parameters -----------
print_output = True
default_parameters = True
parameter_file = "Data/Input/hydro_parameters.txt"


### ----------- Model Parameters -----------
if (default_parameters == True):
  	# Set model parameters
  	params = 0
  	number_of_trading_stages = 2
  	trading_stages = [x for x in range(number_of_trading_stages)]

  	# How the input file should be read
  	index_of_actions = 0
  	index_of_q_bounds = 1
  	index_of_transaction_costs = 9
  	index_of_production_costs = 10
  	index_of_scenario_probabilities = [3,6]
  	index_of_scenarios = [4,7]
  	
  
  	# Parameters to be set in input file
  	min_q = 0
  	max_q = 0
  	transaction_cost = 0
  	production_cost = 0
  	scenarios = []
  	scenario_probabilities = []
  	volume_options = []

else:
  # Retrieve parameters from file
  params = itphelper.read_parameters(parameter_file)

### ----------- Support functions ------------
def read_parameters(parameter_file):
	print("\nReading data...")
	start = time.time()
	# File handling
	with open(parameter_file) as f:
	    data = f.readlines()
	#print(list(data[index_of_q_bounds].strip().split("\t")))
	volume_options = [float(i) for i in data[index_of_actions].strip().split("\t")]
	q_array = (list(data[index_of_q_bounds].strip().split("\t")))
	min_q, max_q = float(q_array[0]), float(q_array[1])
	for t in range(len(index_of_scenario_probabilities)):
		#print(data[index_of_scenarios[t]].split("\t"))
		scenario_probabilities.append([float(i) for i in data[index_of_scenario_probabilities[t]].split()])
		scenarios.append([float(i) for i in data[index_of_scenarios[t]].split("\t")])
	transaction_cost = float(data[index_of_transaction_costs])
	production_cost = float(data[index_of_production_costs])

	#print(scenarios)
	print("Time: " + str(int(10*time.time()-10*start)/10) + " seconds")

	return volume_options, min_q, max_q, scenario_probabilities, scenarios, transaction_cost, production_cost

volume_options, min_q, max_q, scenario_probabilities, scenarios, transaction_cost, production_cost = (read_parameters(parameter_file))

### ----------- Datastructures -----------


### ----------- Variables -----------
bid_volume = [[0 for j in range(number_of_trading_stages)] for i in range(len(scenario_probabilities[1]))]
bid_option_decision = [[[0 for k in range(len(volume_options))] for j in range(number_of_trading_stages)] for i in range(len(scenario_probabilities[1]))]
bid_prices = [[model.addVar(name="bid_price_"+str(i)+"_"+str(j)) for j in range(number_of_trading_stages)] for i in range(len(scenario_probabilities[1]))]
transaction_prices = [[[0 for k in range(number_of_trading_stages)] for j in range(number_of_trading_stages)] for i in range(len(scenario_probabilities[1]))]    # Average transaction price
transaction_volumes = [[[0 for k in range(number_of_trading_stages)] for j in range(number_of_trading_stages)] for i in range(len(scenario_probabilities[1]))]    # Total transaction volume
transaction_profits = [[[0 for k in range(number_of_trading_stages)] for j in range(number_of_trading_stages)] for i in range(len(scenario_probabilities[1]))]    # Total transaction volume

production_quantities = [model.addVar(vtype=GRB.CONTINUOUS,lb=min_q,ub=max_q, name="production_quantity_"+str(s)) for s in range(len(scenario_probabilities[1]))]    # Production quantity


for s in range(len(scenario_probabilities[1])):
	for i in range(number_of_trading_stages):
		bid_volume[s][i] = model.addVar(vtype=GRB.CONTINUOUS,                              # Transaction price
	                                    name=str("b_v_"+str(str(s)+"_"+str(i))))

for s in range(len(scenario_probabilities[1])):
	for i in range(number_of_trading_stages):
		for j in range(len(volume_options)):
			bid_option_decision[s][i][j] = model.addVar(vtype=GRB.BINARY,                              # Transaction price
                        name=str("delta_b_"+str(str(s)+"_"+str(i)+"_"+str(j))))

for s in range(len(scenario_probabilities[1])):
	for i in range(number_of_trading_stages):
		for j in range(number_of_trading_stages):
			transaction_prices[s][i][j] = model.addVar(vtype=GRB.CONTINUOUS,                              # Transaction price
	                                    name=str("p_c_"+str(str(s)+"_"+str(i)+"_"+str(j))))

for s in range(len(scenario_probabilities[1])):
	for i in range(number_of_trading_stages):
		for j in range(number_of_trading_stages):
			transaction_volumes[s][i][j] = model.addVar(vtype=GRB.CONTINUOUS,                              # Transaction price
	                                    name=str("v_c_"+str(str(s)+"_"+str(i)+"_"+str(j))))

for s in range(len(scenario_probabilities[1])):
	for i in range(number_of_trading_stages):
		for j in range(number_of_trading_stages):
			transaction_profits[s][i][j] = model.addVar(vtype=GRB.CONTINUOUS,                              # Transaction price
	                                    name=str("pi_c_"+str(str(s)+"_"+str(i)+"_"+str(j))))


### ----------- Constraints -----------
#model.addConstr(sum(sum(x) for x in v_c) 
#                  + v_bm_neg - v_bm_pos - q == -v_da, "sold_equals_generated")
for s in range(len(scenario_probabilities[1])):
	model.addConstr((production_quantities[s] - sum(transaction_volumes[s][i][j] for j in range(number_of_trading_stages) for i in range(number_of_trading_stages)) == 0), "q=v_c_"+str(s))

for s in range(len(scenario_probabilities[1])):
	for i in range(number_of_trading_stages):
		model.addConstr((sum(transaction_volumes[s][i][j] for j in range(number_of_trading_stages)) <= bid_volume[s][i]), "v_c<=b_v " + str(i))

for s in range(len(scenario_probabilities[1])):
	for i in range(number_of_trading_stages):
		#print(sum(bid_option_decision[s][i] for j in range(len(volume_options))))
		model.addConstr((sum(bid_option_decision[s][i][j] for j in range(len(volume_options))) == 1), "bid_option_choose_one")

for s in range(len(scenario_probabilities[1])):
	for i in range(number_of_trading_stages):
		for j in range(number_of_trading_stages):
			model.addConstr(transaction_prices[s][i][j] == scenarios[1][s], "bp=pc"+str(i)+str(j))

for s in range(len(scenario_probabilities[1])):
	for i in range(number_of_trading_stages):
		for j in range(number_of_trading_stages):
			model.addConstr(transaction_profits[s][i][j] == transaction_volumes[s][i][j] * transaction_prices[s][i][j], "bp=pc"+str(i)+str(j))
print(bid_option_decision[0][0])
print(volume_options)
print(bid_volume[0])
for s in range(len(scenario_probabilities[1])):
	for i in range(number_of_trading_stages):
		model.addConstr(bid_volume[s][i] - quicksum(bid_option_decision[s][i][j]*volume_options[j] for j in range(len(volume_options))) == 0, "bid_option_decider")

for s in range(len(scenario_probabilities[1])):
	model.addConstr(production_quantities[s] <= max_q, "max_q")
	model.addConstr(production_quantities[s] >= min_q, "max_q")

### ----------- Objective Function -----------

model.setObjective((sum(scenario_probabilities[1][s]*transaction_profits[s][i][j] for j in range(number_of_trading_stages) for i in range(number_of_trading_stages) for s in range(len(scenario_probabilities[1]))) - production_cost * sum(scenario_probabilities[1][s] * production_quantities[s] for s in range(len(scenario_probabilities[1])))), GRB.MAXIMIZE)


### ----------- Optimization -----------
model.optimize()


### ----------- Output Results -----------
for v in model.getVars():
    if v.X != 0:
        print("%s %f" % (v.Varname, v.X))

model.write("hydro_output.sol")

### ----------- Support Functions -----------














