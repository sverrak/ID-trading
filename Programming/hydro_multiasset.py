# Status: 
# Description: Initial implementation of the multiasset hydropower producer's trading problem
 
### ----------- External Packages -----------
from gurobipy import *
import time

### ----------- Model Initialization -----------
model = Model('hydro')


### ----------- System Parameters -----------
print_output = True
default_parameters = True
parameter_file = "Data/hydro_multiasset_parameters.txt"


### ----------- Model Parameters -----------
if (default_parameters == True):
  	# Set model parameters
  	params = 0
  	number_of_trading_stages = 2
  	trading_stages = [x for x in range(number_of_trading_stages)]

  	# How the input file should be read
  	index_of_actions = 0
  	index_of_q_bounds = 1
  	index_of_scenario_probabilities = [3,6]
  	index_of_scenarios = [index_of_scenario_probabilities[i]+1 for i in range(len(index_of_scenario_probabilities))]
  	index_of_transaction_costs = 9
  	index_of_cpr = 10
  	index_of_production_quantities = 12
  	index_of_production_costs = 13
  	
  
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

	print("hello",data[index_of_q_bounds])
	#print(list(data[index_of_q_bounds].strip().split("\t")))
	volume_options = [float(i) for i in data[index_of_actions].strip().split("\t")]
	q_array = (list(data[index_of_q_bounds].strip().split("\t")))
	min_q, max_q = float(q_array[0]), float(q_array[1])
	
	scenarios[0] = [scenarios[0][0] for i in range(len(scenarios[1]))]
	transaction_cost = float(data[index_of_transaction_costs])
	#print(data[index_of_production_costs])
	
	production_cost = [float(i) for i in data[index_of_production_costs].strip().split("\t")]
	production_capacities = [float(i) for i in data[index_of_production_quantities].strip().split("\t")]
	cpr = float(data[index_of_cpr])

	#print(scenarios)
	print("Time: " + str(int(10*time.time()-10*start)/10) + " seconds")

	return volume_options, min_q, max_q, scenario_probabilities, scenarios, transaction_cost, production_cost, production_capacities, cpr

volume_options, min_q, max_q, scenario_probabilities, scenarios, transaction_cost, production_cost, production_capacities, cpr = (read_parameters(parameter_file))

### ----------- Datastructures -----------
print("Capacity")
print(production_capacities)
print("Production cost")
print(production_cost)
print("Maxq")
print(max_q)

### ----------- Variables -----------
bid_volume = [[0 for j in range(number_of_trading_stages)] for i in range(len(scenario_probabilities[1]))]
bid_option_decision = [[[0 for k in range(len(volume_options))] for j in range(number_of_trading_stages)] for i in range(len(scenario_probabilities[1]))]
bid_prices = [[model.addVar(name="bid_price_"+str(i)+"_"+str(j)) for j in range(number_of_trading_stages)] for i in range(len(scenario_probabilities[1]))]
transaction_prices = [[[0 for k in range(number_of_trading_stages)] for j in range(number_of_trading_stages)] for i in range(len(scenario_probabilities[1]))]    # Average transaction price
transaction_volumes = [[[0 for k in range(number_of_trading_stages)] for j in range(number_of_trading_stages)] for i in range(len(scenario_probabilities[1]))]    # Total transaction volume
transaction_profits = [[[0 for k in range(number_of_trading_stages)] for j in range(number_of_trading_stages)] for i in range(len(scenario_probabilities[1]))]    # Total transaction volume
production_quantities = [[model.addVar(vtype=GRB.CONTINUOUS,lb=min_q,ub=max_q, name="production_quantity_"+str(s)+"_"+str(j)) for j in range(len(production_capacities))] for s in range(len(scenario_probabilities[1]))]    # Production quantity

for s in range(len(scenario_probabilities[1])):
	for i in range(number_of_trading_stages):
		bid_volume[s][i] = model.addVar(vtype=GRB.CONTINUOUS,                              					# Transaction price
	                                    name=str("b_v_"+str(str(s)+"_"+str(i))))

for s in range(len(scenario_probabilities[1])):
	for i in range(number_of_trading_stages):
		for j in range(len(volume_options)):
			bid_option_decision[s][i][j] = model.addVar(vtype=GRB.BINARY,      	                        	# Transaction price
                        name=str("delta_b_"+str(str(s)+"_"+str(i)+"_"+str(j))))

for s in range(len(scenario_probabilities[1])):
	for i in range(number_of_trading_stages):
		for j in range(number_of_trading_stages):
			transaction_prices[s][i][j] = model.addVar(vtype=GRB.CONTINUOUS,		                        # Transaction price
	                                    name=str("p_c_"+str(str(s)+"_"+str(i)+"_"+str(j))))

for s in range(len(scenario_probabilities[1])):
	for i in range(number_of_trading_stages):
		for j in range(number_of_trading_stages):
			transaction_volumes[s][i][j] = model.addVar(vtype=GRB.CONTINUOUS,			                    # Transaction price
	                                    name=str("v_c_"+str(str(s)+"_"+str(i)+"_"+str(j))))

for s in range(len(scenario_probabilities[1])):
	for i in range(number_of_trading_stages):
		for j in range(number_of_trading_stages):
			transaction_profits[s][i][j] = model.addVar(vtype=GRB.CONTINUOUS,          	                    # Transaction price
	                                    name=str("pi_c_"+str(str(s)+"_"+str(i)+"_"+str(j))))


### ----------- Constraints -----------
#model.addConstr(sum(sum(x) for x in v_c) 
#                  + v_bm_neg - v_bm_pos - q == -v_da, "sold_equals_generated")
print(production_quantities[0][0])
print(transaction_volumes[0][0][0])
for s in range(len(scenario_probabilities[1])):
	model.addConstr((sum(production_quantities[s][x] for x in range(len(production_quantities[s]))) - sum(transaction_volumes[s][i][j] for j in range(number_of_trading_stages) for i in range(number_of_trading_stages)) == 0), "q=v_c_"+str(s))

#for s in range(len(scenario_probabilities[1])):
#	for i in range(number_of_trading_stages):
#		model.addConstr((sum(transaction_volumes[s][i][j] for j in range(number_of_trading_stages)) == bid_volume[s][i]), "v_c<=b_v " + str(i))

for s in range(len(scenario_probabilities[1])):
	for i in range(number_of_trading_stages):
		model.addConstr((transaction_volumes[s][i][i] == bid_volume[s][i]), "v_c<=b_v " + str(s) +" "+ str(i))

for s in range(len(scenario_probabilities[1])):
	for i in range(number_of_trading_stages):
		for j in range(number_of_trading_stages):
			if(i != j):
				model.addConstr((transaction_volumes[s][i][j] == 0), "v_c<=b_v " + str(s) +" "+ str(i))

for s in range(len(scenario_probabilities[1])):
	for i in range(number_of_trading_stages):
		#print(sum(bid_option_decision[s][i] for j in range(len(volume_options))))
		model.addConstr((sum(bid_option_decision[s][i][j] for j in range(len(volume_options))) == 1), "bid_option_choose_one")

for s in range(len(scenario_probabilities[1])):
	for i in range(number_of_trading_stages):
		for j in range(number_of_trading_stages):
			model.addConstr(transaction_prices[s][i][j] == scenarios[i][s], "bp=pc"+str(i)+str(j))

for s in range(len(scenario_probabilities[1])):
	for i in range(number_of_trading_stages):
		for j in range(number_of_trading_stages):
			model.addConstr(transaction_profits[s][i][j] == transaction_volumes[s][i][j] * transaction_prices[s][i][j], "bp=pc"+str(i)+str(j))

for s in range(len(scenario_probabilities[1])):
	for i in range(number_of_trading_stages):
		model.addConstr(bid_volume[s][i] - quicksum(bid_option_decision[s][i][j]*volume_options[j] for j in range(len(volume_options))) == 0, "bid_option_decider")

for s in range(len(scenario_probabilities[1])):
	model.addConstr(sum(production_quantities[s]) <= max_q, "max_q")
	model.addConstr(sum(production_quantities[s]) >= min_q, "max_q")

for s in range(len(scenario_probabilities[1])):
	for i in range(len(production_capacities)):
		print(s,i,production_capacities[i])
		model.addConstr((production_quantities[s][i]) <= production_capacities[i], "production_capacities")


for s in range(len(scenario_probabilities[1])):
	for i in range(number_of_trading_stages):
		print(transaction_volumes[s][i][j] for j in range(0, i))
		model.addConstr(sum(transaction_volumes[s][i][j] for j in range(0, i)) == 0, "no_transaction_before_placement")

for s in range(len(scenario_probabilities[1])-1):	
	for j in range(len(volume_options)):
		model.addConstr(bid_option_decision[s][0][j] == bid_option_decision[s+1][0][j], "non-anticipativity")


### ----------- Objective Function -----------

#model.setObjective((sum(scenario_probabilities[1][s]*transaction_profits[s][i][j] for j in range(number_of_trading_stages) for i in range(number_of_trading_stages) for s in range(len(scenario_probabilities[1]))) - (production_cost[x] * sum(scenario_probabilities[1][s] * production_quantities[s][x] for s in range(len(scenario_probabilities[1]))) for x in range(len(production_cost)))), GRB.MAXIMIZE)
print(production_cost)
print(production_quantities)
print(scenario_probabilities[1])
model.setObjective((sum(scenario_probabilities[1][s]*transaction_profits[s][i][j] for j in range(number_of_trading_stages) for i in range(number_of_trading_stages) for s in range(len(scenario_probabilities[1]))) - sum(production_cost[x]*scenario_probabilities[1][s] * production_quantities[s][x] for x in range(len(production_cost)) for s in range(len(scenario_probabilities[1]))) - transaction_cost*sum(bid_volume[s][i] for i in range(len(bid_volume[s])) for s in range(len(scenario_probabilities)))), GRB.MAXIMIZE)


### ----------- Optimization -----------
model.optimize()


### ----------- Output Results -----------
for v in model.getVars():
    
    print("%s %f" % (v.Varname, v.X))

model.write("Output/hydro_multiasset_output.sol")

### ----------- Support Functions -----------














