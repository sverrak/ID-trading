# Status: 
# Description: Initial implementation of the multiasset hydropower producer's trading problem
 
### ----------- External Packages -----------
from gurobipy import *
import time

### ----------- Model Initialization -----------
model 									= Model('hydro')


### ----------- System Parameters -----------
printing_mode 							= True
default_parameters 						= True
parameter_file 							= "Data/hydro_multiasset_multiproduct_continuous_stochastic_MC_parameters_bm.txt"


### ----------- Model Parameters -----------
if (default_parameters == True):
	# Set model parameters
	params 								= 0
	number_of_trading_stages 			= 2
	number_of_production_units 			= 0
	number_of_dps 						= 0
	min_q 								= 0
	max_q 								= 0
	cpr 								= 0
	initial_storage 					= 0
	min_q 								= 0
	max_q 								= 0
	transaction_cost 					= 0
	production_cost 					= 0

	trading_stages 						= [x for x in range(number_of_trading_stages)]
	volume_options 						= []
	scenario_probabilities 				= []
	scenarios 							= []
	scenario_inflows 					= []
	scenario_production_costs			= []
	scenario_production_capacities 		= []
	transaction_cost 					= []
	production_cost 					= []
	production_capacities 				= []
	storage_bounds 						= []
	scenarios 							= []
	scenario_probabilities 				= []
	volume_options 						= []

	# Indices
	index_of_actions 					= 0								# If necessary, modify this
	index_of_q_bounds 					= index_of_actions + 1
	index_of_dps 						= index_of_q_bounds + 1
	index_of_scenario_probabilities 	= 4								# If necessary, modify this

  	
  
  	

else:
	# Retrieve parameters from file
	params 								= itphelper.read_parameters(parameter_file)

### ----------- Support functions ------------
def read_parameters(parameter_file):
	print("\nReading data...")
	start = time.time()
	# File handling
	with open(parameter_file) as f:
	    data = f.readlines()

	# Read initial data from file to calculate indices correctly
	volume_options 						= [float(i) for i in data[index_of_actions].strip().split("\t")]
	q_array 							= (list(data[index_of_q_bounds].strip().split("\t")))
	min_q		 						= float(q_array[0])
	temp_list 							= list(data[2].strip().split("\t"))
	number_of_dps 						= int(temp_list[0])
	number_of_production_units 			= int(temp_list[1])
	number_of_scenarios 				= int(temp_list[2])

	# How the input file should be read Part 2
	index_of_scenario_inflows 			= index_of_scenario_probabilities + 1
	index_of_scenarios 					= index_of_scenario_probabilities + 1 + number_of_dps
	index_of_bm_price 					= index_of_scenarios + number_of_dps * number_of_trading_stages
	index_of_production_capacities		= index_of_bm_price + number_of_dps
	index_of_transaction_costs 			= index_of_production_capacities + 1 + number_of_dps # If necessary, modify this
	index_of_cpr 						= index_of_transaction_costs + 1
	index_of_production_quantities 		= index_of_transaction_costs + 3
	index_of_production_costs 			= index_of_production_quantities + number_of_production_units
	index_of_initial_storage 			= index_of_production_costs + number_of_production_units + 1
	index_of_storage_bounds 			= index_of_initial_storage + 1

	# Instantiate datastructures
	scenario_probabilities = [float(i) for i in data[index_of_scenario_probabilities].split()]
	scenario_inflows = [[float(i) for i in data[index_of_scenario_inflows+j].split("\t")] for j in range(number_of_dps)]
	scenarios = [[] for i in range(number_of_dps)]
	scenario_production_capacities = [[] for i in range(number_of_production_units)]
	scenario_production_costs = [[] for i in range(number_of_production_units)]

	# Scenario production instantiation
	for pu in range(number_of_production_units):
		#scenario_production_costs[pu] = [[] for j in range(number_of_production_units)]
		#print(index_of_production_quantities+pu+number_of_production_units)
		scenario_production_costs[pu] = [float(i) for i in data[index_of_production_quantities+pu].split("\t")]
		#print(data[index_of_production_quantities+pu+number_of_production_units].split("\t"))
		scenario_production_capacities[pu] = [float(i) for i in data[index_of_production_quantities+pu+number_of_production_units].split("\t")]

	# Scenario price instantiation
	for dp in range(number_of_dps):
		scenarios[dp] = [[] for j in range(number_of_trading_stages)]
		for i in range(number_of_trading_stages):
			scenarios[dp][i] = [float(i) for i in data[index_of_scenarios+i].split("\t")]
	
	
	transaction_cost = float(data[index_of_transaction_costs])
	production_cost = [float(i) for i in data[index_of_production_costs].strip().split("\t")]
	production_capacities = [float(i) for i in data[index_of_production_quantities].strip().split("\t")]
	bms = [[float(i) for i in data[index_of_bm_price+x].strip().split("\t")] for x in range(number_of_dps)]
	cpr = float(data[index_of_cpr])

	temp_list = data[index_of_storage_bounds].split("\t")[:2]
	initial_storage = float(data[index_of_initial_storage])
	storage_bounds = [float(i) for i in temp_list]

	#print(scenarios)
	print("Time: " + str(int(10*time.time()-10*start)/10) + " seconds")

	return number_of_production_units, volume_options, min_q, max_q, scenario_probabilities, scenarios, transaction_cost, production_cost, production_capacities, cpr, number_of_dps, initial_storage, storage_bounds, scenario_inflows, scenario_production_capacities, scenario_production_costs, bms



def test_parameters():
	if(sum(scenario_probabilities) < 0.9999):
		#print(scenario_probabilities)
		raise ValueError('Sum of probabilities not equal 1 but' + str(sum(scenario_probabilities)))


number_of_production_units, volume_options, min_q, max_q, scenario_probabilities, scenarios, transaction_cost, production_cost, production_capacities, cpr, number_of_dps, initial_storage, storage_bounds, scenario_inflows, scenario_production_capacities, scenario_production_costs,bms = read_parameters(parameter_file)
#print(scenario_probabilities)
test_parameters()



### ----------- Variables -----------
bid_volume 								= [[[0 for j in range(number_of_trading_stages)] for i in range(len(scenario_probabilities))] for dp in range(number_of_dps)]
bid_prices 								= [[[model.addVar(name="bid_price_X_"+str(dp)+"_"+str(i)+"_"+str(j)) for j in range(number_of_trading_stages)] for i in range(len(scenario_probabilities))] for dp in range(number_of_dps)]
transaction_prices 						= [[[[0 for k in range(number_of_trading_stages)] for j in range(number_of_trading_stages)] for i in range(len(scenario_probabilities))] for dp in range(number_of_dps)]   # Average transaction price
transaction_volumes 					= [[[[0 for k in range(number_of_trading_stages)] for j in range(number_of_trading_stages)] for i in range(len(scenario_probabilities))] for dp in range(number_of_dps)]   # Total transaction volume
transaction_profits 					= [[[[0 for k in range(number_of_trading_stages)] for j in range(number_of_trading_stages)] for i in range(len(scenario_probabilities))] for dp in range(number_of_dps)]   # Total transaction volume
production_quantities 					= [[[model.addVar(vtype=GRB.CONTINUOUS,lb=min_q,ub=max_q, name="production_quantity_X_"+str(dp)+"_"+str(s)+"_"+str(j)) for j in range(number_of_production_units)] for s in range(len(scenario_probabilities))] for dp in range(number_of_dps)]    # Production quantity
storage_volume 							= [[model.addVar(name="storage_volume_X_X_"+str(dp)+"_"+str(s)) for s in range(len(scenario_probabilities))] for dp in range(number_of_dps)]
imbalance_volume						= [[model.addVar(name="imbalance_volume_X_X_"+str(dp)+"_"+str(s)) for s in range(len(scenario_probabilities))] for dp in range(number_of_dps)]

for dp in range(number_of_dps):
	for s in range(len(scenario_probabilities)):
		for i in range(number_of_trading_stages):
			bid_volume[dp][s][i] = model.addVar(vtype=GRB.CONTINUOUS, lb=-40.0,                              					# Bid volume
		                                    name=str("b_v_X_"+str(dp)+"_"+str(s)+"_"+str(i)))
for dp in range(number_of_dps):
	for s in range(len(scenario_probabilities)):
		for i in range(number_of_trading_stages):
			for j in range(number_of_trading_stages):
				transaction_prices[dp][s][i][j] = model.addVar(vtype=GRB.CONTINUOUS,		                        			# Transaction price
		                                    name=str("p_c_"+str(str(dp)+"_"+str(s)+"_"+str(i)+"_"+str(j))))

for dp in range(number_of_dps):
	for s in range(len(scenario_probabilities)):
		for i in range(number_of_trading_stages):
			for j in range(number_of_trading_stages):
				transaction_volumes[dp][s][i][j] = model.addVar(vtype=GRB.CONTINUOUS, lb=-40,			                    	# Transaction volume
		                                    name=str("v_c_"+str(str(dp)+"_"+str(s)+"_"+str(i)+"_"+str(j))))

for dp in range(number_of_dps):
	for s in range(len(scenario_probabilities)):
		for i in range(number_of_trading_stages):
			for j in range(number_of_trading_stages):
				transaction_profits[dp][s][i][j] = model.addVar(vtype=GRB.CONTINUOUS,          	                    			# Transaction profits
		                                    name=str("pi_c_"+str(str(dp)+"_"+str(s)+"_"+str(i)+"_"+str(j))))


### ----------- Constraints -----------
for s in range(len(scenario_probabilities)):
	model.addConstr(storage_volume[0][s] == initial_storage + scenario_inflows[0][s] - sum(production_quantities[0][s]), "storage_propagation_0")

for dp in range(1, number_of_dps):
	for s in range(len(scenario_probabilities)):
		model.addConstr(storage_volume[dp][s] == storage_volume[dp-1][s] + scenario_inflows[dp][s] - sum(production_quantities[dp][s]), "storage_propagation")

for dp in range(number_of_dps):
	for s in range(len(scenario_probabilities)):
		model.addConstr((sum(production_quantities[dp][s][x] for x in range(len(production_quantities[dp][s]))) - sum(transaction_volumes[dp][s][i][j] for j in range(number_of_trading_stages) for i in range(number_of_trading_stages)) - imbalance_volume[dp][s] == 0), "q=v_c_"+str(s))

for dp in range(number_of_dps):
	for s in range(len(scenario_probabilities)):
		for i in range(number_of_trading_stages):
			model.addConstr((transaction_volumes[dp][s][i][i] == bid_volume[dp][s][i]), "v_c<=b_v " + str(s) +" "+ str(i))

for dp in range(number_of_dps):
	for s in range(len(scenario_probabilities)):
		for i in range(number_of_trading_stages):
			for j in range(number_of_trading_stages):
				if(i != j):
					model.addConstr((transaction_volumes[dp][s][i][j] == 0), "v_c<=b_v " + str(s) +" "+ str(i))

for dp in range(number_of_dps):
	for s in range(len(scenario_probabilities)):
		for i in range(number_of_trading_stages):
			for j in range(number_of_trading_stages):
				model.addConstr(transaction_prices[dp][s][i][j] == scenarios[dp][i][s], "bp=pc"+str(i)+str(j))

for dp in range(number_of_dps):
	for s in range(len(scenario_probabilities)):
		for i in range(number_of_trading_stages):
			for j in range(number_of_trading_stages):
				model.addConstr(transaction_profits[dp][s][i][j] == transaction_volumes[dp][s][i][j] * transaction_prices[dp][s][i][j], "bp=pc"+str(i)+str(j))


for dp in range(number_of_dps):
	for s in range(len(scenario_probabilities)):
		model.addConstr(sum(production_quantities[dp][s]) <= max_q, "max_q")
		model.addConstr(sum(production_quantities[dp][s]) >= min_q, "max_q")

for dp in range(number_of_dps):
	for s in range(len(scenario_probabilities)):
		for i in range(number_of_production_units):
			model.addConstr((production_quantities[dp][s][i]) <= scenario_production_capacities[i][s], "production_capacities")

for dp in range(number_of_dps):
	for s in range(len(scenario_probabilities)):
		for i in range(number_of_trading_stages):
			model.addConstr(sum(transaction_volumes[dp][s][i][j] for j in range(0, i)) == 0, "no_transaction_before_placement")

for dp in range(number_of_dps):
	for s in range(len(scenario_probabilities)-1):	
		model.addConstr(bid_volume[dp][s][0] == bid_volume[dp][s+1][0], "non_anticipativity")

#model.addConstr(sum(sum(x) for x in v_c) 
#                  + v_bm_neg - v_bm_pos - q == -v_da, "sold_equals_generated")

#for s in range(len(scenario_probabilities)):
#	for i in range(number_of_trading_stages):
#		model.addConstr((sum(transaction_volumes[s][i][j] for j in range(number_of_trading_stages)) == bid_volume[s][i]), "v_c<=b_v " + str(i))


#for dp in range(number_of_dps):
#	for s in range(len(scenario_probabilities)):
#		for i in range(number_of_trading_stages):
			#print(sum(bid_option_decision[s][i] for j in range(len(volume_options))))
#			model.addConstr((sum(bid_option_decision[dp][s][i][j] for j in range(len(volume_options))) == 1), "bid_option_choose_one")

#for dp in range(number_of_dps):
	#for s in range(len(scenario_probabilities)):
	#	for i in range(number_of_trading_stages):
	#		model.addConstr(bid_volume[dp][s][i] - quicksum(bid_option_decision[dp][s][i][j]*volume_options[j] for j in range(len(volume_options))) == 0, "bid_option_decider")


### ----------- Objective Function -----------
#print(type(bms[0][0]), bms[0][0])
#print(type(scenario_probabilities[0]), scenario_probabilities[0])
#print(type(imbalance_volume[0][0]), imbalance_volume[0][0])
#model.setObjective((quicksum(scenario_probabilities[s]*transaction_profits[dp][s][i][j] for j in range(number_of_trading_stages) for i in range(number_of_trading_stages) for s in range(len(scenario_probabilities)) for dp in range(number_of_dps)) - sum(scenario_production_costs[x][s]*scenario_probabilities[s] * production_quantities[dp][s][x] for x in range(number_of_production_units) for s in range(len(scenario_probabilities)) for dp in range(number_of_dps)) - transaction_cost*sum(bid_volume[dp][s][i] for i in range(len(bid_volume[dp][s])) for s in range(len(scenario_probabilities)) for dp in range(number_of_dps)) - ((scenario_probabilities[s] * imbalance_volume[dp][s] * bms[dp][s] for s in range(len(scenario_probabilities))) for dp in range(number_of_dps))), GRB.MAXIMIZE)
model.setObjective((quicksum(scenario_probabilities[s]*transaction_profits[dp][s][i][j] for j in range(number_of_trading_stages) for i in range(number_of_trading_stages) for s in range(len(scenario_probabilities)) for dp in range(number_of_dps)) - sum(scenario_production_costs[x][s]*scenario_probabilities[s] * production_quantities[dp][s][x] for x in range(number_of_production_units) for s in range(len(scenario_probabilities)) for dp in range(number_of_dps)) - transaction_cost*sum(bid_volume[dp][s][i] for i in range(len(bid_volume[dp][s])) for s in range(len(scenario_probabilities)) for dp in range(number_of_dps)) - sum(scenario_probabilities[s] * sum(bms[dp][s] * imbalance_volume[dp][s] for dp in range(number_of_dps)) for s in range(len(scenario_probabilities)))), GRB.MAXIMIZE)


# sum(scenario_production_costs[x][s]*scenario_probabilities[s] * production_quantities[dp][s][x] for x in range(number_of_production_units) for s in range(len(scenario_probabilities)) for dp in range(number_of_dps))
# (scenario_probabilities[s] * (bms[dp][s] * imbalance_volume[dp][s] for dp in range(number_of_dps)) for s in range(len(scenario_probabilities)))
# transaction_cost*sum(bid_volume[dp][s][i] for i in range(len(bid_volume[dp][s])) for s in range(len(scenario_probabilities)) for dp in range(number_of_dps))
### ----------- Optimization -----------

model.optimize()


### ----------- Output Results -----------
def print_vars():
	for v in model.getVars():
	    
	    print("%s %f" % (v.Varname, v.X))


	for dp in range(number_of_dps):
		for s in range(len(scenario_probabilities)):
			print("Inflow_X_X_X_"+str(dp)+"_"+str(s)+ " " + str(scenario_inflows[dp][s]))

	model.write("Output/hydro_multiasset_multiproduct_continuous_stochastic_MC_bm_output.sol")

if(printing_mode == True):
	print_vars()

### ----------- Support Functions -----------














