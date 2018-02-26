# Status: 
# Description: Initial implementation of the multiasset hydropower producer's trading problem
 
### ----------- External Packages -----------
from gurobipy import *
import time
import xlsxwriter
### ----------- Model Initialization -----------
model 									= Model('hydro')


### ----------- System Parameters -----------
print_output 							= True
default_parameters 						= True
parameter_file 							= "Data/hydro_multiasset_multiproduct_continuous_stochastic_MC_parameters_bm_OF.txt"
variables_file_name						= "variables_bm.xlsx"
printing_mode							= False


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
	volume_bounds 						= [float(i) for i in data[index_of_actions].strip().split("\t")]
	q_array 							= (list(data[index_of_q_bounds].strip().split("\t")))
	min_q, max_q 						= float(q_array[0]), float(q_array[1])
	temp_list 							= list(data[2].strip().split("\t"))
	number_of_dps 						= int(temp_list[0])
	number_of_production_units 			= int(temp_list[1])
	number_of_scenarios 				= int(temp_list[2])
	bm_upper_bound						= int(temp_list[3])

	# How the input file should be read Part 2
	index_of_scenario_inflows 			= index_of_scenario_probabilities + 1
	index_of_scenarios 					= index_of_scenario_probabilities + 1 + number_of_dps
	index_of_bm_price 					= index_of_scenarios + number_of_dps * number_of_trading_stages
	index_of_transaction_costs 			= index_of_bm_price + 1 + number_of_dps # If necessary, modify this
	index_of_cpr 						= index_of_transaction_costs + 1
	index_of_production_quantities 		= index_of_transaction_costs + 3
	index_of_production_costs 			= index_of_production_quantities + number_of_production_units
	index_of_initial_storage 			= index_of_production_costs + number_of_production_units + 1
	index_of_storage_bounds 			= index_of_initial_storage + 1
	index_of_overflow					= index_of_storage_bounds + 1


	# Data structures
	scenario_probabilities 				= [float(i) for i in data[index_of_scenario_probabilities].split()]
	scenario_inflows 					= [[float(i) for i in data[index_of_scenario_inflows+j].split("\t")] for j in range(number_of_dps)]
	scenarios 							= [[] for i in range(number_of_dps)]
	scenario_production_capacities 		= [[] for i in range(number_of_production_units)]
	scenario_production_costs 			= [[] for i in range(number_of_production_units)]
	transaction_cost 					= float(data[index_of_transaction_costs])
	production_cost 					= [float(i) for i in data[index_of_production_costs].strip().split("\t")]
	production_capacities 				= [float(i) for i in data[index_of_production_quantities].strip().split("\t")]
	cpr 								= float(data[index_of_cpr])
	temp_list 							= data[index_of_storage_bounds].split("\t")[:2]
	storage_bounds 						= [float(i) for i in temp_list]
	initial_storage 					= float(data[index_of_initial_storage])
	bms 								= [[float(i) for i in data[index_of_bm_price+x].strip().split("\t")] for x in range(number_of_dps)]

	for pu in range(number_of_production_units):
		scenario_production_costs[pu] 		= [float(i) for i in data[index_of_production_quantities+pu].split("\t")]
		scenario_production_capacities[pu] 	= [float(i) for i in data[index_of_production_quantities+pu+number_of_production_units].split("\t")]
		
	for dp in range(number_of_dps):
		scenarios[dp] = [[] for j in range(number_of_trading_stages)]
		for i in range(number_of_trading_stages):
			#print(data[index_of_scenarios+i*number_of_dps+dp].split("\t"))
			scenarios[dp][i] = [float(j) for j in data[index_of_scenarios+i*number_of_dps+dp].split("\t")]
	
	allow_overflow = True if str(data[index_of_overflow]).strip() == "1" else False
	print("Time: " + str(int(10*time.time()-10*start)/10) + " seconds")

	return number_of_production_units, volume_bounds, min_q, max_q, scenario_probabilities, scenarios, transaction_cost, production_cost, production_capacities, cpr, number_of_dps, initial_storage, storage_bounds, scenario_inflows, scenario_production_capacities, scenario_production_costs, bms, bm_upper_bound, allow_overflow



def test_parameters():
	if(sum(scenario_probabilities) < 0.9999):
		#print(scenario_probabilities)
		raise ValueError('Sum of probabilities not equal 1 but' + str(sum(scenario_probabilities)))


number_of_production_units, volume_bounds, min_q, max_q, scenario_probabilities, scenarios, transaction_cost, production_cost, production_capacities, cpr, number_of_dps, initial_storage, storage_bounds, scenario_inflows, scenario_production_capacities, scenario_production_costs, bms, bm_ub, allow_overflow = read_parameters(parameter_file)

test_parameters()

### ----------- Variables -----------
bid_volume 								= [[[0 for j in range(number_of_trading_stages)] for i in range(len(scenario_probabilities))] for dp in range(number_of_dps)]
transaction_prices 						= [[[[0 for k in range(number_of_trading_stages)] for j in range(number_of_trading_stages)] for i in range(len(scenario_probabilities))] for dp in range(number_of_dps)]   # Average transaction price
transaction_volumes 					= [[[[0 for k in range(number_of_trading_stages)] for j in range(number_of_trading_stages)] for i in range(len(scenario_probabilities))] for dp in range(number_of_dps)]   # Total transaction volume
production_quantities 					= [[[model.addVar(vtype=GRB.CONTINUOUS,lb=min_q,ub=max_q, name="production_quantity_X_"+str(dp)+"_"+str(s)+"_"+str(j)) for j in range(number_of_production_units)] for s in range(len(scenario_probabilities))] for dp in range(number_of_dps)]    # Production quantity
storage_volume 							= [[model.addVar(name="storage_volume_X_X_"+str(dp)+"_"+str(s),vtype=GRB.CONTINUOUS, lb=0.0) for s in range(len(scenario_probabilities))] for dp in range(number_of_dps)]
imbalance_volume						= [[model.addVar(name="imbalance_volume_X_X_"+str(dp)+"_"+str(s)) for s in range(len(scenario_probabilities))] for dp in range(number_of_dps)]

if(allow_overflow):
	overflow	 						= [[model.addVar(name="overflow_X_X_"+str(dp)+"_"+str(s),vtype=GRB.CONTINUOUS, lb=0.0) for s in range(len(scenario_probabilities))] for dp in range(number_of_dps)]

for dp in range(number_of_dps):
	for s in range(len(scenario_probabilities)):
		for i in range(number_of_trading_stages):
			bid_volume[dp][s][i] = model.addVar(vtype=GRB.CONTINUOUS, lb=volume_bounds[0], ub=volume_bounds[1],		# Bid volume
		                                    name=str("b_v_X_"+str(dp)+"_"+str(s)+"_"+str(i)))
for dp in range(number_of_dps):
	for s in range(len(scenario_probabilities)):
		for i in range(number_of_trading_stages):
			for j in range(number_of_trading_stages):
				transaction_prices[dp][s][i][j] = model.addVar(vtype=GRB.CONTINUOUS,		                        # Transaction price
		                                    name=str("p_c_"+str(str(dp)+"_"+str(s)+"_"+str(i)+"_"+str(j))))

for dp in range(number_of_dps):
	for s in range(len(scenario_probabilities)):
		for i in range(number_of_trading_stages):
			for j in range(number_of_trading_stages):
				transaction_volumes[dp][s][i][j] = model.addVar(vtype=GRB.CONTINUOUS, lb=volume_bounds[0], ub=volume_bounds[1],	                # Transaction volume
		                                    name=str("v_c_"+str(str(dp)+"_"+str(s)+"_"+str(i)+"_"+str(j))))

### ----------- Constraints -----------

for s in range(len(scenario_probabilities)):
	if(allow_overflow):
		model.addConstr(storage_volume[0][s] == initial_storage + scenario_inflows[0][s] - sum(production_quantities[0][s]) - overflow[0][s], "storage_propagation_0")
	else:
		model.addConstr(storage_volume[0][s] == initial_storage + scenario_inflows[0][s]- sum(production_quantities[0][s]), "storage_propagation_0")
for dp in range(1, number_of_dps):
	for s in range(len(scenario_probabilities)):
		if(allow_overflow):
			model.addConstr(storage_volume[dp][s] == storage_volume[dp-1][s] + scenario_inflows[dp][s] - sum(production_quantities[dp][s]) - overflow[dp][s], "storage_propagation")
		else:
			model.addConstr(storage_volume[dp][s] == storage_volume[dp-1][s] + scenario_inflows[dp][s] - sum(production_quantities[dp][s]), "storage_propagation")
		
for dp in range(number_of_dps):
	for s in range(len(scenario_probabilities)):
		model.addConstr(storage_volume[dp][s] <= storage_bounds[1], "storage_bounds_up")
		model.addConstr(storage_volume[dp][s] >= storage_bounds[0], "storage_bounds_down")
		
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
		model.addConstr(sum(production_quantities[dp][s]) <= max_q, "max_q")
		model.addConstr(sum(production_quantities[dp][s]) >= min_q, "max_q")

for dp in range(number_of_dps):
	for s in range(len(scenario_probabilities)):
		for i in range(number_of_production_units):
			#print(s,i,production_capacities[i])
			model.addConstr((production_quantities[dp][s][i]) <= scenario_production_capacities[i][s], "production_capacities")

for dp in range(number_of_dps):
	for s in range(len(scenario_probabilities)):
		for i in range(number_of_trading_stages):
			#print(transaction_volumes[s][i][j] for j in range(0, i))
			model.addConstr(sum(transaction_volumes[dp][s][i][j] for j in range(0, i)) == 0, "no_transaction_before_placement")

for dp in range(number_of_dps):
	for s in range(len(scenario_probabilities)-1):	
		model.addConstr(bid_volume[dp][s][0] == bid_volume[dp][s+1][0], "non_anticipativity")


for dp in range(number_of_dps):
	for s in range(len(scenario_probabilities)):
		model.addConstr(imbalance_volume[dp][s] <= bm_ub, "imbalance_upper_bound")

print("SV", len(storage_volume), len(storage_volume[0]))
print("SI", len(scenario_inflows), len(scenario_inflows[0]))
print("PQ", "DP", "S","PU")
print("PQ", len(production_quantities), len(production_quantities[0]), len(production_quantities[0][0]))
print("OKMASDOM", scenarios)

### ----------- Objective Function -----------

model.setObjective((quicksum(scenario_probabilities[s]*transaction_volumes[dp][s][i][j] * scenarios[dp][i][s] for j in range(number_of_trading_stages) for i in range(number_of_trading_stages) for s in range(len(scenario_probabilities)) for dp in range(number_of_dps)) - sum(scenario_production_costs[x][s]*scenario_probabilities[s] * production_quantities[dp][s][x] for x in range(number_of_production_units) for s in range(len(scenario_probabilities)) for dp in range(number_of_dps)) - transaction_cost*sum(bid_volume[dp][s][i] for i in range(len(bid_volume[dp][s])) for s in range(len(scenario_probabilities)) for dp in range(number_of_dps)) - sum(scenario_probabilities[s] * sum(bms[dp][s] * imbalance_volume[dp][s] for dp in range(number_of_dps)) for s in range(len(scenario_probabilities)))), GRB.MAXIMIZE)


### ----------- Optimization -----------

model.optimize()


### ----------- Output Results -----------

if(printing_mode):
	for v in model.getVars():
	    
	    print("%s %f" % (v.Varname, v.X))


	for dp in range(number_of_dps):
		for s in range(len(scenario_probabilities)):
			print("Inflow_X_X_X_"+str(dp)+"_"+str(s)+ " " + str(scenario_inflows[dp][s]))

	model.write("Output/hydro_multiasset_multiproduct_continuous_stochastic_MC_output.sol")

### ----------- Support Functions -----------
def write_variables_to_file(file_name):
	book = xlsxwriter.Workbook(file_name)
	sheet = book.add_worksheet("Variables")

	# Fill spreadsheet
	counter = 0
	for v in model.getVars():
		sheet.write(counter, 0, str(v.Varname) + " " + str(v.X))
		counter += 1
	for dp in range(number_of_dps):
		for s in range(len(scenario_probabilities)):
			sheet.write(counter, 0, "Inflow_X_X_X_"+str(dp)+"_"+str(s)+ " " + str(scenario_inflows[dp][s]))
			counter += 1
	book.close()

write_variables_to_file(variables_file_name)