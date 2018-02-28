# Status: 
# Description: Initial implementation of the multiasset hydropower producer's trading problem
 
### ----------- External Packages -----------
from gurobipy import *
import time
import numpy
import xlsxwriter
import csv


### ----------- Model Initialization -----------
model 									= Model('hydro')


### ----------- System Parameters -----------
print_output 							= True
generate_scenarios 						= True
parameter_file 							= "Data/hydro_multiasset_multiproduct_continuous_stochastic_MC_parameters_bm_OD.txt"
scenario_generation_file_name			= "Data/scenario_generation_input.txt"
variables_file_name						= "variables_bm.xlsx"
random_variable_file_name				= "random_variables.csv"
printing_mode							= False
generate_random_variables				= True


### ----------- Model Parameters -----------
if (generate_scenarios == True):
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
	parameter_file 						= "Data/hydro_multiasset_multiproduct_continuous_stochastic_MC_parameters_bm_OD.txt"
	variables_file_name					= "variables_bm.xlsx"

	print("\nGenerating scenarios from parameter file...")
	start = time.time()

  	
  
  	

else:
	# Retrieve parameters from file
	params 								= itphelper.read_parameters(parameter_file)

### ----------- Support functions ------------
def fetch_data(parameter_file):
	print("\nReading data...")
	start = time.time()
	# File handling
	with open(parameter_file) as f:
	    data = f.readlines()
	return data

def scenario_generation(parameter_file):
	# Fetch data
	data 								= fetch_data(parameter_file)



	### How the input file should be read
	# Physical entities
	index_of_model_params 				= 0
	index_of_allow_overflow 			= 1
	index_of_production_bounds 			= 3
	index_of_inflow_bounds 				= index_of_production_bounds + 1
	index_of_production_cost_bounds 	= index_of_inflow_bounds + 1
	index_of_production_capacity_bounds = index_of_production_cost_bounds + 1
	index_of_storage_bounds 			= index_of_production_capacity_bounds + 1

	# Financial entities
	index_of_bid_volume_bounds 			= index_of_storage_bounds + 2
	print(index_of_bid_volume_bounds)
	index_of_volume_level_bounds 		= index_of_bid_volume_bounds + 1
	index_of_price_level_bounds 		= index_of_volume_level_bounds + 1
	index_of_bm_multiplier 				= index_of_price_level_bounds + 1
	index_of_transaction_costs			= index_of_bm_multiplier + 1

	### Fetch entities from file
	temp_list 							= list(data[index_of_model_params].strip().split("\t"))
	print(data[index_of_production_bounds])
	number_of_dps 						= int(temp_list[0])
	number_of_production_units 			= int(temp_list[1])
	number_of_trading_stages 			= int(temp_list[2])
	number_of_scenarios 				= int(temp_list[3])
	bm_upper_bound						= int(temp_list[4])
	number_of_price_levels				= int(temp_list[5])
	total_production_lower_bound		= float(data[index_of_production_bounds].split("\t")[0])
	total_production_upper_bound		= float(data[index_of_production_bounds].split("\t")[1])
	inflow_lower_bound					= float(data[index_of_inflow_bounds].split("\t")[0])
	inflow_upper_bound					= float(data[index_of_inflow_bounds].split("\t")[1])
	production_cost_lower_bound			= float(data[index_of_production_cost_bounds].split("\t")[0])
	production_cost_upper_bound			= float(data[index_of_production_cost_bounds].split("\t")[1])
	production_capacity_lower_bound		= float(data[index_of_production_capacity_bounds].split("\t")[0])
	production_capacity_upper_bound		= float(data[index_of_production_capacity_bounds].split("\t")[1])
	initial_storage 					= float(data[index_of_storage_bounds].split("\t")[2])
	storage_bounds 						= [float(data[index_of_storage_bounds].split("\t")[i]) for i in range(2)]
	print(data[index_of_bid_volume_bounds].split("\t")[0])
	bid_volume_bounds 					= [float(data[index_of_bid_volume_bounds].split("\t")[i]) for i in range(2)]
	price_level_bounds 					= [float(data[index_of_price_level_bounds].split("\t")[i]) for i in range(2)]
	volume_level_bounds 				= [float(data[index_of_volume_level_bounds].split("\t")[i]) for i in range(2)]

	### Generate random variables
	if (generate_random_variables == True):
		random_variables 				= [[numpy.random.normal(0, 0.1) for k in range(5)] for i in range(number_of_scenarios)]
		write_random_variables_to_file(random_variables)
	else:
		random_variables_raw 			= fetch_data(random_variable_file_name)
		random_variables 				= [[float(i) for i in random_variables_raw[x].strip().split(",")] for x in range(len(random_variables_raw))]

	random_variable_counter 			= 0
	
	### Initialize variables and datastructures
	scenario_probabilities 				= [1.0/number_of_scenarios for i in range(number_of_scenarios)]
	scenario_inflows 					= [[[] for s in range(number_of_scenarios)] for j in range(number_of_dps)]
	production_cost 					= [float(i) for i in data[index_of_production_costs].strip().split("\t")]
	production_capacities 				= [float(i) for i in data[index_of_production_quantities].strip().split("\t")]
	price_levels 						= [[[[] for p in range(number_of_price_levels)] for t in range(number_of_trading_stages)] for dp in range(number_of_dps)]
	volume_levels 						= [[[[] for p in range(number_of_price_levels)] for t in range(number_of_trading_stages)] for dp in range(number_of_dps)]
	bms 								= [[[] for s in range(number_of_scenarios)] for x in range(number_of_dps)]

	### Instantiate variables and datastructures
	allow_overflow 						= True if str(data[index_of_allow_overflow]).strip() == "1" else False
	min_q, max_q 						= float(data[index_of_production_bounds][0]), float(data[index_of_production_bounds][1])
	bm_multiplier 						= float(data[index_of_bm_multiplier])
	transaction_cost 					= float(data[index_of_transaction_costs])


	for dp in range(number_of_dps):
		for t in range(number_of_trading_stages):
			for p in range(number_of_price_levels):
				# Price levels: declining with p
				if(p == 0):
					price_levels[dp][t][p] 	= [price_level_bounds[0] + (price_level_bounds[1] - price_level_bounds[0]) * random_variables[s][0] for s in range(number_of_scenarios)]
				
				else:
					price_levels[dp][t][p] 	= [price_level_bounds[0] + (price_levels[dp][t][p-1] - price_level_bounds[0]) * random_variables[s][0] for s in range(number_of_scenarios)]

				# Volume levels: no ordering
				volume_levels[dp][t][p]		= [volume_levels[0] + (volume_levels[1] - volume_levels[0]) * random_variables[s][1] for s in range(number_of_scenarios)]

		for s in range(number_of_scenarios):
			# Inflows
			scenario_inflows[dp][s] 		= inflow_lower_bound + (inflow_upper_bound - inflow_lower_bound) * random_variables[s][2]

			# BMS
			bms[dp][s] 						= bm_multiplier * sum(price_level_bounds[dp][t][p][s] * volume_level_bounds[dp][t][p][s] for t in range(number_of_trading_stages) for p in range(number_of_price_levels)) / sum(volume_level_bounds[dp][t][p][s] for t in range(number_of_trading_stages) for p in range(number_of_price_levels))

			# Production cost
			production_cost[dp][s] 			= production_cost_lower_bound + (production_cost_upper_bound - production_cost_lower_bound) * random_variables[s][3]

			# Production capacities
			production_capacity[dp][s] 			= production_capacity_lower_bound + (production_capacity_upper_bound - production_capacity_lower_bound) * random_variables[s][4]


scenario_generation(scenario_generation_file_name)


def read_parameters(parameter_file):
	# Fetch data
	data = fetch_data(parameter_file)

	# Read initial data from file to calculate indices correctly
	volume_bounds 						= [float(i) for i in data[index_of_actions].strip().split("\t")]
	q_array 							= (list(data[index_of_q_bounds].strip().split("\t")))
	min_q, max_q 						= float(q_array[0]), float(q_array[1])
	temp_list 							= list(data[2].strip().split("\t"))
	number_of_dps 						= int(temp_list[0])
	number_of_production_units 			= int(temp_list[1])
	number_of_scenarios 				= int(temp_list[2])
	bm_upper_bound						= int(temp_list[3])
	number_of_price_levels				= int(temp_list[4])

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
	index_of_volume_levels				= index_of_overflow + 2
	index_of_price_levels 				= index_of_volume_levels + 1 + number_of_dps*number_of_trading_stages*number_of_price_levels

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
	price_levels 						= [[[[] for p in range(number_of_price_levels)] for t in range(number_of_trading_stages)] for dp in range(number_of_dps)]
	volume_levels 						= [[[[] for p in range(number_of_price_levels)] for t in range(number_of_trading_stages)] for dp in range(number_of_dps)]

	for pu in range(number_of_production_units):
		scenario_production_costs[pu] 		= [float(i) for i in data[index_of_production_quantities+pu].split("\t")]
		scenario_production_capacities[pu] 	= [float(i) for i in data[index_of_production_quantities+pu+number_of_production_units].split("\t")]
		
	for dp in range(number_of_dps):
		scenarios[dp] = [[] for j in range(number_of_trading_stages)]
		for i in range(number_of_trading_stages):
			#print(data[index_of_scenarios+i*number_of_dps+dp].split("\t"))
			scenarios[dp][i] = [float(j) for j in data[index_of_scenarios+i*number_of_dps+dp].split("\t")]
	
	for dp in range(number_of_dps):
		for t in range(number_of_trading_stages):
			for p in range(number_of_price_levels):
				volume_levels[dp][t][p] 	= [float(i) for i in data[index_of_volume_levels+dp*(number_of_trading_stages+number_of_price_levels)+t*number_of_price_levels+p].strip().split("\t")]
				price_levels[dp][t][p] 		= [float(i) for i in data[index_of_price_levels+dp*(number_of_trading_stages+number_of_price_levels)+t*number_of_price_levels+p].strip().split("\t")]
	

	allow_overflow = True if str(data[index_of_overflow]).strip() == "1" else False


	print("Time: " + str(int(10*time.time()-10*start)/10) + " seconds")

	return number_of_production_units, volume_bounds, min_q, max_q, scenario_probabilities, scenarios, transaction_cost, production_cost, production_capacities, cpr, number_of_dps, initial_storage, storage_bounds, scenario_inflows, scenario_production_capacities, scenario_production_costs, bms, bm_upper_bound, allow_overflow, price_levels, volume_levels, number_of_price_levels

def test_parameters():
	if(sum(scenario_probabilities) < 0.9999):
		#print(scenario_probabilities)
		raise ValueError('Sum of probabilities not equal 1 but' + str(sum(scenario_probabilities)))


number_of_production_units, volume_bounds, min_q, max_q, scenario_probabilities, scenarios, transaction_cost, production_cost, production_capacities, cpr, number_of_dps, initial_storage, storage_bounds, scenario_inflows, scenario_production_capacities, scenario_production_costs, bms, bm_ub, allow_overflow,price_levels, volume_levels,number_of_price_levels = read_parameters(parameter_file)

test_parameters()

### ----------- Variables -----------
# The following variables are created immediately:
production_quantities 					= [[[model.addVar(vtype=GRB.CONTINUOUS,lb=min_q,ub=max_q, name="production_quantity_X_X_"+str(dp)+"_"+str(s)+"_"+str(j)) for j in range(number_of_production_units)] for s in range(len(scenario_probabilities))] for dp in range(number_of_dps)]    # Production quantity
storage_volume 							= [[model.addVar(name="storage_volume_X_X_X_"+str(dp)+"_"+str(s),vtype=GRB.CONTINUOUS, lb=0.0) for s in range(len(scenario_probabilities))] for dp in range(number_of_dps)]
imbalance_volume						= [[model.addVar(name="imbalance_volume_X_X_X_"+str(dp)+"_"+str(s)) for s in range(len(scenario_probabilities))] for dp in range(number_of_dps)]

if(allow_overflow):
	overflow	 						= [[model.addVar(name="overflow_X_X_"+str(dp)+"_"+str(s),vtype=GRB.CONTINUOUS, lb=0.0) for s in range(len(scenario_probabilities))] for dp in range(number_of_dps)]

# The rest are created in the following loops:
bid_volume 								= [[[[0 for p in range(number_of_price_levels)] for j in range(number_of_trading_stages)] for i in range(len(scenario_probabilities))] for dp in range(number_of_dps)]
transaction_prices 						= [[[[[0 for p in range(number_of_price_levels)] for k in range(number_of_trading_stages)] for j in range(number_of_trading_stages)] for i in range(len(scenario_probabilities))] for dp in range(number_of_dps)]   # Average transaction price
transaction_volumes 					= [[[[[0 for p in range(number_of_price_levels)] for k in range(number_of_trading_stages)] for j in range(number_of_trading_stages)] for i in range(len(scenario_probabilities))] for dp in range(number_of_dps)]   # Total transaction volume

for dp in range(number_of_dps):
	for s in range(len(scenario_probabilities)):
		for i in range(number_of_trading_stages):
			for p in range(number_of_price_levels):
				bid_volume[dp][s][i][p] 				= model.addVar(vtype=GRB.CONTINUOUS, lb=volume_bounds[0], ub=volume_bounds[1],		# Bid volume
			                                    name=str("b_v_X_"+str(dp)+"_"+str(s)+"_"+str(i)+"_"+str(p)))
			for j in range(number_of_trading_stages):
				for p in range(number_of_price_levels):
					transaction_prices[dp][s][i][j][p] 	= model.addVar(vtype=GRB.CONTINUOUS,		                        				# Transaction price
			                                    name=str("p_c_"+str(str(dp)+"_"+str(s)+"_"+str(i)+"_"+str(j)+"_"+str(p))))
					
					transaction_volumes[dp][s][i][j][p] = model.addVar(vtype=GRB.CONTINUOUS, lb=volume_bounds[0], ub=volume_bounds[1],		# Transaction volume
			                                    name=str("v_c_"+str(str(dp)+"_"+str(s)+"_"+str(i)+"_"+str(j)+"_"+str(p))))

### ----------- Constraints -----------

### Physical constraints 

# Flow conservation of storage
for s in range(len(scenario_probabilities)):
	if(allow_overflow):
		model.addConstr(storage_volume[0][s] 		== initial_storage + scenario_inflows[0][s] - sum(production_quantities[0][s]) - overflow[0][s], "storage_propagation_0")
	else:
		model.addConstr(storage_volume[0][s] 		== initial_storage + scenario_inflows[0][s]- sum(production_quantities[0][s]), "storage_propagation_0")

for dp in range(1, number_of_dps):
	for s in range(len(scenario_probabilities)):
		if(allow_overflow):
			model.addConstr(storage_volume[dp][s] 	== storage_volume[dp-1][s] + scenario_inflows[dp][s] - sum(production_quantities[dp][s]) - overflow[dp][s], "storage_propagation")
		else:
			model.addConstr(storage_volume[dp][s] 	== storage_volume[dp-1][s] + scenario_inflows[dp][s] - sum(production_quantities[dp][s]), "storage_propagation")
		
# Storage within bounds
for dp in range(number_of_dps):
	for s in range(len(scenario_probabilities)):
		model.addConstr(storage_volume[dp][s] 		<= storage_bounds[1], "storage_bounds_up")
		model.addConstr(storage_volume[dp][s] 		>= storage_bounds[0], "storage_bounds_down")
		
# Global production within bounds
for dp in range(number_of_dps):
	for s in range(len(scenario_probabilities)):
		model.addConstr(sum(production_quantities[dp][s]) 		<= max_q, "max_q")
		model.addConstr(sum(production_quantities[dp][s]) 		>= min_q, "max_q")

# Actual production per asset less than production capacity
for dp in range(number_of_dps):
	for s in range(len(scenario_probabilities)):
		for i in range(number_of_production_units):
			#print(s,i,production_capacities[i])
			model.addConstr((production_quantities[dp][s][i]) 	<= scenario_production_capacities[i][s], "production_capacities")

# Production equal to transaction volumes ++
for dp in range(number_of_dps):
	for s in range(len(scenario_probabilities)):
		model.addConstr((sum(production_quantities[dp][s][x] for x in range(len(production_quantities[dp][s]))) - sum(transaction_volumes[dp][s][i][j][p] for p in range(number_of_price_levels) for j in range(number_of_trading_stages) for i in range(number_of_trading_stages)) - imbalance_volume[dp][s] == 0), "q=v_c_"+str(s))

# Transaction volumes equal bid volume
for dp in range(number_of_dps):
	for s in range(len(scenario_probabilities)):
		for i in range(number_of_trading_stages):
			model.addConstr((sum(transaction_volumes[dp][s][i][i][p] for p in range(number_of_price_levels)) == bid_volume[dp][s][i][p]), "v_c<=b_v " + str(s) +" "+ str(i))

# No transaction except immediately
for dp in range(number_of_dps):
	for s in range(len(scenario_probabilities)):
		for i in range(number_of_trading_stages):
			for j in range(number_of_trading_stages):
				for p in range(number_of_price_levels):
					if(i != j):
						model.addConstr((transaction_volumes[dp][s][i][j][p] == 0), "no transaction except at t=t0 " + str(s) +" "+ str(i))

# No transaction before placement
for dp in range(number_of_dps):
	for s in range(len(scenario_probabilities)):
		for i in range(number_of_trading_stages):
			for p in range(number_of_price_levels):
				model.addConstr(sum(transaction_volumes[dp][s][i][j][p] for j in range(0, i)) == 0, "no_transaction_before_placement")

# Transaction volumes less than volume levels
for dp in range(number_of_dps):
	for s in range(len(scenario_probabilities)):
		for i in range(number_of_trading_stages):
			for p in range(number_of_price_levels):
				model.addConstr(transaction_volumes[dp][s][i][i][p] <= volume_levels[dp][i][p][s], "v_c <= d_p")

# Limit imbalance volume
for dp in range(number_of_dps):
	for s in range(len(scenario_probabilities)):
		model.addConstr(imbalance_volume[dp][s] <= bm_ub, "imbalance_upper_bound")

# Non-anticipativity
for dp in range(number_of_dps):
	for s in range(len(scenario_probabilities)-1):	
		model.addConstr(bid_volume[dp][s][0] == bid_volume[dp][s+1][0], "non_anticipativity")


### ----------- Objective Function -----------

# The objective function can be divided into:
# - Transaction revenues
# - Production costs
# - Transaction costs
# - Imbalance costs

model.setObjective((quicksum(scenario_probabilities[s]*transaction_volumes[dp][s][i][j][p] * price_levels[dp][i][p][s] for p in range(number_of_price_levels) for j in range(number_of_trading_stages) for i in range(number_of_trading_stages) for s in range(len(scenario_probabilities)) for dp in range(number_of_dps))) 
	- sum(scenario_production_costs[x][s]*scenario_probabilities[s] * production_quantities[dp][s][x] for x in range(number_of_production_units) for s in range(len(scenario_probabilities)) for dp in range(number_of_dps)) 
	- transaction_cost*sum(bid_volume[dp][s][i][p] for p in range(number_of_price_levels) for i in range(number_of_trading_stages) for s in range(len(scenario_probabilities)) for dp in range(number_of_dps))
	- sum(scenario_probabilities[s] * sum(bms[dp][s] * imbalance_volume[dp][s] for dp in range(number_of_dps)) for s in range(len(scenario_probabilities))), GRB.MAXIMIZE)

### ----------- Optimization -----------

model.optimize()


### ----------- Output Results -----------

if(printing_mode):
	for v in model.getVars():
	    print("%s %f" % (v.Varname, v.X))

	for dp in range(number_of_dps):
		for s in range(len(scenario_probabilities)):
			print("Inflow_X_X_X_X_"+str(dp)+"_"+str(s)+ " " + str(scenario_inflows[dp][s]))

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
			sheet.write(counter, 0, "Inflow_X_X_X_X_"+str(dp)+"_"+str(s)+ " " + str(scenario_inflows[dp][s]))
			counter += 1
	book.close()

def write_matrix_to_file(matrix):
	with open('random_variables.csv', 'w', newline='') as csvfile:
	    csv_writer = csv.writer(csvfile, delimiter=' ',
	                            quotechar='|', quoting=csv.QUOTE_MINIMAL)
	    for r in matrix:
	    	csv_writer.writerow(r)
	    

write_variables_to_file(variables_file_name)






