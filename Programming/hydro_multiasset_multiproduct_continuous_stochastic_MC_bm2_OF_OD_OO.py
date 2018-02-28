# Status: 
# Description: Initial implementation of the multiasset hydropower producer's trading problem
 
### ----------- External Packages -----------
from gurobipy import *
import time
import numpy
import xlsxwriter
import csv

class ITP_Solver(object):
	"""docstring for Gurobi_Controller"""
	def __init__(self, generate_scenarios=False, generate_random_variables=False):
		super(ITP_Solver, self).__init__()


		### ----------- Model Initialization -----------
		self.model 									= Model('itp')
		self.model.setParam('OutputFlag', 0)
		self.model.params.threads = 1 
		self.model.modelSense = GRB.MAXIMIZE
		self.model.update()

		### ----------- Set System Parameters -----------
		self.generate_scenarios 					= generate_scenarios
		self.parameter_file_name 					= "Data/hydro_multiasset_multiproduct_continuous_stochastic_MC_parameters_bm_OD.txt"
		self.scenario_generation_file_name			= "Data/scenario_generation_input.txt"
		self.variables_file_name					= "variables_bm.xlsx"
		self.random_variable_file_name				= "random_variables.csv"
		self.printing_output						= False
		self.printing_mode							= True
		self.generate_random_variables				= generate_random_variables
		self.start 									= time.time()


		### ----------- Set Model Parameters -----------
		if(generate_scenarios == True):
			if(self.printing_mode):
				print("\nGenerating scenarios from parameter file...")
			self.start = time.time()
			self.scenario_generation(self.scenario_generation_file_name)
			if(self.printing_mode):
				print("Scenarios generated. Elapsed time: " + str(float(int(10*(time.time()-self.start)))/10.0) + " seconds.")
		else:
			# Set self.model parameters
			params 									= 0
			self.number_of_trading_stages 			= 2 # To do: This might not be updated properly if not autogenerating scenarios
			self.number_of_production_units 		= 0
			self.number_of_dps 						= 0
			self.min_q 								= 0
			self.max_q 								= 0
			cpr 									= 0
			self.initial_storage 					= 0
			self.min_q 								= 0
			self.max_q 								= 0
			self.transaction_cost 					= 0
			production_cost 						= 0

			trading_stages 							= [x for x in range(self.number_of_trading_stages)]
			volume_options 							= []
			self.scenario_probabilities 			= []
			scenarios 								= []
			self.scenario_inflows 					= []
			self.scenario_production_costs			= []
			self.scenario_production_capacities		= []
			self.transaction_cost 					= []
			production_cost 						= []
			production_capacities 					= []
			self.storage_bounds 					= []
			scenarios 								= []
			volume_options 							= []

			# Indices
			self.index_of_actions 					= 0								# If necessary, modify this
			self.index_of_q_bounds 					= self.index_of_actions + 1
			self.index_of_dps 						= self.index_of_q_bounds + 1
			self.index_of_scenario_probabilities 	= 4								# If necessary, modify this

			

			# Read parameters in separate function
			self.read_parameters(self.parameter_file_name)

		# Test given parameters
		self.validate_parameters()

	### ----------- Support Functions ------------
	def fetch_data(self, parameter_file_name):
		if(not(self.generate_scenarios) and (self.printing_mode)):
			print("\nReading data...")
		self.start = time.time()
		# File handling
		with open(parameter_file_name) as f:
		    self.data = f.readlines()
		if(not(self.generate_scenarios) and (self.printing_mode)):
			print("Done reading data.")
		return self.data

	def reset_parameters(self, dps, scenarios, production_units, trading_stages):
		print("Parameters: ", dps, scenarios, production_units, trading_stages)
		self.number_of_dps = dps
		self.number_of_scenarios = scenarios
		self.number_of_production_units = production_units
		self.number_of_trading_stages = trading_stages

		# Redo datastructure instantiation
		self.instantiate_datastructures()

	### ----------- Generate scenarios based on self.data in file ----------- 
	def scenario_generation(self, parameter_file_name):
		# Fetch self.data
		self.data 									= self.fetch_data(parameter_file_name)

		### How the input file should be read
		# Physical entities
		self.index_of_model_params 					= 0
		self.index_of_allow_overflow 				= 1
		self.index_of_production_bounds 			= 3
		self.index_of_inflow_bounds 				= self.index_of_production_bounds + 1
		self.index_of_production_cost_bounds 		= self.index_of_inflow_bounds + 1
		self.index_of_production_capacity_bounds 	= self.index_of_production_cost_bounds + 1
		self.index_of_storage_bounds 				= self.index_of_production_capacity_bounds + 1

		# Financial entities
		self.index_of_bid_volume_bounds 			= self.index_of_storage_bounds + 2
		self.index_of_volume_level_bounds 			= self.index_of_bid_volume_bounds + 1
		self.index_of_price_level_bounds	 		= self.index_of_volume_level_bounds + 1
		self.index_of_bm_multiplier 				= self.index_of_price_level_bounds + 1
		self.index_of_transaction_costs				= self.index_of_bm_multiplier + 1

		### Fetch entities from file
		temp_list 									= (self.data[self.index_of_model_params].strip().split("\t"))
		self.number_of_dps 							= int(temp_list[0])
		self.number_of_production_units				= int(temp_list[1])
		self.number_of_trading_stages 				= int(temp_list[2])
		self.number_of_scenarios 					= int(temp_list[3])
		self.bm_upper_bound							= int(temp_list[4])
		self.number_of_price_levels					= int(temp_list[5])
		total_production_lower_bound				= float(self.data[self.index_of_production_bounds].split("\t")[0])
		total_production_upper_bound				= float(self.data[self.index_of_production_bounds].split("\t")[1])
		self.inflow_lower_bound						= float(self.data[self.index_of_inflow_bounds].split("\t")[0])
		self.inflow_upper_bound						= float(self.data[self.index_of_inflow_bounds].split("\t")[1])
		self.production_cost_lower_bound			= float(self.data[self.index_of_production_cost_bounds].split("\t")[0])
		self.production_cost_upper_bound			= float(self.data[self.index_of_production_cost_bounds].split("\t")[1])
		self.production_capacity_lower_bound		= float(self.data[self.index_of_production_capacity_bounds].split("\t")[0])
		self.production_capacity_upper_bound		= float(self.data[self.index_of_production_capacity_bounds].split("\t")[1])
		self.initial_storage 						= float(self.data[self.index_of_storage_bounds].split("\t")[2])
		self.storage_bounds 						= [float(self.data[self.index_of_storage_bounds].split("\t")[i]) for i in range(2)]
		self.volume_bounds 							= [float(self.data[self.index_of_bid_volume_bounds].split("\t")[i]) for i in range(2)]
		self.price_level_bounds 					= [float(self.data[self.index_of_price_level_bounds].split("\t")[i]) for i in range(2)]
		self.volume_level_bounds 					= [float(self.data[self.index_of_volume_level_bounds].split("\t")[i]) for i in range(2)]

		self.instantiate_datastructures()
		self.setup_non_anticipativity_sets()
	
	def setup_non_anticipativity_sets(self):
		self.non_anticipativity_sets				= [{} for t in range(number_of_trading_stages)]
		for t in range(number_of_trading_stages):
			for s in range(number_of_scenarios):
				if(False): # if all scenario specific entities equal to those of existing non_anticipativity_set):
					self.non_anticipativity_sets[t][s].append(s)
				else:
					self.non_anticipativity_sets[t][s] = [s]

	def instantiate_datastructures(self):
		### Generate random variables
		if (self.generate_random_variables == True):
			random_variables 				= [[numpy.random.normal(0, 0.1) for k in range(200)] for i in range(self.number_of_scenarios)]
			write_matrix_to_file(random_variables, self.random_variable_file_name)

		else:
			random_variables_raw 			= self.fetch_data(self.random_variable_file_name)
			random_variables 				= [[float(i) for i in random_variables_raw[x].strip().split(" ")] for x in range(len(random_variables_raw))]

		random_variable_counter 			= 0
		
		### Initialize variables and datastructures
		self.scenario_probabilities 		= [1.0/self.number_of_scenarios for i in range(self.number_of_scenarios)]
		self.scenario_inflows 				= [[[] for s in range(self.number_of_scenarios)] for j in range(self.number_of_dps)]
		self.production_costs_bounds 		= [float(i) for i in self.data[self.index_of_production_cost_bounds].strip().split("\t")]
		self.production_capacities_bounds	= [float(i) for i in self.data[self.index_of_production_capacity_bounds].strip().split("\t")]
		self.price_levels 					= [[[[] for p in range(self.number_of_price_levels)] for t in range(self.number_of_trading_stages)] for dp in range(self.number_of_dps)]
		self.volume_levels 					= [[[[] for p in range(self.number_of_price_levels)] for t in range(self.number_of_trading_stages)] for dp in range(self.number_of_dps)]
		self.bms 							= [[[] for s in range(self.number_of_scenarios)] for x in range(self.number_of_dps)]
		self.scenario_production_costs		= [[0 for s in range(self.number_of_scenarios)] for p in range(self.number_of_production_units)]
		self.scenario_production_capacities	= [[0 for s in range(self.number_of_scenarios)] for p in range(self.number_of_production_units)] 

		### Instantiate variables and datastructures
		self.allow_overflow 				= True if str(self.data[self.index_of_allow_overflow]).strip() == "1" else False
		self.min_q, self.max_q 				= float(self.data[self.index_of_production_bounds].split("\t")[0]), float(self.data[self.index_of_production_bounds].split("\t")[1])
		self.bm_multiplier 					= float(self.data[self.index_of_bm_multiplier])
		self.transaction_cost 				= float(self.data[self.index_of_transaction_costs])


		for dp in range(self.number_of_dps):
			for t in range(self.number_of_trading_stages):
				for p in range(self.number_of_price_levels):
					# Price levels: declining with p
					if(p == 0):
						self.price_levels[dp][t][p] 		= [self.price_level_bounds[0] + (self.price_level_bounds[1] - self.price_level_bounds[0]) * random_variables[s][0] for s in range(self.number_of_scenarios)]
					
					else:
						self.price_levels[dp][t][p] 		= [self.price_level_bounds[0] + (self.price_levels[dp][t][p-1][s] - self.price_level_bounds[0]) * random_variables[s][0] for s in range(self.number_of_scenarios)]

					# Volume levels: no ordering
					self.volume_levels[dp][t][p]			= [self.volume_level_bounds[0] + (self.volume_level_bounds[1] - self.volume_level_bounds[0]) * random_variables[s][1] for s in range(self.number_of_scenarios)]

			for s in range(self.number_of_scenarios):
				# Inflows
				self.scenario_inflows[dp][s] 				= self.inflow_lower_bound + (self.inflow_upper_bound - self.inflow_lower_bound) * random_variables[s][2]

				# self.bms
				self.bms[dp][s] 							= self.bm_multiplier * sum(sum(self.price_levels[dp][t][p][s] * self.volume_levels[dp][t][p][s] for t in range(self.number_of_trading_stages)) for p in range(self.number_of_price_levels))  / sum(self.volume_levels[dp][t][p][s] for t in range(self.number_of_trading_stages) for p in range(self.number_of_price_levels))
		
		for pu in range(self.number_of_production_units):
			for s in range(self.number_of_scenarios):
				# Production cost
				self.scenario_production_costs[pu][s] 		= self.production_cost_lower_bound + (self.production_cost_upper_bound - self.production_cost_lower_bound) * random_variables[s][3+pu]

				# Production capacities
				self.scenario_production_capacities[pu][s]	= self.production_capacity_lower_bound + (self.production_capacity_upper_bound - self.production_capacity_lower_bound) * random_variables[s][3+pu+self.number_of_production_units]

	### ----------- Fetch scenarios from file ----------- 
	def read_parameters(self, parameter_file):
		
		for dp in range(self.number_of_dps):
			for t in range(self.number_of_trading_stages):
				for p in range(self.number_of_price_levels):
					self.volume_levels[dp][t][p] 	= [float(i) for i in self.data[self.index_of_volume_levels+dp*(self.number_of_trading_stages+self.number_of_price_levels)+t*self.number_of_price_levels+p].strip().split("\t")]
					self.price_levels[dp][t][p] 		= [float(i) for i in self.data[self.index_of_price_levels+dp*(self.number_of_trading_stages+self.number_of_price_levels)+t*self.number_of_price_levels+p].strip().split("\t")]
		

		self.allow_overflow = True if str(self.data[self.index_of_overflow]).strip() == "1" else False


		print("Time: " + str(int(10*time.time()-10*self.start)/10) + " seconds")
	
	### ----------- Validating The Given Parameters ----------- 
	def validate_parameters(self):
		if(sum(self.scenario_probabilities) < 0.9999):
			#print(self.scenario_probabilities)
			raise ValueError('Sum of probabilities not equal 1 but' + str(sum(self.scenario_probabilities)))

	### ----------- Variables -----------
	def setup_variables(self):
		# The following variables are created immediately:
		self.production_quantities 					= [[[self.model.addVar(vtype=GRB.CONTINUOUS,lb=self.min_q,ub=self.max_q, name="production_quantity_X_X_"+str(dp)+"_"+str(s)+"_"+str(j)) for j in range(self.number_of_production_units)] for s in range(self.number_of_scenarios)] for dp in range(self.number_of_dps)]    # Production quantity
		self.storage_volume 						= [[self.model.addVar(name="storage_volume_X_X_X_"+str(dp)+"_"+str(s),vtype=GRB.CONTINUOUS, lb=0.0) for s in range(self.number_of_scenarios)] for dp in range(self.number_of_dps)]
		self.imbalance_volume						= [[self.model.addVar(name="imbalance_volume_X_X_X_"+str(dp)+"_"+str(s)) for s in range(self.number_of_scenarios)] for dp in range(self.number_of_dps)]

		if(self.allow_overflow):
			overflow	 							= [[self.model.addVar(name="overflow_X_X_"+str(dp)+"_"+str(s),vtype=GRB.CONTINUOUS, lb=0.0) for s in range(self.number_of_scenarios)] for dp in range(self.number_of_dps)]

		# The rest are created in the following loops. This could probably have been omitted, but is unlikely to affect performance significantly.
		self.bid_volume 							= [[[[0 for p in range(self.number_of_price_levels)] for j in range(self.number_of_trading_stages)] for i in range(self.number_of_scenarios)] for dp in range(self.number_of_dps)]
		self.transaction_prices 					= [[[[[0 for p in range(self.number_of_price_levels)] for k in range(self.number_of_trading_stages)] for j in range(self.number_of_trading_stages)] for i in range(self.number_of_scenarios)] for dp in range(self.number_of_dps)]   # Average transaction price
		self.transaction_volumes 					= [[[[[0 for p in range(self.number_of_price_levels)] for k in range(self.number_of_trading_stages)] for j in range(self.number_of_trading_stages)] for i in range(self.number_of_scenarios)] for dp in range(self.number_of_dps)]   # Total transaction volume

		for dp in range(self.number_of_dps):
			for s in range(self.number_of_scenarios):
				for i in range(self.number_of_trading_stages):
					for p in range(self.number_of_price_levels):
						self.bid_volume[dp][s][i][p] 				= self.model.addVar(vtype=GRB.CONTINUOUS, lb=self.volume_bounds[0], ub=self.volume_bounds[1],		# Bid volume
					                                    name=str("b_v_X_"+str(dp)+"_"+str(s)+"_"+str(i)+"_"+str(p)))
					for j in range(self.number_of_trading_stages):
						for p in range(self.number_of_price_levels):
							self.transaction_prices[dp][s][i][j][p] 	= self.model.addVar(vtype=GRB.CONTINUOUS,		                        				# Transaction price
					                                    name=str("p_c_"+str(str(dp)+"_"+str(s)+"_"+str(i)+"_"+str(j)+"_"+str(p))))
							
							self.transaction_volumes[dp][s][i][j][p] = self.model.addVar(vtype=GRB.CONTINUOUS, lb=self.volume_bounds[0], ub=self.volume_bounds[1],		# Transaction volume
					                                    name=str("v_c_"+str(str(dp)+"_"+str(s)+"_"+str(i)+"_"+str(j)+"_"+str(p))))

	### ----------- Constraints -----------
	def setup_constraints(self):
		### Physical constraints 

		# Flow conservation of storage
		for s in range(self.number_of_scenarios):
			if(self.allow_overflow):
				self.model.addConstr(self.storage_volume[0][s] 		== self.initial_storage + self.scenario_inflows[0][s] - sum(self.production_quantities[0][s]) - overflow[0][s], "storage_propagation_0")
			else:
				self.model.addConstr(self.storage_volume[0][s] 		== self.initial_storage + self.scenario_inflows[0][s]- sum(self.production_quantities[0][s]), "storage_propagation_0")

		for dp in range(1, self.number_of_dps):
			for s in range(self.number_of_scenarios):
				if(self.allow_overflow):
					self.model.addConstr(self.storage_volume[dp][s] 	== self.storage_volume[dp-1][s] + self.scenario_inflows[dp][s] - sum(self.production_quantities[dp][s]) - overflow[dp][s], "storage_propagation")
				else:
					self.model.addConstr(self.storage_volume[dp][s] 	== self.storage_volume[dp-1][s] + self.scenario_inflows[dp][s] - sum(self.production_quantities[dp][s]), "storage_propagation")
				
		# Storage within bounds
		for dp in range(self.number_of_dps):
			for s in range(self.number_of_scenarios):
				self.model.addConstr(self.storage_volume[dp][s] 		<= self.storage_bounds[1], "storage_bounds_up")
				self.model.addConstr(self.storage_volume[dp][s] 		>= self.storage_bounds[0], "storage_bounds_down")
				
		# Global production within bounds
		for dp in range(self.number_of_dps):
			for s in range(self.number_of_scenarios):
				self.model.addConstr(sum(self.production_quantities[dp][s]) 		<= self.max_q, "max_q")
				self.model.addConstr(sum(self.production_quantities[dp][s]) 		>= self.min_q, "max_q")

		# Actual production per asset less than production capacity
		for dp in range(self.number_of_dps):
			for s in range(self.number_of_scenarios):
				for i in range(self.number_of_production_units):
					#print(s,i,production_capacities[i])
					self.model.addConstr((self.production_quantities[dp][s][i]) 	<= self.scenario_production_capacities[i][s], "production_capacities")

		# Production equal to transaction volumes ++
		for dp in range(self.number_of_dps):
			for s in range(self.number_of_scenarios):
				self.model.addConstr((sum(self.production_quantities[dp][s][x] for x in range(len(self.production_quantities[dp][s]))) - sum(self.transaction_volumes[dp][s][i][j][p] for p in range(self.number_of_price_levels) for j in range(self.number_of_trading_stages) for i in range(self.number_of_trading_stages)) - self.imbalance_volume[dp][s] == 0), "q=v_c_"+str(s))

		# Transaction volumes equal bid volume
		for dp in range(self.number_of_dps):
			for s in range(self.number_of_scenarios):
				for i in range(self.number_of_trading_stages):
					for p in range(self.number_of_price_levels):
						self.model.addConstr((self.transaction_volumes[dp][s][i][i][p] == self.bid_volume[dp][s][i][p]), "v_c<=b_v " + str(s) +" "+ str(i))

		# No transaction except immediately
		for dp in range(self.number_of_dps):
			for s in range(self.number_of_scenarios):
				for i in range(self.number_of_trading_stages):
					for j in range(self.number_of_trading_stages):
						for p in range(self.number_of_price_levels):
							if(i != j):
								self.model.addConstr((self.transaction_volumes[dp][s][i][j][p] == 0), "no transaction except at t=t0 " + str(s) +" "+ str(i))

		# No transaction before placement
		for dp in range(self.number_of_dps):
			for s in range(self.number_of_scenarios):
				for i in range(self.number_of_trading_stages):
					for p in range(self.number_of_price_levels):
						self.model.addConstr(sum(self.transaction_volumes[dp][s][i][j][p] for j in range(0, i)) == 0, "no_transaction_before_placement")

		# Transaction volumes less than volume levels
		for dp in range(self.number_of_dps):
			for s in range(self.number_of_scenarios):
				for i in range(self.number_of_trading_stages):
					for p in range(self.number_of_price_levels):
						self.model.addConstr(self.transaction_volumes[dp][s][i][i][p] <= self.volume_levels[dp][i][p][s], "v_c <= d_p")

		# Limit imbalance volume
		for dp in range(self.number_of_dps):
			for s in range(self.number_of_scenarios):
				self.model.addConstr(self.imbalance_volume[dp][s] <= self.bm_upper_bound, "imbalance_upper_bound")

		# Non-anticipativity
		for dp in range(self.number_of_dps):
			for s in range(self.number_of_scenarios-1):	
				self.model.addConstr(self.bid_volume[dp][s][0] == self.bid_volume[dp][s+1][0], "non_anticipativity")

	### ----------- Objective Function -----------
	def setup_objective_function(self):
		# The objective function can be divided into:
		# - Transaction revenues
		# - Production costs
		# - Transaction costs
		# - Imbalance costs

		self.model.setObjective((quicksum(self.scenario_probabilities[s]*self.transaction_volumes[dp][s][i][j][p] * self.price_levels[dp][i][p][s] for p in range(self.number_of_price_levels) for j in range(self.number_of_trading_stages) for i in range(self.number_of_trading_stages) for s in range(self.number_of_scenarios) for dp in range(self.number_of_dps))) 
			- sum(self.scenario_production_costs[x][s]*self.scenario_probabilities[s] * self.production_quantities[dp][s][x] for x in range(self.number_of_production_units) for s in range(self.number_of_scenarios) for dp in range(self.number_of_dps)) 
			- self.transaction_cost*sum(self.bid_volume[dp][s][i][p] for p in range(self.number_of_price_levels) for i in range(self.number_of_trading_stages) for s in range(self.number_of_scenarios) for dp in range(self.number_of_dps))
			- sum(self.scenario_probabilities[s] * sum(self.bms[dp][s] * self.imbalance_volume[dp][s] for dp in range(self.number_of_dps)) for s in range(self.number_of_scenarios)), GRB.MAXIMIZE)

	### ----------- Optimization -----------
	def optimize(self):
		if(self.printing_mode):
			print("\nOptimizing model...")
		self.start = time.time()
		self.model.optimize()
		if(self.printing_mode):
			print("Done optimizing model. Elapsed time: " + str(float(int(10*(time.time()-self.start)))/10.0)  + " seconds.")


	### ----------- Output Results -----------
	def print_results(self):
		if(self.printing_output):
			for v in self.model.getVars():
			    print("%s %f" % (v.Varname, v.X))

			for dp in range(self.number_of_dps):
				for s in range(self.number_of_scenarios):
					print("Inflow_X_X_X_X_"+str(dp)+"_"+str(s)+ " " + str(self.scenario_inflows[dp][s]))

			self.model.write("Output/hydro_multiasset_multiproduct_continuous_stochastic_MC_output.sol")

	### ----------- Support Functions -----------
	def write_variables_to_file(self):
		book = xlsxwriter.Workbook(self.variables_file_name)
		sheet = book.add_worksheet("Variables")

		# Fill spreadsheet
		counter = 0
		for v in self.model.getVars():
			sheet.write(counter, 0, str(v.Varname) + " " + str(v.X))
			counter += 1
		for dp in range(self.number_of_dps):
			for s in range(self.number_of_scenarios):
				sheet.write(counter, 0, "Inflow_X_X_X_X_"+str(dp)+"_"+str(s)+ " " + str(self.scenario_inflows[dp][s]))
				counter += 1
		book.close()

