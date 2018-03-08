# Status: 
# Description: Initial implementation of the multiasset hydropower producer's trading problem
 
### ----------- External Packages -----------
from gurobipy import *
import itphelper
import datetime
import time
import numpy
import math
import xlsxwriter
import csv

class ITP_Solver(object):
	"""docstring for Gurobi_Controller"""
	def __init__(self, generate_scenarios=False, generate_random_variables=False, printing_output=False, output_solutions=False, parameter_file_name="Data/ID101_hydro_multiasset_multiproduct_continuous_stochastic_MC_parameters_bm_OD.txt"):
		super(ITP_Solver, self).__init__()


		### ----------- Model Initialization -----------
		self.model 									= Model('itp')
		self.model.setParam('OutputFlag', 			1 if output_solutions else 0)
		self.model.params.threads 					= 1 
		self.model.update()
		self.model_id								= "101"

		### ----------- Set System Parameters -----------
		self.generate_scenarios 					= generate_scenarios 																															# If scenarios should be autogenerated or not
		self.generate_random_variables				= generate_random_variables																														# if random variables should be loaded from file or not (only possible to load from models with identical dimensions)
		self.printing_output						= printing_output																																# If optimal variables should be printed in terminal or not
		self.parameter_file_name 					= parameter_file_name	                                            		              														# Directory of the input parameter file if not autogenerated scenarios
		self.scenario_generation_file_name			= "Data/ID" 						+ self.model_id + "_" + "scenario_generation_input.txt"														# Directory of the input parameter file for autogenerated scenarios
		self.variables_file_name					= "Output/VariableFiles/ID" 		+ self.model_id + "_" + datetime.datetime.now().strftime("%Y-%m-%d_%H.%M") + "variables.xlsx"				# Directory of the output file with all the optimal variable values
		self.random_variable_file_name				= "Output/RandomVariableFiles/ID" 	+ self.model_id + "_" + datetime.datetime.now().strftime("%Y-%m-%d_%H.%M") + "random_variables.csv"			# Directory of the folder of the random variables 
		self.printing_mode							= False 																																		# If program progression should be printed or not
		self.start 									= time.time()																																	# Time keeper

		### ----------- Set Model Parameters -----------
		if(generate_scenarios == True): 																																							# Generate scenarios
			if(self.printing_mode):
				print("\nGenerating scenarios from parameter file...")
			self.start = time.time()
			self.scenario_generation(self.scenario_generation_file_name)
			if(self.printing_mode):
				print("Scenarios generated. Elapsed time: " + str(float(int(10*(time.time()-self.start)))/10.0) + " seconds.")
		else:																																														# Fetch scenarios from file
			# Set self.model parameters
			self.number_of_trading_stages 			= 0 # To do: This might not be updated properly if not autogenerating scenarios
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
			self.asynchronous_gcs					= False							# Indicates whether we are running in asynchronous gate closure mode or not. Can be set in scenario_generation_file
            self.stages_per_hour                    = 6
            self.time_of_first_gc					= 0								# Parameter used in the asynchronous gate closure cases
			
			# Indices
			self.index_of_actions 					= 0								
			self.index_of_q_bounds 					= self.index_of_actions + 1
			self.index_of_dps 						= self.index_of_q_bounds + 1
			self.index_of_scenario_probabilities 	= 4								

			

			# Read parameters in separate function
			self.read_parameters(self.parameter_file_name)

		# Test given parameters
		self.validate_parameters()													# Validate parameters

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

	# Actually a set function. As of now, the ITP_Solver objects are instantiated with one set of parameters. 
	# When calling ITP_Solver objects from Gurobi_Runner, the parameters must be "reset" after this initial instantiation. 
	# Can probably be omitted
	def reset_parameters(self, dps, scenarios, production_units, trading_stages):
		self.number_of_dps = dps
		self.number_of_scenarios = scenarios
		self.number_of_production_units = production_units
		self.number_of_trading_stages = trading_stages

		# Redo datastructure instantiation
		self.instantiate_datastructures()
		self.setup_non_anticipativity_sets()
        
        # Test given parameters
		self.validate_parameters()													# Validate parameters
        
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
		temp_list									= (self.data[self.index_of_model_params].strip().split("\t"))
		self.number_of_dps 							= int(temp_list[0])
		self.number_of_production_units				= int(temp_list[1])
		self.number_of_trading_stages 				= int(temp_list[2])
		self.number_of_scenarios 					= int(temp_list[3])
		self.bm_upper_bound							= int(temp_list[4])
		self.number_of_price_levels					= int(temp_list[5])
		self.total_production_lower_bound			= float(self.data[self.index_of_production_bounds].split("\t")[0])
		self.total_production_upper_bound			= float(self.data[self.index_of_production_bounds].split("\t")[1])
		self.inflow_lower_bound						= float(self.data[self.index_of_inflow_bounds].split("\t")[0])
		self.inflow_upper_bound						= float(self.data[self.index_of_inflow_bounds].split("\t")[1])
		self.production_cost_lower_bound			= float(self.data[self.index_of_production_cost_bounds].split("\t")[0])
		self.production_cost_upper_bound			= float(self.data[self.index_of_production_cost_bounds].split("\t")[1])
		self.production_capacity_lower_bound		= float(self.data[self.index_of_production_capacity_bounds].split("\t")[0])
		self.production_capacity_upper_bound		= float(self.data[self.index_of_production_capacity_bounds].split("\t")[1])
		self.initial_storage 						= float(self.data[self.index_of_storage_bounds].split("\t")[2])
		self.storage_bounds                     	= [float(self.data[self.index_of_storage_bounds].split("\t")[i]) for i in range(2)]
		self.volume_bounds 							= [float(self.data[self.index_of_bid_volume_bounds].split("\t")[i]) for i in range(2)]
		self.price_level_bounds 					= [float(self.data[self.index_of_price_level_bounds].split("\t")[i]) for i in range(2)]
		self.volume_level_bounds 					= [float(self.data[self.index_of_volume_level_bounds].split("\t")[i]) for i in range(2)]
		
		self.instantiate_datastructures()
		self.setup_non_anticipativity_sets()
	
	def setup_non_anticipativity_sets(self):
		self.non_anticipativity_sets				= [[{} for t in range(self.number_of_trading_stages)] for d in range(self.number_of_dps)]
		for dp in range(self.number_of_dps):
			for t in range(1, self.number_of_trading_stages+1):
				for s in range(self.number_of_scenarios):
					scenario_string = self.generate_scenario_NA_string2(dp,s,t) 				# Scenario_string is a textual representation of the scenario variables up until time t
					
					if(scenario_string in self.non_anticipativity_sets[dp][t-1].keys()): 		# If all scenario specific entities equal to those of existing non_anticipativity_set:
						
						self.non_anticipativity_sets[dp][t-1][scenario_string].append(s)		# Append the scenario to the list of equal scenarios
					else:

						self.non_anticipativity_sets[dp][t-1][scenario_string] = [s]			# Otherwise: create a new list including the scenario_string
	
	# Check if scenario can be placed in a non-anticipativity set for timestep t. The only trading time dependent scenario variables are the price and volume levels
	def generate_scenario_NA_string(self, s, t):
		scenario_string 							= ""
		
		price_levels_string				 			= str(itphelper.get_sublist(self.price_levels, 1, t, s))
		volume_levels_string				 		= str(itphelper.get_sublist(self.volume_levels, 1, t, s))
		
		scenario_string 							+= price_levels_string + volume_levels_string
		
		return scenario_string

	def generate_scenario_NA_string2(self, dp, s, t):
		scenario_string 							= ""
		
		price_levels_string				 			= str(itphelper.get_sublist(self.price_levels[dp], 0, t, s))
		volume_levels_string				 		= str(itphelper.get_sublist(self.volume_levels[dp], 0, t, s))
		
		scenario_string 							+= price_levels_string + volume_levels_string
		
		return scenario_string


	def instantiate_datastructures(self):
		### Generate random variables
		if (self.generate_random_variables == True):
			random_variables 				= [[numpy.random.uniform(low=0, high=1.0) for k in range(200)] for i in range(self.number_of_scenarios * self.number_of_dps * self.number_of_trading_stages * self.number_of_price_levels)]
			itphelper.write_matrix_to_file(random_variables, self.random_variable_file_name)

		else:
			random_variables_raw 			= self.fetch_data(self.random_variable_file_name)
			random_variables 				= [[float(i) for i in random_variables_raw[x].strip().split(" ")] for x in range(len(random_variables_raw))]

		
		### Initialize variables and datastructures
		self.scenario_probabilities 		= [1.0/self.number_of_scenarios for i in range(self.number_of_scenarios)]
		self.scenario_inflows 				= [[[] for s in range(self.number_of_scenarios)] for j in range(self.number_of_dps)]
		self.production_costs_bounds 		= [float(i) for i in self.data[self.index_of_production_cost_bounds].strip().split("\t")]
		self.production_capacities_bounds	= [float(i) for i in self.data[self.index_of_production_capacity_bounds].strip().split("\t")]
		self.price_levels 					= [[[[0 for s in range(self.number_of_scenarios)] for p in range(self.number_of_price_levels)] for t in range(self.number_of_trading_stages)] for dp in range(self.number_of_dps)]
		self.volume_levels 					= [[[[0 for s in range(self.number_of_scenarios)] for p in range(self.number_of_price_levels)] for t in range(self.number_of_trading_stages)] for dp in range(self.number_of_dps)]
		self.bms 							= [[[] for s in range(self.number_of_scenarios)] for x in range(self.number_of_dps)]
		self.scenario_production_costs		= [[0 for s in range(self.number_of_scenarios)] for p in range(self.number_of_production_units)]
		self.scenario_production_capacities	= [[0 for s in range(self.number_of_scenarios)] for p in range(self.number_of_production_units)] 

		### Instantiate variables and datastructures
		self.allow_overflow 				= True if str(self.data[self.index_of_allow_overflow][0]).strip() == "1" else False
		self.asynchronous_gcs 				= True if str(self.data[self.index_of_allow_overflow][1]).strip() == "1" else False
        self.stages_per_hour                = int(self.data[self.index_of_allow_overflow][2])
		self.min_q, self.max_q 				= float(self.total_production_lower_bound), float(self.total_production_upper_bound)
		self.bm_multiplier 					= float(self.data[self.index_of_bm_multiplier])
		self.transaction_cost 				= float(self.data[self.index_of_transaction_costs])
		self.time_of_first_gc				= self.number_of_trading_stages - self.stages_per_hour * self.number_of_dps
		

		### Instantiate the price and volume levels as well as the inflow and the imbalance prices (all the datastructures having dp as their first dimension.
		for dp in range(self.number_of_dps):
			for t in range(self.number_of_trading_stages):
				for p in range(self.number_of_price_levels):
					for s in range(self.number_of_scenarios):
						# Price levels: declining with p
						if (False):#(s > 0 and t == 0):
							self.price_levels[dp][t][p][s] 				= self.price_levels[dp][t][p][0]
						else:

							# A lot of effort was placed on getting the following instantiation right. 
							anticipativity_threshold = 0 																# The number of trading stages where some subset of scenarios share the same attribute values
							for n in range(int(math.sqrt(self.number_of_scenarios))):									# To set this threshold, we reduce the bucket size exponentially until the bucket size equals two
								if(float(self.number_of_scenarios)/(2.0**n) >= 2.0):									
									anticipativity_threshold = n
								else:
									break																				# When bucket size is no longer greater than 2, we quit the loop
							
							# Two cases are relevant here. Those of the lowest price level (p == 0) and those of other price levels (p > 0). 
							# In the former, we bound the price by the price level bounds suggested in the input file.
							# In the latter, we also bound the price by the price of the previous price level from below.
							# The ugly modulo expressions ensure that some subset of the scenarios share the same price info
							# Note: volume levels are only bounded by the bounds of the input file. Thus, there is no ordering of volume levels.
							if (p == 0):
								if(t <= anticipativity_threshold):
									
									self.price_levels[dp][t][p][s] 		= int(self.price_level_bounds[0] 	+ (self.price_level_bounds[1] 		- self.price_level_bounds[0]) 	* random_variables[dp * self.number_of_trading_stages * self.number_of_scenarios * self.number_of_price_levels + t * self.number_of_scenarios * self.number_of_price_levels + p * self.number_of_scenarios + s - (s % int(float(self.number_of_scenarios) / 2.0**t) if int(float(self.number_of_scenarios) / 2.0**t) > 0 else self.number_of_scenarios)][0])
									self.volume_levels[dp][t][p][s]		= int(self.volume_level_bounds[0] 	+ (self.volume_level_bounds[1] 		- self.volume_level_bounds[0]) 	* random_variables[dp * self.number_of_trading_stages * self.number_of_scenarios * self.number_of_price_levels + t * self.number_of_scenarios * self.number_of_price_levels + p * self.number_of_scenarios + s - (s % int(float(self.number_of_scenarios) / 2.0**t) if int(float(self.number_of_scenarios) / 2.0**t) > 0 else self.number_of_scenarios)][1])
								else:
									self.price_levels[dp][t][p][s] 		= int(self.price_level_bounds[0] 	+ (self.price_level_bounds[1] 		- self.price_level_bounds[0]) 	* random_variables[dp * self.number_of_trading_stages * self.number_of_scenarios * self.number_of_price_levels + t * self.number_of_scenarios * self.number_of_price_levels + p * self.number_of_scenarios + s][0])
									self.volume_levels[dp][t][p][s]		= int(self.volume_level_bounds[0] 	+ (self.volume_level_bounds[1] 		- self.volume_level_bounds[0]) 	* random_variables[dp * self.number_of_trading_stages * self.number_of_scenarios * self.number_of_price_levels + t * self.number_of_scenarios * self.number_of_price_levels + p * self.number_of_scenarios + s][1])
							else:
								if(t <= anticipativity_threshold):
									self.price_levels[dp][t][p][s] 		= int(self.price_level_bounds[0] 	+ (self.price_levels[dp][t][p-1][s]	- self.price_level_bounds[0]) 	* random_variables[dp * self.number_of_trading_stages * self.number_of_scenarios * self.number_of_price_levels + t * self.number_of_scenarios * self.number_of_price_levels + p * self.number_of_scenarios + s - (s % int(float(self.number_of_scenarios) / 2.0**t) if int(float(self.number_of_scenarios) / 2.0**t) > 0 else self.number_of_scenarios)][0])
									self.volume_levels[dp][t][p][s]		= int(self.volume_level_bounds[0] 	+ (self.volume_level_bounds[1] 		- self.volume_level_bounds[0]) 	* random_variables[dp * self.number_of_trading_stages * self.number_of_scenarios * self.number_of_price_levels + t * self.number_of_scenarios * self.number_of_price_levels + p * self.number_of_scenarios + s - (s % int(float(self.number_of_scenarios) / 2.0**t) if int(float(self.number_of_scenarios) / 2.0**t) > 0 else self.number_of_scenarios)][1])
								else:
									self.price_levels[dp][t][p][s] 		= int(self.price_level_bounds[0] 	+ (self.price_levels[dp][t][p-1][s] - self.price_level_bounds[0]) 	* random_variables[dp * self.number_of_trading_stages * self.number_of_scenarios * self.number_of_price_levels + t * self.number_of_scenarios * self.number_of_price_levels + p * self.number_of_scenarios + s][0])
									self.volume_levels[dp][t][p][s]		= int(self.volume_level_bounds[0] 	+ (self.volume_level_bounds[1] 		- self.volume_level_bounds[0]) 	* random_variables[dp * self.number_of_trading_stages * self.number_of_scenarios * self.number_of_price_levels + t * self.number_of_scenarios * self.number_of_price_levels + p * self.number_of_scenarios + s][1])
			
			### Instantiate the inflows and the imbalance prices
			for s in range(self.number_of_scenarios):
				# Inflows
				self.scenario_inflows[dp][s] 				= self.inflow_lower_bound + (self.inflow_upper_bound - self.inflow_lower_bound) * random_variables[s][2]

				# Imbalance prices are set as the input multiplier times the weighted average bid price
				self.bms[dp][s] 							= self.bm_multiplier * sum(sum(self.price_levels[dp][t][p][s] * self.volume_levels[dp][t][p][s] for t in range(self.number_of_trading_stages)) for p in range(self.number_of_price_levels))  / sum(self.volume_levels[dp][t][p][s] for t in range(self.number_of_trading_stages) for p in range(self.number_of_price_levels))
		
		### Instantiate production costs and production capacities (all the datastructures having pu as their first dimension)
		for pu in range(self.number_of_production_units):
			for s in range(self.number_of_scenarios):
				# Production cost
				self.scenario_production_costs[pu][s] 		= self.production_cost_lower_bound + (self.production_cost_upper_bound - self.production_cost_lower_bound) * random_variables[s][3+pu]

				# Production capacities
				self.scenario_production_capacities[pu][s]	= self.production_capacity_lower_bound + (self.production_capacity_upper_bound - self.production_capacity_lower_bound) * random_variables[s][3+pu+self.number_of_production_units]

	### ----------- Fetch scenarios from file ----------- 
	def read_parameters(self, parameter_file):
		# Fetch data
		self.data = self.fetch_data(parameter_file)

		# Read initial data from file to calculate indices correctly
		self.volume_bounds 						= [float(i) for i in self.data[self.index_of_actions].strip().split("\t")]
		self.q_array 							= (list(self.data[self.index_of_q_bounds].strip().split("\t")))
		self.min_q, self.max_q 					= float(self.q_array[0]), float(self.q_array[1])
		temp_list 								= list(self.data[2].strip().split("\t"))
		self.number_of_dps 						= int(temp_list[0])
		self.number_of_production_units 		= int(temp_list[1])
		self.number_of_scenarios 	 			= int(temp_list[2])
		self.bm_upper_bound						= int(temp_list[3])
		self.number_of_price_levels				= int(temp_list[4])
		self.number_of_trading_stages			= 2

		# How the input file should be read Part 2
		self.index_of_scenario_inflows 			= self.index_of_scenario_probabilities + 1
		self.index_of_scenarios 				= self.index_of_scenario_probabilities + 1 + self.number_of_dps
		self.index_of_bm_price 					= 14
		self.index_of_transaction_costs 		= self.index_of_bm_price + 1 + self.number_of_dps # If necessary, modify this
		self.index_of_cpr 						= self.index_of_transaction_costs + 1
		self.index_of_production_quantities 	= self.index_of_transaction_costs + 3
		self.index_of_production_costs 			= self.index_of_production_quantities + self.number_of_production_units
		self.index_of_initial_storage 			= self.index_of_production_costs + self.number_of_production_units + 1
		self.index_of_storage_bounds 			= self.index_of_initial_storage + 1
		self.index_of_overflow					= self.index_of_storage_bounds + 1
		self.index_of_volume_levels				= self.index_of_overflow + 2
		self.index_of_price_levels 				= self.index_of_volume_levels + 1 + self.number_of_dps * self.number_of_trading_stages * self.number_of_price_levels

		# self.data structures
		self.scenario_probabilities 			= [float(i) for i in self.data[self.index_of_scenario_probabilities].split()]
		self.scenario_inflows 					= [[float(i) for i in self.data[self.index_of_scenario_inflows+j].split("\t")] for j in range(self.number_of_dps)]
		self.scenarios 							= [[] for i in range(self.number_of_dps)]
		self.scenario_production_capacities	= [[] for i in range(self.number_of_production_units)]
		self.scenario_production_costs 		= [[] for i in range(self.number_of_production_units)]
		self.transaction_cost 					= float(self.data[self.index_of_transaction_costs])
		self.production_cost 					= [float(i) for i in self.data[self.index_of_production_costs].strip().split("\t")]
		self.production_capacities 				= [float(i) for i in self.data[self.index_of_production_quantities].strip().split("\t")]
		self.cpr 					    			= float(self.data[self.index_of_cpr])
		temp_list 						      		= self.data[self.index_of_storage_bounds].split("\t")[:2]
		self.storage_bounds 		    		  	= [float(i) for i in temp_list]
		self.initial_storage 					= float(self.data[self.index_of_initial_storage])
		self.bms 					    			= [[float(i) for i in self.data[self.index_of_bm_price+x].strip().split("\t")] for x in range(self.number_of_dps)]
		self.price_levels 						= [[[[] for p in range(self.number_of_price_levels)] for t in range(self.number_of_trading_stages)] for dp in range(self.number_of_dps)]
		self.volume_levels 						= [[[[] for p in range(self.number_of_price_levels)] for t in range(self.number_of_trading_stages)] for dp in range(self.number_of_dps)]

		for pu in range(self.number_of_production_units):
			self.scenario_production_costs[pu] 		= [float(i) for i in self.data[self.index_of_production_quantities+pu].split("\t")]
			self.scenario_production_capacities[pu] = [float(i) for i in self.data[self.index_of_production_quantities+pu+self.number_of_production_units].split("\t")]
			
		for dp in range(self.number_of_dps):
			self.scenarios[dp] 						= [[] for j in range(self.number_of_trading_stages)]
			for i in range(self.number_of_trading_stages):
				self.scenarios[dp][i] 				= [float(j) for j in self.data[self.index_of_scenarios+i * self.number_of_dps+dp].split("\t")]
		
		for dp in range(self.number_of_dps):
			for t in range(self.number_of_trading_stages):
				for p in range(self.number_of_price_levels):
					self.volume_levels[dp][t][p] 	= [float(i) for i in self.data[self.index_of_volume_levels+dp*(self.number_of_trading_stages+self.number_of_price_levels)+t*self.number_of_price_levels+p].strip().split("\t")]
					self.price_levels[dp][t][p] 	= [float(i) for i in self.data[self.index_of_price_levels+dp*(self.number_of_trading_stages+self.number_of_price_levels)+t*self.number_of_price_levels+p].strip().split("\t")]
		

		self.allow_overflow = True if str(self.data[self.index_of_overflow]).strip() == "1" else False
		self.setup_non_anticipativity_sets()
	
	### ----------- Validating The Given Parameters ----------- 
	def validate_parameters(self):
        # Multiple tests could (and should?) be added.
		if(sum(self.scenario_probabilities) < 0.9999):
			raise ValueError('Sum of probabilities not equal 1 but' + str(sum(self.scenario_probabilities)))
		if(self.asynchronous_gcs == True and self.time_of_first_gc < 0):
			raise ValueError('Not enough trading timeslots to allow for asynchronous gate closures.')
	### ----------- Variables -----------
	def setup_variables(self):
		# The following variables are created immediately:
		self.production_quantities 					= [[[self.model.addVar(vtype=GRB.CONTINUOUS,lb=self.min_q,ub=self.max_q, name="production_quantity_X_X_"+str(dp)+"_"+str(s)+"_"+str(j)) for j in range(self.number_of_production_units)] for s in range(self.number_of_scenarios)] for dp in range(self.number_of_dps)]    # Production quantity
		self.storage_volume 						= [[self.model.addVar(name="storage_volume_X_X_X_"+str(dp)+"_"+str(s),vtype=GRB.CONTINUOUS, lb=0.0) for s in range(self.number_of_scenarios)] for dp in range(self.number_of_dps)]
		self.imbalance_volume						= [[self.model.addVar(name="imbalance_volume_X_X_X_"+str(dp)+"_"+str(s)) for s in range(self.number_of_scenarios)] for dp in range(self.number_of_dps)]

		if(self.allow_overflow):
			self.overflow	 						= [[self.model.addVar(name="overflow_X_X_"+str(dp)+"_"+str(s),vtype=GRB.CONTINUOUS, lb=0.0) for s in range(self.number_of_scenarios)] for dp in range(self.number_of_dps)]

		# The rest are created in the following loops. This could probably have been omitted, but is unlikely to affect performance significantly.
		self.bid_volume 							= [[[[0 for p in range(self.number_of_price_levels)] for j in range(self.number_of_trading_stages)] for i in range(self.number_of_scenarios)] for dp in range(self.number_of_dps)]
		self.transaction_prices 					= [[[[[0 for p in range(self.number_of_price_levels)] for k in range(self.number_of_trading_stages)] for j in range(self.number_of_trading_stages)] for i in range(self.number_of_scenarios)] for dp in range(self.number_of_dps)]   # Average transaction price
		self.transaction_volumes 					= [[[[[0 for p in range(self.number_of_price_levels)] for k in range(self.number_of_trading_stages)] for j in range(self.number_of_trading_stages)] for i in range(self.number_of_scenarios)] for dp in range(self.number_of_dps)]   # Total transaction volume

		for dp in range(self.number_of_dps):
			for s in range(self.number_of_scenarios):
				for i in range(self.number_of_trading_stages):
					for p in range(self.number_of_price_levels):
						self.bid_volume[dp][s][i][p] 					= self.model.addVar(vtype=GRB.CONTINUOUS, lb=self.volume_bounds[0], ub=self.volume_bounds[1],		# Bid volume
					                                    name=str("b_v_X_"+str(dp)+"_"+str(s)+"_"+str(i)+"_"+str(p)))
					for j in range(self.number_of_trading_stages):
						for p in range(self.number_of_price_levels):
							self.transaction_prices[dp][s][i][j][p] 	= self.model.addVar(vtype=GRB.CONTINUOUS,		                        				# Transaction price
					                                    name=str("p_c_"+str(str(dp)+"_"+str(s)+"_"+str(i)+"_"+str(j)+"_"+str(p))))
							
							self.transaction_volumes[dp][s][i][j][p] 	= self.model.addVar(vtype=GRB.CONTINUOUS, lb=self.volume_bounds[0], ub=self.volume_bounds[1],		# Transaction volume
					                                    name=str("v_c_"+str(str(dp)+"_"+str(s)+"_"+str(i)+"_"+str(j)+"_"+str(p))))

	### ----------- Constraints -----------
	def setup_constraints(self):
		### Physical constraints 

		# Flow conservation of storage
		for s in range(self.number_of_scenarios):
			if(self.allow_overflow):
				self.model.addConstr(self.storage_volume[0][s] 			== self.initial_storage + self.scenario_inflows[0][s] - sum(self.production_quantities[0][s]) - self.overflow[0][s], "storage_propagation_0")
			else:
				self.model.addConstr(self.storage_volume[0][s] 			== self.initial_storage + self.scenario_inflows[0][s]- sum(self.production_quantities[0][s]), "storage_propagation_0")

		for dp in range(1, self.number_of_dps):
			for s in range(self.number_of_scenarios):
				if(self.allow_overflow):
					self.model.addConstr(self.storage_volume[dp][s] 	== self.storage_volume[dp-1][s] + self.scenario_inflows[dp][s] - sum(self.production_quantities[dp][s]) - self.overflow[dp][s], "storage_propagation")
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
					self.model.addConstr((self.production_quantities[dp][s][i]) 	<= self.scenario_production_capacities[i][s], "production_capacities")

		# Production equal to transaction volumes ++
		for dp in range(self.number_of_dps):
			for s in range(self.number_of_scenarios):
				self.model.addConstr((sum(self.production_quantities[dp][s][x] for x in range(len(self.production_quantities[dp][s]))) - sum(self.transaction_volumes[dp][s][i][j][p] for p in range(self.number_of_price_levels) for j in range(self.number_of_trading_stages) for i in range(self.number_of_trading_stages)) - self.imbalance_volume[dp][s] == 0), "q=v_c_"+str(s))

		# Asynchronous time of gate closure
		if(self.asynchronous_gcs):
			if(self.number_of_trading_stages - self.number_of_dps > self.time_of_first_gc):
				for dp in range(self.number_of_dps):
					for s in range(self.number_of_scenarios):
						for i in range(self.number_of_trading_stages - self.number_of_dps, self.number_of_trading_stages):
							for p in range(self.number_of_price_levels):
								self.model.addConstr((self.bid_volume[dp][s][i][p] == 0), "no bid placement after gate closure")
			else:
				print("Error. Not enough trading stages to allow for asynchronous gate closures")

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

		# Setup non-anticipativity constraints 
		self.setup_non_anticipativity_constraints() 

	# Setup non-anticipativity constraints (this function is called from setup_constraints(self))
	# Consists of two parts: 
	# 	- Bid volume NACs
	# 	- Production quantity NACs
	def setup_non_anticipativity_constraints(self):
		# If we would like to count the number of NA-constraints
		na_counter = 0

		# We loop through all the timesteps in the non_anticipativity sets and collect the keys. 
		# Then, we force each of scenarios with identical identifier to have identical bid volume value in the given trading timeslot.
		for dp in range(self.number_of_dps):
			for t in range(self.number_of_trading_stages):
				# NA_set is the sets of non_anticipative(?) scenarios in trading timeslot t
				NA_set = self.non_anticipativity_sets[dp][t]

				for key in NA_set.keys():
					
					# If there are more than one scenario having a certain scenario key, we must create NA constraints!
					if(len(NA_set[key]) > 1):
						# Now, loop through all the scenarios sharing this scenario key and force them to have identical bid volumes in the given trading timeslot
						for i,s in enumerate(NA_set[key][0:-1]):
							for p in range(self.number_of_price_levels):
								self.model.addConstr(self.bid_volume[dp][s][t][p] == self.bid_volume[dp][NA_set[key][i+1]][t][p], "Bid_volume_NACs")
								na_counter += 1
		
		# Asynchronous time of gate closure 
		if(self.asynchronous_gcs):
			na_counter_gcs = 0
			for dp in range(self.number_of_dps):
				
				# For the current combination of delivery product (dp) and time (t=time_of_first_gc + dp), 
				# we collect the corresponding NA sets
				NA_set = self.non_anticipativity_sets[dp][self.time_of_first_gc+self.stages_per_hour * dp] # What if timeslot size != 60 m?

				# For each of these NA_sets, we create NACs
				for key in NA_set.keys():
					
					# If there are more than one scenario having a certain scenario key, we must create NA constraints!
					if(len(NA_set[key]) > 1):
						for i,s in enumerate(NA_set[key][0:-1]):
							for pu in range(self.number_of_production_units):
								self.model.addConstr((self.production_quantities[dp][s][pu] == self.production_quantities[dp][NA_set[key][i+1]][pu]), "Production_quantity_NACs")
								na_counter_gcs += 1
	

			
		if(self.printing_mode):
			print("Number of bid volume NA constraints: " + str(na_counter))
			print("Number of gate closure NA constraints: " + str(na_counter_gcs))
		
		# Non-anticipativity
		#for dp in range(self.number_of_dps):
		#	for s in range(self.number_of_scenarios - 1):	
		#		self.model.addConstr(self.bid_volume[dp][s][0] == self.bid_volume[dp][s + 1][0], "non_anticipativity")

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

		for dp in range(self.number_of_dps):
			for t in range(self.number_of_trading_stages):
				for p in range(self.number_of_price_levels):
					for s in range(self.number_of_scenarios):
						sheet.write(counter, 0, "Price_level_"+str(dp)+"_"+str(t)+"_"+str(p)+"_"+str(s)+ " "+ str(self.price_levels[dp][t][p][s]))
						counter += 1
		book.close()

