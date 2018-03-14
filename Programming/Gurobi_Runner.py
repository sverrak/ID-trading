import itphelper
from ID102_ITP_Solver import ITP_Solver 
import datetime
import time
import numpy
import xlsxwriter
import csv

class Gurobi_Runner(object):
	"""docstring for Gurobi_Runner"""
	def __init__(self):
		super(Gurobi_Runner, self).__init__()
		self.result_table = [["Model", "DPs", "Scenarios", "Production_units", "Trading_timeslots", "Elapsed_time", "Number_of_variables","Number_of_linear_constraints","Number_of_SOS_constraints","Number_of_quadratic_constraints","Number_of_non-zero_coefficients_in_the_constraint_matrix","Number_of_non-zero_quadratic_objective_terms","Number_of_non-zero_terms_in_quadratic_constraints","Number_of_integer_variables","Number_of_binary_variables","Number_of_variables_with_piecewise-linear_objective_functions.","Model_name","Model_sense_(minimization_or_maximization)","Constant_offset_for_objective_function","Objective_value_for_current_solution"]]

	def do_one_run(self, dp, s, pu, tt, generate_scenarios_arg=True, file_name=""):
		if(file_name==""):
			self.itp_solver = ITP_Solver(generate_scenarios=generate_scenarios_arg, generate_random_variables=True, printing_output=False)
		else:
			self.itp_solver = ITP_Solver(generate_scenarios=generate_scenarios_arg, generate_random_variables=True, printing_output=False, parameter_file_name=file_name)
		
		# Fetch and setup the parameters
		if(generate_scenarios_arg == True):
			print("Reset parameters...")
			self.itp_solver.reset_parameters(dp, s, pu, tt)
		
		# Setup variables, constraints and objective function
		self.itp_solver.setup_variables()
		self.itp_solver.setup_constraints()
		self.itp_solver.setup_objective_function()

		# Solve the model
		self.itp_solver.optimize()

		# Output the variables
		self.itp_solver.write_variables_to_file()
		try:
			return [self.itp_solver.model.NumVars, self.itp_solver.model.NumConstrs, self.itp_solver.model.NumSOS, self.itp_solver.model.NumQConstrs, self.itp_solver.model.NumNZs, self.itp_solver.model.NumQNZs, self.itp_solver.model.NumQCNZs, self.itp_solver.model.NumIntVars, self.itp_solver.model.NumBinVars, self.itp_solver.model.NumPWLObjVars, self.itp_solver.model.ModelName, self.itp_solver.model.ModelSense, self.itp_solver.model.ObjCon, self.itp_solver.model.ObjVal]
		except:
			return [self.itp_solver.model.NumVars, self.itp_solver.model.NumConstrs, self.itp_solver.model.NumSOS, self.itp_solver.model.NumQConstrs, self.itp_solver.model.NumNZs, self.itp_solver.model.NumQNZs, self.itp_solver.model.NumQCNZs, self.itp_solver.model.NumIntVars, self.itp_solver.model.NumBinVars, self.itp_solver.model.NumPWLObjVars, self.itp_solver.model.ModelName, self.itp_solver.model.ModelSense, self.itp_solver.model.ObjCon, "Infeasible"]
		

	def do_multiple_runs(self, dps, ss, pus, tts):
		x = 1.0
		if (True):
			a = 0
		else:
			dps 		= [2 for i in range(10)]
			ss 			= [2 for i in range(10)]
			pus 		= [2 for i in range(10)]
			tts 		= [3 for i in range(10)]

		number_of_models = len(dps)*len(ss)*len(pus)*len(tts)
		
		model_iterator = 1
		for dp in dps:
			for s in ss:
				for pu in pus:
					for tt in tts:
						print("Parameters",dp,s,pu,tt)
						self.start = time.time()
						list = self.do_one_run(dp,s,pu,tt)
						self.elapsed_time = time.time() - self.start
						model_iterator += 1
                
						self.result_table.append([model_iterator, dp, s, pu, tt, self.elapsed_time] + list)
						del self.itp_solver
						
						if(model_iterator % 20 == 0):
							
							print("\n\n\n***** Model " + str(model_iterator) + " ( " + str(int(100*model_iterator/number_of_models)) + " %) *****")

	def print_solution(self):
		for r in self.result_table:
			print(r)
		

	def write_results_to_file(self):
		file_name = "Output/" + datetime.datetime.now().strftime("%Y-%m-%d_%H.%M") + "_Running_time_sensitivity_analysis" + ".csv"
		itphelper.write_matrix_to_file(list(self.result_table), file_name)

if __name__ == "__main__":
	t0 = time.time()
	mode = 2

	if(mode == 1): # Multirun case
		gr = Gurobi_Runner()
		 # Parameter set 1
		dps 		= [5,10,24]
		ss 			= [10,40,70,100]
		pus 		= [1,2,3,5]
		tts 		= [2,3,5]
		
		gr.do_multiple_runs(dps, ss, pus, tts)
		gr.write_results_to_file()

		del gr
		print("\nFILE CREATED")
		gr2 = Gurobi_Runner()

		# Parameter set 2
		dps 		= [24]
		ss 			= [10,100,200,500]
		pus 		= [2,3,10,20]
		tts 		= [2,10,20,40]
		
		gr2.do_multiple_runs(dps, ss, pus, tts)
		gr2.write_results_to_file()
		print("\nFILE CREATED")
		del gr2
		gr3 = Gurobi_Runner()
		# Parameter set 3
		dps 		= [24]
		ss 			= [10,100,200,500,1000]
		pus 		= [2,3]
		tts 		= [2,10,100]
		
		gr3.do_multiple_runs(dps, ss, pus, tts)
		gr3.write_results_to_file()
		
		#gr.print_solution()
		
	elif(mode == 2): # Single run
		gr = Gurobi_Runner()
		# Arguments: DP,S,PU,T,GenerateScenarios,ParameterFileName
		gr.do_one_run(10,100,1,10, True, "") # Insert parameter file name in the ""
		# Your code here
		gr.result_table.append(list)
		gr.write_results_to_file()
		
		
	print("\n\nTotal running time: " + str(time.time() - t0))