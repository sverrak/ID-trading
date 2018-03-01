import itphelper
from ITP_Solver import ITP_Solver 
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

	def do_one_run(self, dp, s, pu, tt):
		self.itp_solver = ITP_Solver(generate_scenarios=True, generate_random_variables=True, printing_output=False)
		# Fetch and setup the parameters
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
		

	def do_multiple_runs(self):
		x = 1.0
		if (False):
			dps 		= [i for i in range(2, int(12*x), 4)][::-1]
			ss 			= [i for i in range(10, int(11*x), 1)]
			pus 		= [i for i in range(2, int(4*x), 1)][::-1]
			tts 		= [i for i in range(2, int(20*x), 6)][::-1]
		else:
			dps 		= [5,10]
			ss 			= [100]
			pus 		= [3]
			tts 		= [3]

		model_iterator = 1
		for dp in dps:
			for s in ss:
				for pu in pus:
					for tt in tts:
						self.start = time.time()
						list = self.do_one_run(dp,s,pu,tt)
						self.elapsed_time = time.time() - self.start
						model_iterator += 1
						self.result_table.append([model_iterator, dp, s, pu, tt, self.elapsed_time] + list)
						del self.itp_solver
						
						if(model_iterator % 20 == 0):
							print("Model iterator: " + str(model_iterator))

	def print_solution(self):
		for r in self.result_table:
			print(r)
		

	def write_results_to_file(self):
		itphelper.write_matrix_to_file(self.result_table, "Output/" + datetime.datetime.now().strftime("%Y-%m-%d_%H.%M") + "_Running_time_sensitivity_analysis" + ".csv")

if __name__ == "__main__":
	t0 = time.time()
	if(True):
		gr = Gurobi_Runner()
		#gr.do_one_run(10,100,1,10)
		#list = gr.do_one_run(2,10,1,2)
		gr.result_table.append(list)
		gr.do_multiple_runs()
		#gr.print_solution()
		gr.write_results_to_file()
	else:
		
        # Testing environment
		mylist = [[[20*i+5*j+k for k in range(5)] for j in range(4)] for i in range(3)]
		print(mylist)
		print(itphelper.get_sublist(mylist, 2, 2))
		print(itphelper.get_sublist(mylist, 1, 1))
	print("\n\nTotal running time: " + str(time.time() - t0))



