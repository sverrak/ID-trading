# Description: This file contains several support functions for the Gurobi_Runner and ITP_Solver classes. 

import time
import numpy
import xlsxwriter
import csv

def write_matrix_to_file(matrix, file_name):
	try:
		with open(file_name, 'w', newline='') as csvfile:
		    csv_writer = csv.writer(csvfile, delimiter=' ',
		                            quotechar='|', quoting=csv.QUOTE_MINIMAL)
		    for r in matrix:
		    	csv_writer.writerow(r)
	except:
	   	with open(file_name, 'w', newline='') as csvfile:
		    csv_writer = csv.writer(csvfile, delimiter=' ',
		                            quotechar='|', quoting=csv.QUOTE_MINIMAL)
		    #for r in matrix:
		    #	csv_writer.writerow(r)
		    csv_writer.writerows(map(lambda x: [x], matrix))

def get_sublist(my_list, time_index, time, s):
	if(time_index == 0):
		temp = []
		for t in range(time):
			temp_row = []
			for p in range(len(my_list[t])):
				temp_row.append(my_list[t][p][s])
			temp.append(temp_row)
		return temp

	if(time_index == 1):
		temp = []

		for dp in range(len(my_list)):
			temp_row = []
			for t in range(time):
				temp_row_row = []
				for p in range(len(my_list[dp][t])):
					temp_row_row_row = [my_list[dp][t][p][s]]
					
					temp_row_row.append(temp_row_row_row)

				temp_row.append(temp_row_row)
			temp.append(temp_row)
		
		return temp
		
		for dp in range(len(my_list)):
			temp_row = []
			for t in range(time):
				temp_row_row = []
				for u in range(len(my_list[dp][t])):
					temp_row_row.append(my_list[dp][t][u])
				temp_row.append(temp_row_row)
			temp.append(temp_row)
		return tempmy_list
		return [((my_list[i][j][k] for k in range(len(my_list[i][j]))) for j in range(time_index)) for i in range(0,len(my_list))]
	elif(time_index == 2):
		return [[my_list[i][j][:time] for j in range(len(my_list[i]))] for i in range(0,len(my_list))]
	elif(time_index == 3):
		return [[[my_list[i][j][k][:time] for k in range(len(my_list[i][j]))] for j in range(len(my_list[i]))] for i in range(0,len(my_list))]	

	raise ValueError("More than 3 levels of sublist")