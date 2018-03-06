# Description: The purpose of this program is to split the data from the original EPEX self.Orderbooks from Markedskraft, where 15-31 days where bundled together,
# so that all orders of a day are saved in a separate file. Also, some effort has been made in order to try to split each delivery product.
# Status: Not working properly. Hypothesis: Change in data complicates the process from sept-16

### External packages
from datetime import datetime as dt
import xlsxwriter
import datetime
import time

class Data_Organizer(object):
	"""docstring for Data_Organizer"""
	def __init__(self, generate_orderbook_urls=False):
		super(Data_Organizer, self).__init__()

		# Datastructure instantiation. Used to 
		self.years 							= ["2014","2015","2016","2017"]
		self.months 						= [str(i) if i>9 else "0"+str(i) for i in range(1,13)]
		self.days_of_months					= [0,31,28,31,30,31,30,31,31,30,31,30,31]
		self.orderbooks 					= {}
		self.program_starting_time 			= time.time()
		self.generate_orderbook_urls		= generate_orderbook_urls
		self.data 							= []

		# Generate orderbook URLs
		if(self.generate_orderbook_urls == True):
			for y in self.years:
				for i,m in enumerate(self.months):
					if(int(y) < 2016 and int(m) < 10):

						self.orderbooks[(y,m,0)] 	= ("ComXervOrderbooks_" + y + "_" + m + ".txt")
						print(("ComXervOrderbooks_" + y + "_" + m + ".txt"))
					else:
						self.orderbooks[(y,m,1)] 	= ("ComXervOrderbooks_" + y + "_" + m + "_01-" + y + "_" + m + "_15"".txt")
						self.orderbooks[(y,m,2)] 	= ("ComXervOrderbooks_" + y + "_" + m + "_16-" + y + "_" + m + "_"+str(self.days_of_months[int(m)])+".txt")
			
		# Custom orderbook URL
		else:
			self.orderbooks[("2016","09",0)] 		= "ComXervOrderbooks_2016_09_01-2016_09_15.txt"

	# Takes a start and an end day and returns a list containing all the dates between these two days
	def date_strings_between(self, start,end):
		start_date 			= dt.strptime(start + " " + "00:00:00", '%Y-%m-%d %H:%M:%S')
		end_date 			= dt.strptime(end 	+ " " + "00:00:00", '%Y-%m-%d %H:%M:%S')
		dates_between_str	= []
		current_date		= start_date
		
		while(current_date	<= end_date):
			dates_between_str.append(dt.strftime(current_date,'%Y-%m-%d'))
			current_date 	= current_date + datetime.timedelta(days=1)

		return dates_between_str

	# Returns the content of file_name in a matrix. Actual_run=False runs this method in testing mode
	def read_data(self, file_name, actual_run=True):
		start = time.time()
		print("Reading data. ")

		if(actual_run):
			try:
				with open(file_name) as f:
				    data 	= f.readlines()
				print("Elapsed time: " + str(time.time() - start))
				self.data 	= data
			except:
				print("Could not find ", file_name)
				self.data 	= []
		else:
			self.data 		= [[i for i in range(20)] for j in range(30)]

	# Splits the orders into a dictionary with dates as keys and orders of that day as values
	def split_data(self, data, date_range):
		
		index_of_dp 				= 18
		out_data 					= {}
		start_date					= date_range[0]
		end_date					= date_range[1]
		dates 						= self.date_strings_between(start_date, end_date)
		number_of_daily_orders		= len(data)
		
		# Create date keys in out_data dictionary
		for d in dates:
			out_data[d] 			= []

		# Loop through the data and add the order to its correct key (date)
		for i,line in enumerate(data):
			if(i % 10000 == 0):
				print("\t* Progression (splitting): ", int(float(i)/float(len(data))*1000)/100, " %")
			line_list 				= [i for i in line.split("\t")]
			try:
				date_str			= str(list(line_list)[index_of_dp][0:10])
				#print("Line", len(line), "Line list", len(line_list), "date_str", date_str)
				# If the line (order) is not a quarterly delivery product, we carry the order forward
				if(str(date_str[14:16]) not in [15, 30, 45]):
					try:
						out_data[date_str].append(line_list)
					except:
						print("KeyError line 59: ", date_str, " not in out_data (should be a date).")
			except:
				print("Something happened.")

		return out_data


	# Creates a file in the folder 'folder' for date 'date' containing all orders organized by their delivery products.
	# Two modes are supported: is_xlsx=True (creates one xlsx file per DP) and is_xlsx=False (creates one txt file per DP)
	def create_file(self, folder, date, orders, is_xlsx=False):
		book 								= xlsxwriter.Workbook(folder + "/" + date+".xlsx")						# .xlsx file in folder 'folder' with date as file name
		hours 								= [str(i) if i>9 else "0"+str(i) for i in range(0,24)]					# hours of the day. Sheet names
		dps 								= [(str(date) + " " + hh + ":00:00") for hh in hours]					# Delivery products
		out_data 							= {}																	# Dictionary containing all orders on a per-DP level
		index_of_dp							= 18																	# The index of the dp in the order list
			
		if(is_xlsx):
			# Create keys in dictionary (delivery products)
			for dp in dps:
				out_data[dp] 				= []

			# Loop through all orders and append order to its proper value list
			for i,order_str in enumerate(orders):
				if(i % 10000 == 0):
					print("\t* Progression (organizing): ", int(float(i)/float(len(data))*1000)/1000, " %")
				order_list = order_str
				try:
					out_data[order_list[index_of_dp][:19]].append(order_list)
				except:
					print("Key error: ", order_list[index_of_dp][:19])

			# For each dp, create a new sheet in the book and add all orders for that dp to the list
			for dp in (out_data.keys()):
				print("Creating sheet for: ", date, dp)
				sheet = book.add_worksheet(str(dp)[11:13])
				for i,order in enumerate(out_data[dp]):
					for j,attr in enumerate(order):
						sheet.write(i, j, attr)

			# Finishing the process
			book.close()
			print("File created for date ", date, ".")
		else:
			# Create keys in dictionary (delivery products)
			for dp in dps:
				out_data[dp]	= []

			# Loop through all orders and append order to its proper value list
			for i,order_str in enumerate(orders):
				if(i % 10000 	== 0):
					print("\t* Progression (organizing): ", int(float(i)/float(len(data))*1000)/100, " %")

				order_list 		= order_str
				
				try:
					out_data[order_list[index_of_dp][:19]].append(order_list)
				except:
					print("Key error: ", order_list[index_of_dp][:19])

			for dp in (out_data.keys()):
				with open("Data/" + dt.strftime(dt.strptime(dp, '%Y-%m-%d %H:%M:%S'), '%Y-%m-%d_%H-%M-%S') + ".txt", "w") as f:
					if(len(out_data[dp]) >= 1):
						string = ""
						for r in out_data[dp]:
							temp_string = ""
							for c in r:
								temp_string = temp_string + c + "\t" 
							string = string + temp_string + "\n"
						f.write(string)
						del string
					elif(len(out_data[dp]) == 1):
						#str_to_print = "\t".join(str(out_data[dp][0]))
						str_to_print = (str(out_data[dp][0]))
						string = ""
						for r in out_data[dp]:
							temp_string = ""
							for c in r:
								temp_string = temp_string + c + "\t" 
							string = string + temp_string + "\n"
						#print(string)
						f.write(string) 
			# Finishing the process
			book.close()
	
	def create_subfile(self, data, i):
		with open("Data/Data_"+str(i), "w") as f:
			len_data = len(data)
			string = ""
			if(len_data >= 1):
				for x, r in enumerate(data):
					print(float(int(1000*float(x)/float(len(data))))/10, "%")
					temp_string = ""
					for c in r:
						temp_string = temp_string 	+ c 			+ "\t" 
					string 			= string 		+ temp_string 	+ "\n"
				
				f.write(string)
				del string

			elif(len(data) == 1):
				
				str_to_print = (str(data[0]))				
				
				for r in data:
				
					temp_string = ""
					for c in r:
				
						temp_string = temp_string + c + "\t" 
				
					string = string + temp_string + "\n"
				
				f.write(string) 


	def organize(self):

		# Fetch and split data
		# Formally the correct way of doing it
		if(False):
			for key in self.orderbooks.keys():
				y = key[0]
				m = key[1]
				x = key[2]
				
				self.read_data(self.orderbooks[key], actual_run=True)
				#print(key, self.data)
				
				if(len(self.data) > 0):
					if(x == 0):
						date_range = [y + "-" + m + "-" + "01", y + "-" + m + "-" + str(self.days_of_months[int(m)])]
					elif(x == 1):
						date_range = [y + "-" + m + "-" + "01", y + "-" + m + "-15"]
					else:
						date_range = [y + "-" + m + "-" + "16", y + "-" + m + "-" + str(self.days_of_months[int(m)])]
					
					print("\n\n***** SPLITTING PHASE *****")
					splitted_data = self.split_data(self.data, date_range)
					#splitted_data = self.split_data(self.data[0:300], date_range)
					

					for i,day in enumerate(splitted_data.keys()):
						
						print(day, "Progression:", float(i)/len(splitted_data.keys()))
						self.create_file(y, day, splitted_data[day])
				
				#del self.data
				
			print("Total running time: ", time.time() - self.program_starting_time)
		else:
			for key in self.orderbooks.keys():
				print("Current key:", key)
				y = key[0]
				m = key[1]
				x = key[2]
				
				# Read data
				self.read_data(self.orderbooks[key], actual_run=True)
				
				# Split the data in more appropriate pieces
				len_data = len(self.data)					# Number of elements in the dataset
				sublength = int(0.1*len_data) 				# Size of each sublist
				print(len_data)
				print(sublength)
				print(10*sublength-1, len_data)

				print("Creating sublists")
				sublists = [[self.data[r*sublength+c] for c in range(sublength)] for r in range(10)]
				print("Sublists created! Creating subfiles")
				for i in range(len(sublists)):
					print("Create subfile", i)
					self.create_subfile(sublists[i],i*sublength)

				print("Subfiles created!")
				self.create_subfile(self.data[10*sublists:], "rest")



if __name__ == '__main__':
	# Create a Data_Organizer object and organize()
	organizer = Data_Organizer(generate_orderbook_urls=False)
	organizer.organize()

