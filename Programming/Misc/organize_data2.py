# External packages
from datetime import datetime as dt
import xlsxwriter
import datetime
import time

### Support functions
def date_strings_between(start,end):
	start_date 			= dt.strptime(start + " " + "00:00:00", '%Y-%m-%d %H:%M:%S')
	end_date 			= dt.strptime(end + " " + "00:00:00", '%Y-%m-%d %H:%M:%S')
	dates_between_str	= []
	current_date		= start_date
	
	while(current_date	<= end_date):
		dates_between_str.append(dt.strftime(current_date,'%Y-%m-%d'))
		current_date = current_date + datetime.timedelta(days=1)

	return dates_between_str


def read_data(file_name, actual_run=False):
	start = time.time()
	print("Reading data. ")

	if(actual_run):
		try:
			with open(file_name) as f:
			    data = f.readlines()
			print("Elapsed time: " + str(time.time() - start))
			return data
		except:
			print("Could not find ", file_name)
			return []
	else:
		return [[i for i in range(20)] for j in range(30)]


def split_data(data, date_range):
	
	index_of_dp 				= 18
	out_data 					= {}
	start_date					= date_range[0]
	end_date					= date_range[1]
	dates 						= date_strings_between(start_date, end_date)
	number_of_daily_orders		= len(data)
	
	# Create date keys in out_data dictionary
	for d in dates:
		out_data[d] 			= []

	# Loop through the data and add the order to its correct key (date)
	for i,line in enumerate(data):
		if(i % 1000 == 0):
			print("\t* Progression (splitting): ", int(float(i)/float(len(data))*1000)/100, " %")
		line_list 				= [i for i in line.split("\t")]
		
		date_str				= str(list(line_list)[index_of_dp][0:10])
		print("Line", len(line), "Line list", len(line_list), "date_str", date_str)
		# If the line (order) is not a quarterly delivery product, we carry the order forward
		if(str(date_str[14:16]) not in [15, 30, 45]):
			try:
				out_data[date_str].append(line_list)
			except:
				print("KeyError line 59: ", date_str, " not in out_data (should be a date).")

	return out_data


# Creates a file in the folder 'folder' for date 'date' containing all orders organized by their delivery products.
def create_file(folder, date, orders):
	book 								= xlsxwriter.Workbook(folder + "/" + date+".xlsx")						# .xlsx file in folder 'folder' with date as file name
	hours 								= [str(i) if i>9 else "0"+str(i) for i in range(0,24)]					# hours of the day. Sheet names
	dps 								= [(str(date) + " " + hh + ":00:00") for hh in hours]					# Delivery products
	out_data 							= {}																	# Dictionary containing all orders on a per-DP level
	index_of_dp							= 18																	# The index of the dp in the order list
	
	# Create keys in dictionary (delivery products)
	for dp in dps:
		out_data[dp] 					= []

	# Loop through all orders and append order to its proper value list
	for i,order_str in enumerate(orders):
		if(i % 1000 == 0):
			print("\t* Progression (organizing): ", int(float(i)/float(len(data))*1000)/100, " %")
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

# Organize data
if __name__ == '__main__':
	program_starting_time = time.time()
	
	# Datastructure instantiation. Used to 
	years 							= ["2014","2015","2016","2017"]
	months 							= [str(i) if i>9 else "0"+str(i) for i in range(1,13)]
	days_of_months					= [0,31,28,31,30,31,30,31,31,30,31,30,31]
	orderbooks 						= {}

	# Generate orderbook URLs
	if(True):
		for y in years:
			for i,m in enumerate(months):
				if(int(y) < 2016 and int(m) < 10):
					orderbooks[(y,m,0)] = ("ComXervOrderbooks_" + y + "_" + m + ".txt")
				else:
					orderbooks[(y,m,1)] = ("ComXervOrderbooks_" + y + "_" + m + "_01-" + y + "_" + m + "_15"".txt")
					orderbooks[(y,m,2)] = ("ComXervOrderbooks_" + y + "_" + m + "_16-" + y + "_" + m + "_"+str(days_of_months[int(m)])+".txt")
	# Custom orderbook URL
	else:
		orderbooks[("2014","02",0)] = "ComXervOrderbooks_2014_02.txt"
	
	# Fetch and split data
	for key in orderbooks.keys():
		y = key[0]
		m = key[1]
		x = key[2]
		
		data = read_data(orderbooks[key], actual_run=True)
		
		if(len(data) > 0):
			if(x == 0):
				date_range = [y + "-" + m + "-" + "01", y + "-" + m + "-" + str(days_of_months[int(m)])]
			elif(x == 1):
				date_range = [y + "-" + m + "-" + "01", y + "-" + m + "-15"]
			else:
				date_range = [y + "-" + m + "-" + "16", y + "-" + m + "-" + str(days_of_months[int(m)])]
			
			print("\n\n***** SPLITTING PHASE *****")
			splitted_data = split_data(data, date_range)
			

			for i,day in enumerate(splitted_data.keys()):
				print(day, "Progression:", float(i)/len(splitted_data.keys()))
				create_file(y, day, splitted_data[day])
		
	print("Total running time: ", time.time() - program_starting_time)
