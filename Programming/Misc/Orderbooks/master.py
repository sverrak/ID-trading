# Status: 
# Description: 
# Related components: market.py, itp.py

# External packages
import time
from market_oo import Market
import bid
import transaction
import numpy as np
from numpy import ma
import matplotlib.pyplot as plt
import time
from datetime import datetime as dt
import datetime
import re
from bid import Bid
from transaction import Transaction
import xlsxwriter


class Market_Runner(object):
	"""docstring for MarketRunner"""
	def __init__(self, dates):
		super(Market_Runner, self).__init__()
		
		# Model parameters
		self.dates = dates
		self.stats_file_name = "Orderbook_stats_time_range_" + self.dates[0] + "_-_" + self.dates[-1]
		self.stats = []





		# Datastructures
		cost_levels = []
		self.dps	= []

		self.initiate_dps()

	def run_one_market(self, dp):
		# Initiate and run market
		bid_file_tag 	= dt.strftime(dp, "%Y-%m-%d")
		market 			= Market(dp, bid_file_tag, "N/A", printing_mode=True)
		market.initandrun()

		# Collect the stats
		self.stats.append(market.get_stats())


	def run_multiple_markets(self):
		for dp in self.dps:
			self.run_one_market(dp)



	# Call itp
	def learn_strategy(self, mode=None):

		return 0

	# Call market
	def evaluate_strategy(self, strategy, mode=None):
		for dp in self.dp_timeslots:
			# Run a market sequence with all the bids of the corresponding dp
			strdp 					= str(dp) if dp>9 else "0"+str(dp)
			bid_file_tag 			= "dp" + strdp + "d1"
			customer_bid_file_tag 	= "dp" + strdp + "d1cc"
			delivery_product 		= initiate_dps([dp])[0]
			market 					= Market(delivery_product, bid_file_tag, customer_bid_file_tag, printing_mode=True)
			market.main()
			


		return 0

	def initiate_dps(self):
		timeslots 		= [str(i) if i>9 else "0"+str(i) for i in range(24)]
		for date in self.dates:
			for hh in timeslots:
				self.dps.append(dt.strptime(date + " " + str(hh) + ":00:00", '%Y-%m-%d %H:%M:%S'))


	def write_3d_matrix_to_file(self, stats):
		book = xlsxwriter.Workbook(self.stats_file_name)
		
		for dp in range(len(stats)):
			# Create DP specific spreadsheet
			sheet = book.add_worksheet(dp)

			# Fill spreadsheet
			for t in range(len(self.stats)):
				for i in range(len(self.transactions[t])):
					sheet.write(t, i, self.stats[t][i])

		book.close()


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
	if(actual_run):
		with open(file_name) as f:
		    data = f.readlines()

		return data
	else:
		return [[i for i in range(20)] for j in range(30)]


def split_data(data, date_range):
	
	index_of_dp 				= 18
	out_data 					= {}
	start_date					= date_range[0]
	end_date					= date_range[1]
	dates 						= date_strings_between(start_date, end_date)
	
	# Create date keys in out_data dictionary
	for d in dates:
		out_data[d] 			= []

	# Loop through the data and add the order to its correct key (date)
	for line in data:
		line_list 				= line.split("\t")
		date_str				= str(line[index_of_dp][:10])	
		out_data[date_str].append(line_list)

	return out_data

def create_file(date, orders):
	book 								= xlsxwriter.Workbook(self.variables_file_name)
	hours 								= [str(i) if i>9 else "0"+str(i) for i in range(0,24)]
	dps 								= [str(date) + " " + hh + ":00:00" for hh in hours]
	out_data 							= {}

	# Create keys in dictionary (delivery products)
	for dp in dps:
		out_data[dp] 					= []

	# Loop through all orders and append order to its proper value list
	for i,order_str in enumerate(out_data[dp]):
		order_list = order_str.split("\t")
		out_data[order_list[index_of_dp]].append(order_list)

	# For each dp, create a new sheet in the book and add all orders for that dp to the list
	for dp in enumerate(out_data.keys()):
		sheet = book.add_worksheet(str(dp))
		for i,order in enumerate(out_data[dp]):
			for j,attr in enumerate(order):
				sheet.write(i, j, attr)


	

if __name__ == '__main__':
	# Organize data
	if(True):
		years 							= ["2014","2015","2016","2017"]
		months 							= [str(i) if i>9 else "0"+str(i) for i in range(1,13)]
		days_of_months					= [0,31,28,31,30,31,30,31,31,30,31,30,31]
		orderbooks 						= {}

		# Generate orderbook URLs
		for y in years:
			for i,m in enumerate(months):
				if(int(y) < 2016 and int(m) < 10):
					orderbooks[(y,m,0)] = ("ComXervOrderbooks_" + y + "_" + m + ".txt")
				else:
					orderbooks[(y,m,1)] = ("ComXervOrderbooks_" + y + "_" + m + "_01-" + y + "_" + m + "_15"".txt")
					orderbooks[(y,m,2)] = ("ComXervOrderbooks_" + y + "_" + m + "_16-" + y + "_" + m + "_"+str(days_of_months[int(m)])+".txt")

		# Fetch and split data
		for key in orderbooks.keys():
			y = key[0]
			m = key[1]
			x = key[2]
			
			data = read_data(orderbooks[key])
			if(x == 0):
				date_range = [y + "-" + m + "-" + "01", y + "-" + m + "-" + str(days_of_months[int(m)])]
			elif(x == 1):
				date_range = [y + "-" + m + "-" + "01", y + "-" + m + "-15"]
			else:
				date_range = [y + "-" + m + "-" + "16", y + "-" + m + "-" + str(days_of_months[int(m)])]
			
			splitted_data = split_data(data, date_range)

			for day in splitted_data.keys():
				create_file(day, splitted_data[day])













		mr 						= Market_Runner()



	# Strategy evaluation mode
	if(False):
		mr 						= Market_Runner()
		mr.delivery_products 	= mr.initiate_dps(dp_timeslots)

		strategy 				= mr.learn_strategy()
		
		mr.evaluate_strategy(strategy)

	# Delivery Product Statistics Mode
	if(False):
		mr 						= Market_Runner(["2014-07-01", "2014-07-02"])

		mr.run_multiple_markets()
		stats 					= []

		
		# Save the stats to a file
		#mr.write_3d_matrix_to_file(stats)



