# Status:
# Description: Bid handling and clearing is done in this file
# Related: Bid, Transaction

# External libraries
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

### ----------- System Parameters -----------
print_output 					= True
default_parameters 				= False
start_time 						= time.time()
folder 							= "Data/"
bid_file_tag 					= "dp12d1"
customer_bid_file_tag 			= "dp12d1cc"
	
time_lag 						= 0.0
length_of_timestep 				= 10 						# Number of minutes per timestep

# Documentation parameters
printing_mode 					= True
write_transactions_to_file 		= True
transaction_file_name 			= "transactions" + bid_file_tag + ".xlsx"


#printing_mode = False


### ----------- Model Parameters -----------
if (default_parameters == True):
  	# Set model parameters
  	params = 0
  	#number_of_trading_stages = 12

else:
	share 						= 1.0 # Share of input orderbook to be considered
	delivery_product 			= dt.strptime("2014-07-01 12:00:00", '%Y-%m-%d %H:%M:%S')
	time_from_gc_to_d 			= 30
	
	# Indices
	index_of_timestamp 			= 8
	index_of_isBuy 				= 9
	index_of_price 				= 11
	index_of_volume 			= 12
	index_of_order_id 			= 13
	index_of_delivery_product 	= 18
	
	
	# Trading window parameters
	trading_start_time 			= delivery_product - datetime.timedelta(days=1) + datetime.timedelta(hours=13) - datetime.timedelta(hours=delivery_product.hour, minutes=delivery_product.minute)
	trading_end_time 			= delivery_product - datetime.timedelta(minutes=time_from_gc_to_d)

# Data structure initialization
data = [] 										# All bids in /filename
bidsB = [] 										# List of buy bid objects of the delivery product described above
bidsS = [] 										# List of sell bid objects of the delivery product described above
trading_timeslots = [] 							# List of timeslots
open_buy_bids = [] 								# Iteratively updated
open_sell_bids = [] 							# Iteratively updated
transactions = [] 								# Iteratively updated
closed_buy_bids = []
closed_sell_bids = []
killed_bids = []


def print_elapsed_time(start_time):
	print("Time: " + str(int(10*time.time()-10*start_time)/10) + " seconds")
	start_time = time.time()

# Read data
def read_data(tag):
	print("Reading data...")
	filename = folder+tag+".txt"
	with open(filename) as f:
	    data = f.readlines()

	processed_data = []
	for b in data[:int(share*len(data))]:
		processed_data.append(re.split(r'\t+', b))

	del data
	print_elapsed_time(start_time)
	return processed_data

# Filter data
def filter_data(data, customer_bids=False):
	print("Filter data...")
	
	for bid_array in data:
		bid_array_dp = dt.strptime(bid_array[index_of_delivery_product].split(".")[0], '%Y-%m-%d %H:%M:%S')
		bid_array_timestamp = dt.strptime(bid_array[index_of_timestamp].split(".")[0], '%Y-%m-%d %H:%M:%S')
		if(bid_array_dp == delivery_product):
			if(bid_array[index_of_isBuy] == "1"):
				# 						Price 							Volume 							Timestamp 				isBuy 						Order ID 				Placed by Customer
				bidsB.append(Bid(float(bid_array[index_of_price]), float(bid_array[index_of_volume]), bid_array_timestamp, bid_array[index_of_isBuy], bid_array[index_of_order_id], customer_bids))
			else:
				bidsS.append(Bid(float(bid_array[index_of_price]), float(bid_array[index_of_volume]), bid_array_timestamp, bid_array[index_of_isBuy], bid_array[index_of_order_id], customer_bids))

	if(printing_mode):
		print("Number of buy bids: " + str(len(bidsB)))
		print("Number of sell bids: " + str(len(bidsS)))
		print("Total number of bids: " + str(len(bidsS)+(len(bidsB))))

	tot_vol = 0.0
	for i in range(len(bidsB)):
		tot_vol += bidsB[i].volume
	for i in range(len(bidsS)):
		tot_vol += bidsS[i].volume
	print("Total volume: " + str(tot_vol))
	print_elapsed_time(start_time)
	return bidsB, bidsS

# Setup timeslots
def setup_timeslots(start, end, length):
	timeslots = []
	time_iterator = start
	step_length = datetime.timedelta(minutes=length)

	while True:
		if(time_iterator + step_length > end + datetime.timedelta(minutes=length_of_timestep)):
			break
		else:
			timeslots.append(time_iterator)
			time_iterator = time_iterator + step_length
	if(printing_mode):
		print("Number of timeslots: " + str(len(timeslots)))
	return timeslots

def create_bid_dictionary(bids):
	bid_dictionary = {}
	if(printing_mode):
		print("Number of bids before dict: " + str(len(bids)))
	# Create keys
	bids_temp = bids[:]
	for timeslot in trading_timeslots:
		bid_dictionary[timeslot] = []
		for i,b in enumerate(bids_temp):
			#print(b.get_timestamp(), timeslot)
			if (b.get_timestamp() < timeslot):
				bid_dictionary[timeslot].append(b)
				#print(bid_dictionary[timeslot])
				bids_temp.remove(b)

			else:
				bids_temp = bids_temp[i:]
				break

	print("Number of bids after dict: " + str(len(bids_temp)))

	return bid_dictionary

# Support Functions
def retrieve_new_bids(open_sell_bids, open_buy_bids, timeslot):
	# To do: retrieve new bids
	#open_sell_bids.append(bid_dictionary[timeslot])
	#open_buy_bids.append(bid_dictionary[timeslot])
	return open_sell_bids, open_buy_bids

def create_transactions(open_sell_bids, open_buy_bids):
	# Sort by price
	open_sell_bids.sort(key=lambda x: x.price, reverse=True)
	open_buy_bids.sort(key=lambda x: x.price, reverse=True)

	# Not taken into account yet:
		# Assume bids only arrive in x min blocks
		# Do not reduce volume of the same bid in other zones
		# Who takes the spread
		# Only partial clearing

	for s in open_sell_bids:
		buy_bid_iterator = 0
		while s.volume > 0 and buy_bid_iterator < len(open_buy_bids):
			if(open_buy_bids[buy_bid_iterator].price >= s.price):
				if(printing_mode):
					print("Transaction created!")
				# Create transaction
				timestamp = s.timestamp if s.timestamp > open_buy_bids[buy_bid_iterator].timestamp else open_buy_bids[buy_bid_iterator].timestamp
				transaction_volume = min(s.volume, open_buy_bids[buy_bid_iterator].volume)
				transactions.append(Transaction(s, open_buy_bids[buy_bid_iterator], s.price, transaction_volume, timestamp))
				
				# Identify all bids with same order id
				identical_sell_bids = []
				identical_buy_bids = []

				# Reduce their volumes and remove them if no residual volume
				for t in open_sell_bids:
					if(t.order_id == s.order_id):
						t.reduce_volume(transaction_volume)
						if(t.volume == 0):
							open_sell_bids.remove(t)
							closed_sell_bids.append(t)

				for t in open_buy_bids:
					if(open_buy_bids[buy_bid_iterator].order_id == t.order_id):
						t.reduce_volume(transaction_volume)
						
						if(t.volume == 0):
							open_buy_bids.remove(t)
							closed_buy_bids.append(t)
							# Necessary to decrease iterator?
							#buy_bid_iterator -= 1

				# Increase iterator
				buy_bid_iterator = buy_bid_iterator + 1
			else:
				break






	# To do: create transactions
	return open_sell_bids, open_buy_bids

def print_bid_curves(buy_bids, sell_bids):

	return 0
	x = np.arange(1, 7, 0.4)
	y0 = np.sin(x)
	y = y0.copy() + 2.5

	plt.step(x, y, label='pre (default)')

	y -= 0.5
	plt.step(x, y, where='mid', label='mid')

	y -= 0.5
	plt.step(x, y, where='post', label='post')

	y = ma.masked_where((y0 > -0.15) & (y0 < 0.15), y - 0.5)
	plt.step(x, y, label='masked (pre)')

	plt.legend()

	plt.xlim(0, 7)
	plt.ylim(-0.5, 4)

	plt.show()

def remove_killed_bids(bids):
	# For each bid with zero volume, remove all other bids with identical order id
	zero_volume_bids = []
	for b in bids:
		if(b.volume == 0):
			zero_volume_bids.append(b)

	for k in zero_volume_bids:
		for b in bids:
			if(k.order_id == b.order_id):
				killed_bids.append(k)
				bids.remove(b)


	return bids


def print_transactions(mode):
	for t in transactions:
		if(mode=="price"):
			print(t.price/100)
		else:
			print("Transaction created " + str(t.timestamp) + " with price: " + str(t.price) + ", transaction volume: " + str(t.volume))

def write_transactions():
	book = xlsxwriter.Workbook(transaction_file_name)
	sheet = book.add_worksheet("Transactions")

	# Fill spreadsheet
	headers = ["Timestamp", "Price", "Volume", "Buy bid", "Sell bid"]
	for h in range(len(headers)):
		sheet.write(0, h, headers[h])

	for i in range(len(transactions)):
		sheet.write(i+1, 0, transactions[i].timestamp)
		sheet.write(i+1, 1, transactions[i].price)
		sheet.write(i+1, 2, transactions[i].volume)
		#sheet.write(i+1, 3, transactions[i].buy_bid)
		#sheet.write(i+1, 4, transactions[i].sell_bid)
		
	book.close()


def initandrun():
	# Data structure instantiation
	trading_timeslots = setup_timeslots(trading_start_time, trading_end_time, length_of_timestep)  # List of timeslots
	data = read_data(bid_file_tag)
	customer_bid_array = read_data(customer_bid_file_tag)

	# Create lists of bids
	buy_bids, sell_bids = filter_data(data, False)
	_, customer_bids = filter_data(customer_bid_array, True)

	# Add customer's bids to list of bids
	sell_bids += customer_bids

	# Sort bids by their time blocks
	sell_bid_dict = create_bid_dictionary(sell_bids)
	buy_bid_dict = create_bid_dictionary(buy_bids)

	for t, timeslot in enumerate(trading_timeslots):
		if(printing_mode):
			print("\n\n\nTimeslot: " + str(timeslot) + " (Iteration " + str(t) + ")")
		# Retrieve all incoming bids
		#print("\n Number of incoming bids: " + str(len(sell_bid_dict[timeslot])))
		open_sell_bids += sell_bid_dict[timeslot]
		open_buy_bids += buy_bid_dict[timeslot]

		open_sell_bids = remove_killed_bids(open_sell_bids)
		open_buy_bids = remove_killed_bids(open_buy_bids)

		#print("Number of open bids (sell, buy): " + str(len(open_sell_bids)) + ", " + str(len(open_buy_bids)))
		print_bid_curves(open_buy_bids, open_sell_bids)
		#print_elapsed_time(start_time)
		
		# Create transactions and update bid portfolio
		open_sell_bids, open_buy_bids = create_transactions(open_sell_bids, open_buy_bids)

		# Print bid curves
		print_bid_curves(open_buy_bids, open_sell_bids)
		if(printing_mode):
			print("Timestep " + str(timeslot) + " bid order depth " + str(len(closed_sell_bids))+"/"+str(len(open_sell_bids)) +"-"+ str(len(closed_buy_bids))+"/"+str(len(open_buy_bids)))

		time.sleep(time_lag)

def get_customer_transactions():
	customer_transactions = []
	for t in transactions:
		if(t.sell_bid.isCustomer == True):
			customer_transactions.append(t)

	return customer_transactions


	
def main():
	initandrun()
	

	if(printing_mode):
		print("Number of transactions: " + str(len(transactions)))
		print_transactions(mode="price")
	if(write_transactions_to_file):
		write_transactions()
	print("\nDone\n\n")
if __name__ == "__main__":
    main()
