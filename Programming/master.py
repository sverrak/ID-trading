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


# Model parameters
date = "2014-07-01"
dp_timeslots = [0]





# Datastructures
cost_levels = []
delivery_products = []


# Call itp
def learn_strategy(mode=None):
	return 0

# Call market
def evaluate_strategy(strategy, mode=None):
	print("Got here")
	for dp in dp_timeslots:
		# Run a market sequence with all the bids of the corresponding dp
		strdp = str(dp) if dp>9 else "0"+str(dp)
		bid_file_tag = "dp" + strdp + "d1"
		customer_bid_file_tag = "dp" + strdp + "d1cc"
		delivery_product = initiate_dps([dp])[0]
		market = Market(delivery_product, bid_file_tag, customer_bid_file_tag, printing_mode=True)
		print(market)
		market.main()
		#market.initandrun()


	return 0

def initiate_dps(timeslots):
	dp_timeslots = []
	for dp in timeslots:
		dp_timeslots.append(dt.strptime(date + " " + str(dp) + ":00:00", '%Y-%m-%d %H:%M:%S'))

	return dp_timeslots



def main():
	delivery_products = initiate_dps(dp_timeslots)

	strategy = learn_strategy()
	evaluate_strategy(strategy)

if __name__ == '__main__':
	main()
