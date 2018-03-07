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
        self.stats_file_name = "Orderbook_stats_time_range_" + self.dates[0] + "_-_" + self.dates[-1] # ???
        self.stats = []

        # Datastructures
        self.dps    = []

        self.initiate_dps(False)

    def run_one_market(self, dp):
        # Initiate and run market
        bid_file_tag     = dt.strftime(dp, "%Y-%m-%d_%H-%M-%S")
        market             = Market(dp, bid_file_tag, "N/A", printing_mode=True)
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
            strdp                     = str(dp) if dp>9 else "0"+str(dp)
            bid_file_tag             = "dp" + strdp + "d1"
            customer_bid_file_tag     = "dp" + strdp + "d1cc"
            delivery_product         = self.initiate_dps([dp])[0]
            market                     = Market(delivery_product, bid_file_tag, customer_bid_file_tag, printing_mode=True)
            market.main()
            


        return 0

    def initiate_dps(self, testing_mode=True):
        if(testing_mode==True):
            self.dps = ["2016-09-01_00-00-00", "2016-09-01_01-00-00", "2016-09-01_02-00-00"]
        timeslots         = [str(i) if i>9 else "0"+str(i) for i in range(24)]
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
    
if __name__ == '__main__':
    
    # Strategy evaluation mode
    if(False):
        mr                         = Market_Runner()
        strategy                 = mr.learn_strategy()
        
        mr.evaluate_strategy(strategy)

    # Delivery Product Statistics Mode
    if(True):
        mr                         = Market_Runner(["2016-09-01", "2016-09-02"])
        
        mr.run_multiple_markets()
        stats                     = []

        
        # Save the stats to a file
        #mr.write_3d_matrix_to_file(stats)



