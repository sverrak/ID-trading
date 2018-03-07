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
    def __init__(self, date_range, testing_mode=False):
        super(Market_Runner, self).__init__()

        # Model parameters
        self.date_range                         	= date_range
        self.dates_str, self.dates                 	= self.date_strings_between(self.date_range[0], self.date_range[1])
        self.timeslots                             	= [str(i) if i>9 else "0"+str(i) for i in range(24)]
        self.stats_file_names                     	= ["Output/Orderbook_stats_time_range_" + d + ".xlsx" for d in self.dates_str]
        self.stats                                 	= []

        # Datastructures
        self.dps                       		        = []
        self.initiate_dps(testing_mode)


    def date_strings_between(self, start,end):
        start_date             						= dt.strptime(start + " " + "00-00-00", '%Y-%m-%d %H-%M-%S')
        end_date     								= dt.strptime(end     + " " + "00-00-00", '%Y-%m-%d %H-%M-%S')
        dates_between_str    						= []
        dates_between         						= []
        current_date        						= start_date
        
        while(current_date    <= end_date):
            dates_between_str.append(dt.strftime(current_date,'%Y-%m-%d'))
            dates_between.append(current_date)
            current_date     = current_date + datetime.timedelta(days=1)

        return dates_between_str, dates_between


    def run_one_market(self, dp):
        # Initiate and run market
        bid_file_tag             					= dt.strftime(dp, "%Y-%m-%d_%H-%M-%S")
        market                     					= Market(dp, bid_file_tag, "N/A", printing_mode=False)
        market.initandrun()

        # Collect the stats
        self.stats.append(market.get_stats())


    def run_multiple_markets(self):
        for dp in self.dps:
            print("Currently examining the market of DP ", dt.strftime(dp,'%Y-%m-%d %H:%M:%S'))
            self.run_one_market(dp)
    
    # Call itp
    def learn_strategy(self, mode=None):
        return 0

    # Call market
    def evaluate_strategy(self, strategy, mode=None):
        for dp in self.dp_timeslots:
            # Run a market sequence with all the bids of the corresponding dp
            strdp                     				= str(dp) if dp>9 else "0"+str(dp)
            bid_file_tag             				= "dp" + strdp + "d1"
            customer_bid_file_tag     				= "dp" + strdp + "d1cc"
            delivery_product         				= self.initiate_dps([dp])[0]
            market                     				= Market(delivery_product, bid_file_tag, customer_bid_file_tag, printing_mode=False)
            market.main()

        return 0
    
    def write_3d_matrix_to_file(self, stats):
        for y,d in enumerate(self.dates_str):
            print("Printing file " + str(y) + "/" + str(len(self.dates_str)))
            book = xlsxwriter.Workbook(self.stats_file_names[y])
            
            for x,dp in enumerate(self.dps):
                dp_str = dt.strftime(dp, '%Y-%m-%d %H-%M-%S')
                # Create DP specific spreadsheet
                sheet = book.add_worksheet(dp_str)

                # Fill spreadsheet
                for t in range(len(self.stats[x])):
                    for i in range(len(self.stats[x][t])):
                        sheet.write(t, i, self.stats[x][t][i])

            book.close()
            del book

    def initiate_dps(self, testing_mode):
        if(testing_mode==True):
            self.dps = [dt.strptime("2016-09-1 13-00-00", '%Y-%m-%d %H-%M-%S')]
        else:
            for date in self.dates_str:
                for hh in self.timeslots:
                    self.dps.append(dt.strptime(date + " " + str(hh) + "-00-00", '%Y-%m-%d %H-%M-%S'))

    


if __name__ == '__main__':
    running_mode = 2
    # Strategy evaluation mode
    if(running_mode == 1):
        mr                             			= Market_Runner()
        strategy                     			= mr.learn_strategy()
        mr.evaluate_strategy(strategy)

    # Delivery Product Statistics Mode
    if(running_mode == 2):
        mr                          			= Market_Runner(["2016-09-01", "2016-09-15"], testing_mode=False)
        mr.run_multiple_markets()
        # Save the stats to a file
        mr.write_3d_matrix_to_file(mr.stats)



