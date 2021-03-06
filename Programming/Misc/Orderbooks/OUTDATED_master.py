# Status: # Status: OUTDATED (Name of new file: dp_to_stats.py)
# Description: 
# Related components: market.py, itp.py

# External packages
import time
from market_v2 import Market
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
from multiprocessing import Pool as ThreadPool

class Market_Runner(object):
    """docstring for MarketRunner"""
    def __init__(self, date_range, testing_mode=False):
        super(Market_Runner, self).__init__()

        # Model parameters
        self.date_range                             = date_range
        self.dates_str, self.dates                     = self.date_strings_between(self.date_range[0], self.date_range[1])
        self.timeslots                                 = [str(i) if i>9 else "0"+str(i) for i in range(24)]
        self.stats_file_names                         = ["Output/DynamicStep/Orderbook_stats_time_range_" + d + ".xlsx" for d in self.dates_str]
        self.stats                                     = []

        # Datastructures
        self.dps                                       = []
        self.initiate_dps(testing_mode)


    def date_strings_between(self, start, end):
        start_date                                     = dt.strptime(start + " " + "00-00-00", '%Y-%m-%d %H-%M-%S')
        end_date                                     = dt.strptime(end     + " " + "00-00-00", '%Y-%m-%d %H-%M-%S')
        dates_between_str                            = []
        dates_between                                 = []
        current_date                                = start_date
        
        while(current_date    <= end_date):
            dates_between_str.append(dt.strftime(current_date,'%Y-%m-%d'))
            dates_between.append(current_date)
            current_date     = current_date + datetime.timedelta(days=1)

        return dates_between_str, dates_between


    def run_one_market(self, dp):
        # Initiate and run market
        
        bid_file_tag                                 = dt.strftime(dp, "%Y-%m-%d_%H-%M-%S")
        market                                         = Market(dp, bid_file_tag, "N/A", timeslot_length=10, printing_mode=False)
        market.alternative_runner()
        
        # Collect the stats
        self.stats.append(market.get_stats())
        
        # Clean up
        del market
        return dp

    def run_multiple_markets(self):
        parallell = False
        if(parallell):
            for i in range(int(len(self.dps)/24)):
                current_date = str(dt.strftime(self.dps[i], "%Y-%m-%d"))
                current_dps = self.dps[24*i:24*i+24]
                pool = ThreadPool(24)
                results = pool.map(self.run_one_market, self.dps[i:i+24])
                self.write_stats(current_dps, i, current_date)
                
                
           
           
           

        else:
            print("Currently examining:",str(self.date_range[0]))
            date_counter = 0
            current_dps = []
            for dp in self.dps:
                #print("\tCurrent dp:", dp)
                #print(str(dt.strftime(dp, "%Y-%m-%d")) == str(self.date_range[0]))
                if(int(dp.hour) == 0 and not str(dt.strftime(dp, "%Y-%m-%d")) == str(self.date_range[0])):
                    current_date = str(dt.strftime(dp, "%Y-%m-%d"))
                    print("Currently examining:",current_date)
                    
                    self.write_stats(current_dps, date_counter, current_date)
                    del self.stats
                    self.stats = []
                    current_dps = []
                    date_counter += 1
                self.run_one_market(dp)
                current_dps.append(dp)
    
    # Call itp
    def learn_strategy(self, mode=None):
        return 0

    # Call market
    def evaluate_strategy(self, strategy, mode=None):
        for dp in self.dp_timeslots:
            # Run a market sequence with all the bids of the corresponding dp
            strdp                                     = str(dp) if dp>9 else "0"+str(dp)
            bid_file_tag                             = "dp" + strdp + "d1"
            customer_bid_file_tag                     = "dp" + strdp + "d1cc"
            delivery_product                         = self.initiate_dps([dp])[0]
            market                                     = Market(delivery_product, bid_file_tag, customer_bid_file_tag, printing_mode=False)
            market.main()

        return 0
    
    def write_stats(self, current_dps, date_counter, date):
        print("Creating file", date)
        book = xlsxwriter.Workbook(self.stats_file_names[date_counter])
        date_format = book.add_format({'num_format': 'dd/mm/yy hh:mm'})
        
        for x,dp in enumerate(current_dps):
            dp_str = dt.strftime(dp, '%Y-%m-%d %H-%M-%S')
            # Create DP specific spreadsheet
            sheet = book.add_worksheet(dp_str)
            
            # Fill spreadsheet
            for t in range(len(self.stats[x])):
                for i in range(len(self.stats[x][t])):
                    if(i == 0):
                        try:
                            sheet.write(t, i, self.stats[x][t][i], date_format)
                        except:
                            sheet.write(t, i, self.stats[x][t][i])
                    else:
                        sheet.write(t, i, self.stats[x][t][i])
                        
        book.close()
        del book
        print("File created.\n")
    
    def write_3d_matrix_to_file(self, stats):
        for y,d in enumerate(self.dates_str):
            print("Printing file " + str(y+1) + "/" + str(len(self.dates_str)))
            book = xlsxwriter.Workbook(self.stats_file_names[y])
            date_format = book.add_format({'num_format': 'dd/mm/yy hh:mm'})
            
            for x,dp in enumerate(self.dps):
                dp_str = dt.strftime(dp, '%Y-%m-%d %H-%M-%S')
                # Create DP specific spreadsheet
                sheet = book.add_worksheet(dp_str)
                
                # Fill spreadsheet
                for t in range(len(self.stats[x])):
                    for i in range(len(self.stats[x][t])):
                        if(i == 0):
                            try:
                                sheet.write(t, i, self.stats[x][t][i], date_format)
                            except:
                                sheet.write(t, i, self.stats[x][t][i])
                        else:
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
    running_mode = 3
    data = 9
    # Strategy evaluation mode
    if(running_mode == 1):
        mr                                         = Market_Runner()
        strategy                                 = mr.learn_strategy()
        mr.evaluate_strategy(strategy)

    # Delivery Product Statistics Mode
    if(running_mode == 2):
        start = time.time()
        if(data == 2):
            mr                             = Market_Runner(["2017-02-16", "2017-02-28"], testing_mode=False)
        else:
            mr                             = Market_Runner(["2017-01-01", "2017-02-28"], testing_mode=False)
            mr.run_multiple_markets()
            
            try:
                mr                             = Market_Runner(["2017-01-01", "2017-02-28"], testing_mode=False)
            except:
                a = 0
        try:
            mr.run_multiple_markets()
            # Save the stats to a file
            mr.write_3d_matrix_to_file(mr.stats)
            
            del mr
        except:
            a=0
    if(running_mode == 3):
        try:
            mr = Market_Runner(["2017-02-28", "2017-02-28"], testing_mode=False)
            
            mr.run_multiple_markets()
            # Save the stats to a file
            
            mr.write_3d_matrix_to_file(mr.stats)
        except:
            a = 0
        
            
        print("Elapsed time:", time.time() - start)
        
        



