# Description: This class aggregates data from the DP transaction time series and outputs data for timeseries analysis

# -*- coding: utf-8 -*-
"""
Created on Wed Apr  4 09:15:18 2018

@author: sverrak
"""

from datetime import datetime as dt
import datetime
import numpy
import xlsxwriter
from TimeslotState import TimeslotState
from TransitionMatrixIOHandler import TransitionMatrixIOHandler
from multiprocessing import Pool as ThreadPool
import multiprocessing

class TimeseriesAnalyzer(object):
    """docstring for MarketRunner"""
    def __init__(self, date_range, directory = "", start_dp = 11, end_dp = 16):
        super(TimeseriesAnalyzer, self).__init__()
        
        self.printing_mode = True
        
        self.start_dp = start_dp
        self.end_dp = end_dp
        self.date_range                             = date_range
        self.dates_str, self.dates                     = self.date_strings_between(self.date_range[0], self.date_range[1])
        
        self.timeslots                                 = [str(i) if i>9 else "0"+str(i) for i in range(self.start_dp, self.end_dp)]
        #self.timeslots                                 = [str(4*i) if 4*i>9 else "0"+str(4*i) for i in range(6)]
        #print(self.timeslots)
        self.directories = [directory + "ConstantStep5/Orderbook_stats_time_range_" + date_str + ".xlsx" for date_str in self.dates_str]
        self.data = []
        self.headers = []
        
        self.results = []
        
        self.io = TransitionMatrixIOHandler()
        
    def analyze(self, mode=None):
        try:
            del self.data
            del self.headers
            del self.results
            
            self.data = []
            self.headers = []
            self.results = []
        except:
            pass
        for dp, dp_str in enumerate(self.timeslots):
            
            if(self.printing_mode == True):
                print(mode,"Currently examining delivery product",dp_str)
            filtered_data = []
            
            for d, date_str in enumerate(self.dates_str):
                print(date_str)    
                current_timeseries, header = self.io.read_file(input_file=self.directories[d],analysis_mode=True, return_headers=True,error_value="#N/A")
                
                # Depending on the chosen mode, store the desired data
                
                if(mode == "Spread"):
                    filtered_data.append([ts.spread for ts in current_timeseries[self.start_dp + dp]])
                elif(mode == "Buy volume"):
                    filtered_data.append([ts.buy_order_depth for ts in current_timeseries[self.start_dp + dp]])
                elif(mode == "Sell volume"):
                    filtered_data.append([ts.sell_order_depth for ts in current_timeseries[self.start_dp + dp]])
                elif(mode == "Price"):
                    filtered_data.append([ts.base_price for ts in current_timeseries[self.start_dp + 4*dp]])
                elif(mode == "Sell price"):
                    filtered_data.append([ts.best_sell_price for ts in current_timeseries[self.start_dp + dp]])
                elif(mode == "Buy price"):
                    filtered_data.append([ts.best_buy_price for ts in current_timeseries[self.start_dp + dp]])
                elif(mode[:2] == "BV"):
                    filtered_data.append([ts.bvs[int(mode[-1])] for ts in current_timeseries[self.start_dp + dp]])
                elif(mode[:2] == "BP"):
                    filtered_data.append([ts.bps[int(mode[-1])] for ts in current_timeseries[self.start_dp + dp]])
                elif(mode[:2] == "SV"):
                    filtered_data.append([ts.svs[int(mode[-1])] for ts in current_timeseries[self.start_dp + dp]])
                elif(mode[:2] == "SP"):
                    filtered_data.append([ts.sps[int(mode[-1])] for ts in current_timeseries[self.start_dp + dp]])
                    
                else:
                    raise ValueError("Mode", mode, "not supported")
            self.data.append(filtered_data)
            
            self.headers.append(header[int(dp_str)])
        
    def analyze_parallel(self, mode=None):
        # Setup threads
        pool = ThreadPool(len(self.timeslots))
        
        # Setup input
        xrange = [i for i in range(len(self.timeslots))]
        
        # Do threading
        pool.map(self.analyze_helper, xrange, self.timeslots)
        
    def analyze_helper(self, dp, dp_str, mode=None):
        
        filtered_data = []
        for d, date_str in enumerate(self.dates_str):
            current_timeseries, header = self.io.read_file(input_file=self.directories[d],analysis_mode=True, return_headers=True)
            
            # Depending on the chosen mode, store the desired data
            if(mode == "Spread"):
                filtered_data.append([ts.spread for ts in current_timeseries[dp]])
            elif(mode == "Buy volume"):
                filtered_data.append([ts.buy_order_depth for ts in current_timeseries[dp]])
            elif(mode == "Sell volume"):
                filtered_data.append([ts.sell_order_depth for ts in current_timeseries[dp]])
            elif(mode == "Price"):
                filtered_data.append([ts.base_price for ts in current_timeseries[dp]])
            elif(mode == "Sell price"):
                filtered_data.append([ts.best_sell_price for ts in current_timeseries[dp]])
            elif(mode == "Buy price"):
                filtered_data.append([ts.best_buy_price for ts in current_timeseries[dp]])
                
        self.data.append(filtered_data)
        self.headers.append(header)

    
    def date_strings_between(self, start, end):
        start_date                                   = dt.strptime(start + " " + "00-00-00", '%Y-%m-%d %H-%M-%S')
        end_date                                     = dt.strptime(end     + " " + "00-00-00", '%Y-%m-%d %H-%M-%S')
        dates_between_str                            = []
        dates_between                                = []
        current_date                                 = start_date
        
        while(current_date <= end_date):
            dates_between_str.append(dt.strftime(current_date,'%Y-%m-%d'))
            dates_between.append(current_date)
            current_date     = current_date + datetime.timedelta(days=1)

        return dates_between_str, dates_between
    
    def write_data_to_file(self, file_name):
        book = xlsxwriter.Workbook(file_name)
        date_format = book.add_format({'num_format': 'dd/mm/yy hh:mm'})
        
        for x,hour in enumerate(self.timeslots):
            # Create DP specific spreadsheet
            sheet = book.add_worksheet(hour)
            
            
            for y, timeslot in enumerate(self.headers[x]):
                
                sheet.write(0, y, timeslot, date_format)
            
            # Fill spreadsheet
            for i,row in enumerate(self.data[x]):
                #print(hour, timeslot, row[0].value)
                sheet.write(i+1,0,self.dates_str[i])
                for j,col in enumerate(row):
                    sheet.write(i+1,j+1,col)
                    
        book.close()
        #del book
        print("File created.\n")
    
if __name__ == '__main__':
    date_range = ["2016-03-01", "2017-02-28"]
    #date_range = ["2016-03-05", "2016-03-07"]
    start_dp = 0
    end_dp = 24
    
    analyzer = TimeseriesAnalyzer(date_range, start_dp=start_dp, end_dp=end_dp)
    
    #modes = ["Spread", "Price", "Sell volume", "Buy volume", "Buy price", "Sell price"]
    modes = ["Price"]
    #modes = ["SP" + str(i) for i in range(5)]
    for mode in modes:
        file_name_counter = ""
        analyzer.analyze(mode)
        while True:
            
            if(file_name_counter == ""):            
                analyzer.write_data_to_file("DataForAnalysis/TimeSeries/" + mode + "_" + date_range[0] + "-" + date_range[1] + "_" + str(start_dp) + "-" + str(end_dp) + ".xlsx")
            
            else:
                analyzer.write_data_to_file("DataForAnalysis/TimeSeries/" + mode + "_" + date_range[0] + "-" + date_range[1] + "-v" + file_name_counter + ".xlsx")
            
            break
            
    