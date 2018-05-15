# -*- coding: utf-8 -*-
"""
Created on Fri Apr  6 09:59:09 2018

@author: sverrak
"""

# Status:
# Description: Bid handling and clearing is done in this file. 
# Called from: dp_to_stats.py
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

class Market(object):

    """docstring for Market"""
    def __init__(self, dp, bid_file_tag, customer_bid_file_tag, use_dynamic_timestep, write_transactions_to_file=False, printing_mode=False, timeslot_length=10):
        super(Market, self).__init__()
        self.delivery_product             			= dp
        self.bid_file_tag                 			= bid_file_tag
        self.customer_bid_file_tag         			= customer_bid_file_tag
        self.write_transactions_to_file 			= write_transactions_to_file
        self.printing_mode               			= printing_mode
        self.visualization_mode       			  	= False
        self.testing_mode                           = False
        self.customer_mode                 			= customer_bid_file_tag != "      "
        
        ### ----------- System Parameters -----------
        self.print_output                   		= True
        self.default_parameters          			= False
        self.check_create_transactions = False
        self.use_dynamic_timestep = use_dynamic_timestep
        self.start_time                 			= time.time()
        self.folder                    				= "Data/"    
        self.time_lag                         		= 0.0
        
        
        # Documentation parameters
        self.transaction_file_name             		= "Output/Transactions/transactions" + bid_file_tag + ".xlsx"
        
        # Stats headers
        self.stats                 = [["Trading timeslot", "Avg transaction price", "Max transaction price", "Min transaction price", "Open transaction price", "Close transaction price", "Max open bid order price", "Min open ask order price", "Transaction volume", "Open bid volumes", "Open ask volumes", "Inflow buy bids", "Inflow sell bids", "Inflow buy bid volume", "Inflow sell bid volume", "Inflow attractive buy bid volumes", "Inflow attractive sell bid volumes", "Killed buy order volumes", "Killed sell order volumes", "Number of transactions", "Number of open bid orders", "Number of open ask orders", "Average buy order maturity", "Average sell order maturity"] + ["B.V"+str(i) for i in range(1,6)] + ["B.P"+str(i) for i in range(1,6)]+ ["S.V"+str(i) for i in range(1,6)] + ["S.P"+str(i) for i in range(1,6)]]

        ### ----------- Model Parameters -----------
        if (self.default_parameters == True):
              # Set model parameters
              self.params = 0
              

        else:
            self.share                         		= 1.0 # Share of input orderbook to be considered
            
            if(self.delivery_product < dt.strptime("2017-06-01 00:00:00",'%Y-%m-%d %H:%M:%S')):
                self.interzonal_gc                      = 90 # According to Phillippe
                self.time_from_gc_to_d        			= 30 
            else:
                self.interzonal_gc                      = 30 
                self.time_from_gc_to_d        			= 5 
            
            # Indices
            self.index_of_timestamp         		= 8
            self.index_of_timestamp2         		= 10
            self.index_of_isBuy            			= 9
            self.index_of_price             		= 11
            self.index_of_volume             		= 12
            self.index_of_order_id            		= 13
            self.index_of_delivery_product   		= 18
            self.index_of_zone                      = 3
            
            
            # Trading window parameters
            self.trading_start_time         		= self.delivery_product - datetime.timedelta(days=1) + datetime.timedelta(hours=13) - datetime.timedelta(hours=self.delivery_product.hour, minutes=self.delivery_product.minute)
            self.trading_end_time             		= self.delivery_product - datetime.timedelta(minutes=self.time_from_gc_to_d)
            
            self.length_of_timestep = timeslot_length
    
            #self.length_of_timestep        		= ((self.trading_end_time - self.trading_start_time) / self.stages).seconds / 60.0
            
            self.mini_time_inc_mode = True
            self.collect_aggregate_data = False
            self.mini_time_inc = datetime.timedelta(seconds=30)
            
            self.stages = int((self.trading_end_time - self.trading_start_time).seconds / self.length_of_timestep/60)
            self.number_of_minis_per_timeslot = int(self.length_of_timestep / self.mini_time_inc.seconds * 60)



        # Data structure initialization
        self.data 									= []                                        # All bids in /filename
        self.bidsB 									= []                                        # List of buy bid objects of the delivery product described above
        self.bidsS 									= []                                        # List of sell bid objects of the delivery product described above
        self.trading_timeslots 					= []                                        # List of timeslots
        self.open_buy_bids 							= []                                        # Iteratively updated
        self.open_sell_bids 						= []                                        # Iteratively updated
        self.transactions 							= []                                        # Iteratively updated # To do: necessary to add 1? 
        self.closed_buy_bids 						= []
        self.closed_sell_bids 						= []
        self.killed_bids 							= []


    

    ### ******** I/O HANDLING ******** 
    # Read data
    def read_data(self, tag):
        if(self.printing_mode == True):
            print("Reading data...")
        
        filename = self.folder + tag + ".txt"
        
        try:
            with open(filename) as f:
                data = f.readlines()
            
            processed_data = []
            
            for b in data[:int(self.share*len(data))]:
                processed_data.append(re.split(r'\t+', b))

            del data
            
            if(self.printing_mode == True):
                self.print_elapsed_time(self.start_time)

        except: # That is, if the data cannot be read

            print("Could not find file ", filename)

            processed_data = []

        return processed_data
    
    def write_transactions(self):
        book = xlsxwriter.Workbook(self.transaction_file_name)
        sheet = book.add_worksheet("Transactions")

        # Fill spreadsheet
        headers = ["Timestamp", "Price", "Volume", "Buy bid", "Sell bid"]
        for h in range(len(headers)):
            sheet.write(0, h, headers[h])
            
        for i in range(len(self.transactions)):
            sheet.write(i+1, 0, self.transactions[i].timestamp)
            sheet.write(i+1, 1, self.transactions[i].price)
            sheet.write(i+1, 2, self.transactions[i].volume)
            sheet.write(i+1, 3, self.transactions[i].buy_bid)
            sheet.write(i+1, 4, self.transactions[i].sell_bid)
        book.close()

    ### ******** PREPROCESSING FUNCTIONS ******** 
    # Filter data
    def filter_data(self, data, customer_bids=False):

        if(self.printing_mode == True):
            print("Filter data...")
        
        # Error checking variables 
        lens = [] # List of bid_array lengths discovered in the dataset
        buy_count = 0 # Number of buy bid_arrays
        sell_count = 0 # Number of sell bid_arrays
        junk = 0 # Number of bid_arrays not appended to the lists of bids
        
        for xx,bid_array in enumerate(data):
            	# If the timestamps were splitted, indices must be changed
            	# To do: Is this done correctly?
                
            if(len(bid_array) <= 1):
                # Some datasets have internal break-lines
                if(xx <= len(data) - 3):
                    continue
                # Handling unexpected line at the end of some dataests
                else:
                    print("breaking at ",xx, bid_array)
                    break
            
            elif(len(bid_array) == 3):
                continue
            
            # Vstrip
            if(bid_array[-1] == "\n"):
                bid_array = bid_array[:-1]
            
            # Error checking
            if(self.printing_mode == True and len(bid_array) not in lens):
                lens.append(len(bid_array))
                print(xx, len(bid_array), bid_array)
                
                
            # Setup indices based on bid array length
            if(len(bid_array) == 30):
                
                # Indices
                self.index_of_timestamp  						= 10
                self.index_of_timestamp2                    = 13
                self.index_of_isBuy             				= 9
                self.index_of_price            					= 15
                self.index_of_volume             				= 16
                self.index_of_order_id             				= 17
                self.index_of_delivery_product     				= 23
                self.index_of_zone                          = 3
            
            elif(len(bid_array) == 31):
                
                # Indices
                self.index_of_timestamp         				= 8
                self.index_of_timestamp2                    = 11
                self.index_of_isBuy             				= 13
                self.index_of_price             				= 16
                self.index_of_volume             				= 17
                self.index_of_order_id             				= 18
                self.index_of_delivery_product     				= 24
                self.index_of_zone                          = 3
            
            elif(len(bid_array) == 22):
                
                # Indices
                self.index_of_timestamp         				= 8
                self.index_of_timestamp2                    = 10
                self.index_of_isBuy             				= 9
                self.index_of_price             				= 11
                self.index_of_volume             				= 12
                self.index_of_order_id             				= 13
                self.index_of_delivery_product     				= 18
                self.index_of_zone                          = 3
            
            elif(len(bid_array) == 28):
                
                # Indices
                self.index_of_timestamp         				= 8
                self.index_of_timestamp2                    = 11
                self.index_of_isBuy             				= 10
                self.index_of_price             				= 13
                self.index_of_volume             				= 14
                self.index_of_order_id             				= 15
                self.index_of_delivery_product     				= 21
                self.index_of_zone                          = 3
                
            elif(len(bid_array) == 29):
                
                # Indices
                self.index_of_timestamp         				= 6
                self.index_of_timestamp2                    = 9
                self.index_of_isBuy             				= 11
                self.index_of_price             				= 14
                self.index_of_volume             				= 15
                self.index_of_order_id             				= 16
                self.index_of_delivery_product     				= 22
                self.index_of_zone                          = 3
            
            
            # Error checking
            if(bid_array[self.index_of_isBuy] not in ["0","1"]):
                raise ValueError("Not legal bid is buy", bid_array[self.index_of_isBuy], bid_array,"length:",len(bid_array))
            
            # Check the length of bid array
            if(len(bid_array) not in [22,28,29,30,31]):
                raise ValueError("NBNB: Bid array does not have a supported length.")
                
            
            # Setting the bid features
            bid_array_timestamp             					= bid_array[self.index_of_timestamp] + " " + bid_array[self.index_of_timestamp + 1] 
            bid_array_timestamp2                            = bid_array[self.index_of_timestamp2] + " " + bid_array[self.index_of_timestamp2 + 1]
            bid_array_is_buy                 					= bid_array[self.index_of_isBuy]
            bid_array_price                 					= float(bid_array[self.index_of_price]) / 100.0
            bid_array_volume                 					= float(bid_array[self.index_of_volume]) / 1000.0
            bid_array_order_id                 					= bid_array[self.index_of_order_id]
            bid_array_zone                                  = bid_array[self.index_of_zone]
            
            # Based on the bid features, add the bid_array to one of the bid lists
            if(bid_array_zone not in ["10YFR-RTE------C", "10YAT-APG------L", "10YCH-SWISSGRIDZ", "10YBE----------2", "10YNL----------L"]):
                # Identify and set the bid array delivery product
                if(len(bid_array[self.index_of_delivery_product]) == 27):
                    bid_array_dp = bid_array[self.index_of_delivery_product]
                
                elif(len(bid_array[self.index_of_delivery_product + 1]) == 27):
                    bid_array_dp = bid_array[self.index_of_delivery_product + 1]
                
                elif(":" in bid_array[self.index_of_delivery_product + 1]):
                    bid_array_dp                     					= bid_array[self.index_of_delivery_product] + " " + bid_array[self.index_of_delivery_product + 1]
                    
                elif(":" in bid_array[self.index_of_delivery_product]):
                    bid_array_dp                     					= bid_array[self.index_of_delivery_product - 1] + " " + bid_array[self.index_of_delivery_product]
                    
                else:
                    raise ValueError("Don't know where to find delivery product attribute")
                    
                # Identify and set the bid array timestamp
                if(len(bid_array[self.index_of_timestamp]) == 27):
                    bid_array_dp = bid_array[self.index_of_timestamp]
                
                elif(len(bid_array[self.index_of_timestamp + 1]) == 27):
                    bid_array_dp = bid_array[self.index_of_timestamp + 1]
                
                elif(":" in bid_array[self.index_of_timestamp + 1]):
                    bid_array_timestamp                     					= bid_array[self.index_of_timestamp] + " " + bid_array[self.index_of_timestamp + 1]
                
                elif(":" in bid_array[self.index_of_timestamp]):
                    bid_array_timestamp = bid_array[self.index_of_timestamp - 1] + " " + bid_array[self.index_of_timestamp]
                    print("Got B", len(bid_array_dp), bid_array_dp, bid_array)
                
                elif(":" in bid_array[self.index_of_timestamp + 1]):
                    bid_array_timestamp             					= bid_array[self.index_of_timestamp + 1] + " " + bid_array[self.index_of_timestamp + 2]
                
                else:
                    
                    raise ValueError("Don't know where to find timestamp attribute")
                    
                try:
                    
                    bid_array_dp                     					= dt.strptime(bid_array_dp.split(".")[0]        , '%Y-%m-%d %H:%M:%S')
                    bid_array_timestamp             					= dt.strptime(bid_array_timestamp.split(".")[0]    , '%Y-%m-%d %H:%M:%S')
                except:
                    bid_array_dp                     					= dt.strptime(bid_array_dp.split(".")[0]        , '%m/%d/%Y %H:%M:%S')
                    bid_array_timestamp             					= dt.strptime(bid_array_timestamp.split(".")[0]    , '%m/%d/%Y %H:%M:%S')
                
                
                if(True or not(float(bid_array_price) > 19899.0 or float(bid_array_price) < -19899.0)):
                    #print(bid_array_is_buy, bid_array_is_buy == "0")
                    if(bid_array_is_buy == "1"):
                        buy_count += 1
                        #                         Price                  Volume                Timestamp              Timestamp2               isBuy            Order ID     Placed by Customer
                        self.bidsB.append(Bid(float(bid_array_price), float(bid_array_volume), bid_array_timestamp, bid_array_timestamp2, bid_array_is_buy, bid_array_order_id, customer_bids, bid_array_zone))
                    elif(bid_array_is_buy == "0"):
                        sell_count += 1
                        self.bidsS.append(Bid(float(bid_array_price), float(bid_array_volume), bid_array_timestamp, bid_array_timestamp2, bid_array_is_buy, bid_array_order_id, customer_bids, bid_array_zone))
                    else:
                        raise ValueError("Is buy: " + str(bid_array_is_buy) + str(type(bid_array_is_buy)))
                else:
                    junk += 1
        
        
        if(self.printing_mode == True):
            self.print_elapsed_time(self.start_time)
        
        return self.bidsB, self.bidsS

    # Setup timeslots
    def setup_timeslots(self, start, end, isMini):
        timeslots = []
        time_iterator = start
        if(self.mini_time_inc_mode == True and isMini):
            step_length = self.mini_time_inc
        else:
            step_length = datetime.timedelta(minutes=self.length_of_timestep)

        while True:
            
            if(time_iterator + step_length > end + datetime.timedelta(minutes=11)): # Offset RHS by 1 minute to include end case
                break
            else:
                timeslots.append(time_iterator)
                time_iterator = time_iterator + step_length
        
        if(self.printing_mode):
            print("Number of timeslots: " + str(len(timeslots)))

        return timeslots

    def create_bid_dictionary(self, bids, timeslots):
        bid_dictionary = {}

        if(self.printing_mode):
            print("Number of bids before dict: " + str(len(bids)))
        
        # Create keys
        bids_temp = bids[:]
        if(self.mini_time_inc_mode):
            for timeslot in self.main_trading_timeslots:
                for mini_timeslot in range(self.number_of_minis_per_timeslot):
                    bid_dictionary[timeslot + mini_timeslot * self.mini_time_inc] = []
                    for i,b in enumerate(bids_temp):
                        if (b.get_timestamp() < timeslot + mini_timeslot * self.mini_time_inc):
        
                            bid_dictionary[timeslot + mini_timeslot * self.mini_time_inc].append(b)
                            bids_temp.remove(b)
        
                        #else:
                        #    bids_temp = bids_temp[i:]
                        #    break
                    
        else:
            for timeslot in self.trading_timeslots:        
                bid_dictionary[timeslot] = []
                for i,b in enumerate(bids_temp):
                    if (b.get_timestamp() < timeslot):
    
                        bid_dictionary[timeslot].append(b)
                        bids_temp.remove(b)
    
                    else:
                        bids_temp = bids_temp[i:]
                        break

        return bid_dictionary


    

    ### Support Functions
    
    def find_best_bids(self, bids, n, isBuy):
        bid_prices = []
        bid_volumes = []
        order_ids_of_used_bids = []
        bids.sort(key=lambda x: x.price, reverse = isBuy)
        
        for i in range(n):
            try:        
                if(bids[i].order_id not in order_ids_of_used_bids):
                    bid_prices.append(bids[i].price)
                    bid_volumes.append(bids[i].volume)
                    order_ids_of_used_bids.append(bids[i])
            except:
                break
        
        bid_prices = bid_prices + ["     " for i in range(n-len(bid_prices))]
        bid_volumes = bid_volumes + ["      " for i in range(n-len(bid_volumes))]
        
        return bid_prices, bid_volumes
    
    def print_elapsed_time(self, start_time):
        print("Time: " + str(int(10*time.time()-10*start_time)/10) + " seconds")
        start_time = time.time()
        
    def get_stats(self):
        return self.stats
    
    def compute_next_iterator(self, iterator, buy_bids):
        if(buy_bids[iterator + 1].volume > 0):
            return iterator + 1
        else:
            try:
                return iterator + 1 + self.compute_next_iterator(0, buy_bids[iterator+1:])
            except:
                return 9999999
    
    
    # This method can probably be improved by taking advantage of the fact that
    # the bid arrays are sorted by price. Not necessary to loop through the whole sets
    def create_transactions(self, open_sell_bids, open_buy_bids,  timeslot, interzonal_trades_allowed):
        if(timeslot > self.trading_end_time):
            return open_sell_bids, open_buy_bids
        
        open_sell_bids.sort(key=lambda x: x.price, reverse=False)
        open_buy_bids.sort(key=lambda x: x.price, reverse=True)
        closed_sell_bids = []
        closed_buy_bids = []
        
        # Loop through all buy bids and sell bids and create transaction if possible
        for j,s in enumerate(open_sell_bids):
            if(s.isOpen == True and s.volume > 0):
                # Loop through the buy bids and look for possible transactions
                iterator = 0
                
                while(iterator < len(open_buy_bids) and s.price < open_buy_bids[iterator].price and s.isOpen == True):
                    
                    # If interzonal trade is allowed or the zones of the compared bids are identical
                    if(open_buy_bids[iterator].isOpen == True):
                        if(interzonal_trades_allowed == True or s.zone == open_buy_bids[iterator].zone):
                            
                            # Compute transaction attributes
                            transaction_price = s.price if s.timestamp < open_buy_bids[iterator].timestamp else open_buy_bids[iterator].price
                            transaction_volume = min(s.volume, open_buy_bids[iterator].volume)
                            transaction_timestamp = s.timestamp if s.timestamp > open_buy_bids[iterator].timestamp else open_buy_bids[iterator].timestamp
                            transaction_sell_id = s.order_id
                            transaction_buy_id = open_buy_bids[iterator].order_id
                            
                            # Add the transaction to the transaction list
                            self.transactions.append(Transaction(transaction_sell_id, transaction_buy_id, transaction_price, transaction_volume, transaction_timestamp))
                            
                            
                            # Reduce volume of all bids having the order ids above
                            for i,b in enumerate(open_sell_bids):
                                if(b.order_id == transaction_sell_id and b.volume > 0):
                                    b.reduce_volume(transaction_volume, transaction_timestamp)
                                    if(b.volume == 0):
                                        b.isOpen = False
                                        closed_sell_bids.append(b)
                            
                            for i,b in enumerate(open_buy_bids):
                                if(b.order_id == transaction_buy_id and b.volume > 0):
                                    b.reduce_volume(transaction_volume, transaction_timestamp)
                                    if(b.volume == 0):
                                        b.isOpen = False
                                        closed_buy_bids.append(b)

                    
                    
                    iterator = iterator + 1
                
        # Safety mechanism. Loop through all bids and save only those who...:
        # - Are not in closed_X_bids
        # - Do not have order id equal to one of those in closed_X_ids
        # - Are not closed (isOpen == True)
        open_s = []
        open_b = []
        closed_sell_ids = [s.order_id for s in closed_sell_bids]
        closed_buy_ids = [s.order_id for s in closed_buy_bids]
        
        for j,s in enumerate(open_sell_bids):
            if(s.isOpen == True and s not in closed_sell_bids and s.order_id not in closed_sell_ids):
                open_s.append(s)
        
        for j,s in enumerate(open_buy_bids):
            if(s.isOpen == True and s not in closed_buy_bids and s.order_id not in closed_buy_ids):
                open_b.append(s)
        
        return open_s, open_b
    
    
    def remove_killed_bids(self, bids, new_bid):
        killed_bids = []
        temp_bids =[] # The list of bids that are not yet killed
        
        for b in bids:
            if(b.order_id == new_bid.order_id):
                b.kill_bid(new_bid.timestamp)
                killed_bids.append(b)
            else:
                temp_bids.append(b)
                
        return temp_bids, killed_bids


    
    # Finds the indices of open_buy_bids having order_id equal to order_id
    def find_indices_of_bids_with_order_id(self, open_buy_bids, order_id):
        indices = []
        
        for i,bid in enumerate(open_buy_bids):
            if (bid.order_id == order_id):
                indices.append(i)
        
        return [-1] if len(indices) == 0 else indices
    
    
    ### ******** MAIN FUNCTION ******** 
    # Bid by bid iterations.
    # Major current error: only one bid at a time will make duplicate bids (identical order ids) occur one after one
    def alternative_runner(self):
        # Data structure instantiation
        
        self.trading_timeslots     = self.setup_timeslots(self.trading_start_time, self.trading_end_time, True)  # List of timeslots
        
        self.main_trading_timeslots     = self.setup_timeslots(self.trading_start_time, self.trading_end_time, False)  # List of timeslots
        data                     = self.read_data(self.bid_file_tag)
        
        # Create lists of bids
        buy_bids, sell_bids     = self.filter_data(data, False)
        
        sell_bids.sort(key=lambda x: x.timestamp, reverse=False)
        buy_bids.sort(key=lambda x: x.timestamp, reverse=False)
        number_of_buy_bids = len(buy_bids)
        number_of_sell_bids = len(sell_bids)
        
        buy_bid_iterator = 0
        sell_bid_iterator = 0
        buy_bid_iterator_most_recently_changed = 0
        buy_bid_iterator_count = 0
        sell_bid_iterator_count = 0
        
        open_buy_bids = []
        open_sell_bids = []
        unique_open_buy_bids = []
        unique_open_sell_bids = []
        unique_open_buy_bids = []
        unique_open_sell_bids = []
        killed_buy_volume = 0
        killed_sell_volume = 0
        inflow_buy_bids = []
        inflow_sell_bids = []
        inflow_buy_volume = 0
        inflow_sell_volume = 0
        inflow_attractive_buy_volume = 0
        inflow_attractive_sell_volume = 0
        open_buy_bids_prices = [0 for i in range(5)]
        open_sell_bids_prices = [0 for i in range(5)]
        
        transaction_index_first = 0
        number_of_transactions = 0
        next_aggregation = self.trading_start_time + datetime.timedelta(minutes=10)
        price_threshold = 14899.0
        
        while(buy_bid_iterator < number_of_buy_bids and sell_bid_iterator < number_of_sell_bids):

            # Fetch new bid
            if(buy_bid_iterator_most_recently_changed == True):
                new_bid = (buy_bids[buy_bid_iterator])
                inflow_buy_bids.append(new_bid)
                
                inflow_buy_volume += new_bid.volume
                
                if(" " in str(open_buy_bids_prices)):
                    inflow_attractive_buy_volume += new_bid.volume
                elif(new_bid.price >= float(open_buy_bids_prices[-1])):
                    inflow_attractive_buy_volume += new_bid.volume
                
            else:
                new_bid = (sell_bids[sell_bid_iterator])
                inflow_sell_bids.append(new_bid)
                inflow_sell_volume += new_bid.volume
                if(" " in str(open_sell_bids_prices)):
                    inflow_attractive_sell_volume += new_bid.volume
                elif(new_bid.price <= float(open_sell_bids_prices[-1])):
                    inflow_attractive_sell_volume += new_bid.volume
                
            # Update bid volumes depending on situation:
            # - If the new_bid timestamp is equal to the 
            
            if(new_bid.volume > 0.0):
                if(new_bid.order_id in unique_open_buy_bids):
                    indices = self.find_indices_of_bids_with_order_id(open_buy_bids, new_bid.order_id)
                    
                    # If the bid is present in open_buy_bids
                    if(indices[0] >= 0): 
                        if(new_bid.timestamp == open_buy_bids[indices[0]].timestamp):
                            new_bid.volume = open_buy_bids[indices[0]].volume
                            new_bid.price = open_buy_bids[indices[0]].price
                        else:
                            for i in indices:
                                
                                open_buy_bids[i].volume = new_bid.volume
                                open_buy_bids[i].price = new_bid.price
                                
                
                if(new_bid.order_id in unique_open_sell_bids):
                    indices = self.find_indices_of_bids_with_order_id(open_sell_bids, new_bid.order_id)
                    
                    # If the bid is present in open_buy_bids
                    if(indices[0] >= 0): 
                        if(new_bid.timestamp == open_sell_bids[indices[0]].timestamp):
                            new_bid.volume = open_sell_bids[indices[0]].volume
                            new_bid.price = open_sell_bids[indices[0]].price
                        else:
                            for i in indices:
                                open_sell_bids[i].volume = new_bid.volume
                                open_sell_bids[i].price = new_bid.price
                                
            # If this bid has volume 0, kill all bids with equal order id
            # Killed_X_volume is updated using the Max-function. Average would have been incorrect
            # as one of the bids [new bid] will have volume == 0 
            if(float(new_bid.volume) == 0.0):
                if(buy_bid_iterator_most_recently_changed == True):
                    open_buy_bids, killed_buy_bids = self.remove_killed_bids(open_buy_bids, new_bid)
                    if(len(killed_buy_bids) > 0):    
                        killed_buy_volume += max([b.volume for b in killed_buy_bids])
                else:
                    open_sell_bids, killed_sell_bids = self.remove_killed_bids(open_sell_bids, new_bid)
                    if(len(killed_sell_bids) > 0):    
                        killed_sell_volume += max([b.volume for b in killed_sell_bids])
                
            # Otherwise, look for potential transactions involving this bid
            else:
                if(buy_bid_iterator_most_recently_changed == True):
                    if (new_bid.order_id not in unique_open_buy_bids): # To do: This currently makes bid updating impossible (except from bid killing)
                        if(False and new_bid.price > 14890.0):
                            print("\t\t",new_bid.timestamp, "Tradeable volume:", sum([b.volume if b.price < new_bid.price else 0 for b in open_sell_bids]), new_bid.volume)
                        unique_open_buy_bids.append(new_bid.order_id)
                        open_sell_bids, new_buy_bid_after_transactions = self.create_transactions(open_sell_bids, [new_bid], new_bid.timestamp, new_bid.timestamp < self.trading_end_time)
                        if(len(new_buy_bid_after_transactions) > 0 and not (new_buy_bid_after_transactions[0].price > price_threshold)):
                            open_buy_bids = open_buy_bids + new_buy_bid_after_transactions
                        
                else:
                    if(new_bid.order_id not in unique_open_sell_bids):
                        if(False and new_bid.price > 14890.0 or new_bid.price < -14890.0):
                            print("\t\t",new_bid.timestamp, "Tradeable volume:", sum([b.volume if b.price < new_bid.price else 0 for b in open_sell_bids]), new_bid.volume)
                        unique_open_sell_bids.append(new_bid.order_id)
                        new_sell_bid_after_transactions, open_buy_bids = self.create_transactions([new_bid], open_buy_bids, new_bid.timestamp, new_bid.timestamp < self.trading_end_time)
                        if(len(new_sell_bid_after_transactions) > 0 and not (new_sell_bid_after_transactions[0].price < - price_threshold)):
                            open_sell_bids = open_sell_bids + new_sell_bid_after_transactions
                            
                        
            # If the time is right, aggregate the data:
            if(new_bid.timestamp > next_aggregation): # to do: snapshot Before or after timestamp???
                
                transaction_index_first = transaction_index_first + number_of_transactions
                number_of_transactions = len(self.transactions[transaction_index_first:])
                number_of_open_buy_bids = len(open_buy_bids)
                number_of_open_sell_bids = len(open_sell_bids)
                
                
                if(number_of_transactions > 0):
                    transaction_volume = sum([t.volume for t in self.transactions[transaction_index_first:]])                    
                    avg_transaction_price = sum(t.price*t.volume for t in self.transactions[transaction_index_first:]) / transaction_volume
                    max_transaction_price = max([t.price for t in self.transactions[transaction_index_first:]])
                    min_transaction_price = min([t.price for t in self.transactions[transaction_index_first:]])
                    open_price = self.transactions[transaction_index_first].price
                    close_price = self.transactions[-1].price
                else:
                    avg_transaction_price = "      "
                    max_transaction_price = "      "
                    min_transaction_price = "      "
                    transaction_volume = "      "
                    open_price = "      "
                    close_price = "      "
                
                if(number_of_open_buy_bids > 0):
                    max_open_bid_order_price = max(b.price for b in open_buy_bids)
                    total_open_buy_bid_volumes = sum(b.volume for b in open_buy_bids)  
                    avg_buy_order_maturity = sum(b.compute_maturity(next_aggregation) for b in open_buy_bids) / number_of_open_buy_bids
                    open_buy_bids_prices, open_buy_bids_volumes = self.find_best_bids(open_buy_bids, 5, True)
                    
                else:
                    max_open_bid_order_price = "      "
                    total_open_buy_bid_volumes = 0
                    avg_buy_order_maturity = "      "
                    open_buy_bids_prices, open_buy_bids_volumes = "      ","      "
                
                if(number_of_open_sell_bids):
                    min_open_ask_order_price = min(b.price for b in open_sell_bids)
                    total_open_sell_bid_volumes = sum(b.volume for b in open_sell_bids)
                    avg_sell_order_maturity = sum(b.compute_maturity(next_aggregation) for b in open_sell_bids) / number_of_open_sell_bids
                    open_sell_bids_prices, open_sell_bids_volumes = self.find_best_bids(open_sell_bids, 5, False)
                else:
                    min_open_ask_order_price = "      "
                    total_open_sell_bid_volumes = 0
                    avg_sell_order_maturity = "      "
                    open_sell_bids_prices, open_sell_bids_volumes = "      ","      "
                    
                killed_buy_volume = killed_buy_volume
                killed_sell_volume = killed_sell_volume
                
                self.stats.append([next_aggregation, avg_transaction_price, max_transaction_price, min_transaction_price, open_price, close_price, max_open_bid_order_price, min_open_ask_order_price, transaction_volume, total_open_buy_bid_volumes, total_open_sell_bid_volumes, len(inflow_buy_bids), len(inflow_sell_bids), inflow_buy_volume, inflow_sell_volume, inflow_attractive_buy_volume, inflow_attractive_sell_volume, killed_buy_volume, killed_sell_volume, number_of_transactions, number_of_open_buy_bids, number_of_open_sell_bids, avg_buy_order_maturity, avg_sell_order_maturity] + list(open_buy_bids_volumes) + list(open_buy_bids_prices) + list(open_sell_bids_volumes) + list(open_sell_bids_prices))
                
                # Reset counters
                buy_bid_iterator_count = 0
                sell_bid_iterator_count = 0
                killed_buy_volume = 0
                killed_sell_volume = 0
                inflow_buy_bids = []
                inflow_sell_bids = []
                inflow_buy_volume = 0
                inflow_sell_volume = 0
                inflow_attractive_buy_volume = 0
                inflow_attractive_sell_volume = 0
                killed_buy_volume = 0
                killed_sell_volume = 0
                
                
                # Set the upcoming aggregation timeslot
                if(self.use_dynamic_timestep == True):
                    
                    if(next_aggregation < self.trading_end_time - datetime.timedelta(minutes=180)):
                        next_aggregation = next_aggregation + datetime.timedelta(minutes=15)
                        
                    elif(next_aggregation < self.trading_end_time - datetime.timedelta(minutes=60)):
                        next_aggregation = next_aggregation + datetime.timedelta(minutes=5)
                    else:
                        next_aggregation = next_aggregation + datetime.timedelta(minutes=2)
                
                else:
                    next_aggregation = next_aggregation + datetime.timedelta(minutes=self.length_of_timestep)
                
                
            
            # Find out which bid to update next
            if(buy_bid_iterator + 1 == len(buy_bids) and sell_bid_iterator + 1 == len(sell_bids)):
                break
            
            elif(buy_bid_iterator + 1 == len(buy_bids)):
                
                sell_bid_iterator_count += 1
                sell_bid_iterator += 1
                buy_bid_iterator_most_recently_changed = False
            
            elif(sell_bid_iterator + 1 == len(sell_bids)):
                
                buy_bid_iterator_count += 1
                buy_bid_iterator += 1
                buy_bid_iterator_most_recently_changed = True
            
            elif(buy_bids[buy_bid_iterator + 1].timestamp < sell_bids[sell_bid_iterator + 1].timestamp):
                
                buy_bid_iterator_count += 1
                buy_bid_iterator += 1
                buy_bid_iterator_most_recently_changed = True
            
            else:
                
                sell_bid_iterator_count += 1
                sell_bid_iterator += 1
                buy_bid_iterator_most_recently_changed = False
                
        # Append the final incoming bid stats
        transaction_index_first = transaction_index_first + number_of_transactions
        number_of_transactions = len(self.transactions[transaction_index_first:])
        number_of_open_buy_bids = len(open_buy_bids)
        number_of_open_sell_bids = len(open_sell_bids)
        
        if(number_of_transactions > 0):
            transaction_volume = sum([t.volume for t in self.transactions[transaction_index_first:]])                    
            avg_transaction_price = sum(t.price*t.volume for t in self.transactions[transaction_index_first:]) / transaction_volume
            max_transaction_price = max([t.price for t in self.transactions[transaction_index_first:]])
            min_transaction_price = min([t.price for t in self.transactions[transaction_index_first:]])
            open_price = self.transactions[transaction_index_first].price
            close_price = self.transactions[-1].price
        else:
            avg_transaction_price = "      "
            max_transaction_price = "      "
            min_transaction_price = "      "
            transaction_volume = "      "
            open_price = "      "
            close_price = "      "
        
        if(number_of_open_buy_bids > 0):
            max_open_bid_order_price = max(b.price for b in open_buy_bids)
            total_open_buy_bid_volumes = sum(b.volume for b in open_buy_bids)  
            avg_buy_order_maturity = sum(b.compute_maturity(self.trading_end_time) for b in open_buy_bids) / number_of_open_buy_bids
            open_buy_bids_prices, open_buy_bids_volumes = self.find_best_bids(open_buy_bids, 5, True)
            
        else:
            max_open_bid_order_price = "      "
            total_open_buy_bid_volumes = 0
            avg_buy_order_maturity = "      "
            open_buy_bids_prices, open_buy_bids_volumes = "      ","      "
        
        if(number_of_open_sell_bids):
            min_open_ask_order_price = min(b.price for b in open_sell_bids)
            total_open_sell_bid_volumes = sum(b.volume for b in open_sell_bids)
            avg_sell_order_maturity = sum(b.compute_maturity(self.trading_end_time) for b in open_sell_bids) / number_of_open_sell_bids
            open_sell_bids_prices, open_sell_bids_volumes = self.find_best_bids(open_sell_bids, 5, False)
        else:
            min_open_ask_order_price = "      "
            total_open_sell_bid_volumes = 0
            avg_sell_order_maturity = "      "
            open_sell_bids_prices, open_sell_bids_volumes = "      ","      "
            
        killed_buy_volume = killed_buy_volume
        killed_sell_volume = killed_sell_volume
        
        self.stats.append([self.trading_end_time, avg_transaction_price, max_transaction_price, min_transaction_price, open_price, close_price, max_open_bid_order_price, min_open_ask_order_price, transaction_volume, total_open_buy_bid_volumes, total_open_sell_bid_volumes, killed_buy_volume, killed_sell_volume, number_of_transactions, number_of_open_buy_bids, number_of_open_sell_bids, avg_buy_order_maturity, avg_sell_order_maturity] + list(open_buy_bids_volumes) + list(open_buy_bids_prices) + list(open_sell_bids_volumes) + list(open_sell_bids_prices))
        
        # Reset counters
        buy_bid_iterator_count = 0
        sell_bid_iterator_count = 0
        
        
        self.write_transactions()
        

if __name__ == "__main__":
    market = Market()
    market.initandrun()
    
    print("Done running the market!")
    if(market.printing_mode):
        print("Number of transactions: " + str(len(market.transactions)))
        market.print_transactions(mode="price")
    if(market.write_transactions_to_file):
        market.write_transactions()
    print("\nDone\n\n")
