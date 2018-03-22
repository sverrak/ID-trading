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

class Market(object):

    """docstring for Market"""
    def __init__(self, dp, bid_file_tag, customer_bid_file_tag, write_transactions_to_file=False, printing_mode=False, timeslot_length=10):
        super(Market, self).__init__()
        self.delivery_product             			= dp
        self.bid_file_tag                 			= bid_file_tag
        self.customer_bid_file_tag         			= customer_bid_file_tag
        self.write_transactions_to_file 			= write_transactions_to_file
        self.printing_mode               			= printing_mode
        self.visualization_mode       			  	= False
        self.testing_mode                           = False
        self.customer_mode                 			= customer_bid_file_tag != "N/A"
        
        ### ----------- System Parameters -----------
        self.print_output                   		= True
        self.default_parameters          			= False
        self.check_create_transactions = False
        self.start_time                 			= time.time()
        self.folder                    				= "Data/"    
        self.time_lag                         		= 0.0
        
        # Documentation parameters
        self.transaction_file_name             		= "Output/Transactions/transactions" + bid_file_tag + ".xlsx"

        ### ----------- Model Parameters -----------
        if (self.default_parameters == True):
              # Set model parameters
              self.params = 0
              

        else:
            self.share                         		= 1.0 # Share of input orderbook to be considered
            
            if(self.delivery_product < dt.strptime("2016-06-01 00:00:00",'%Y-%m-%d %H:%M:%S')):
                self.interzonal_gc                      = 45 # According to Phillippe
                self.time_from_gc_to_d        			= 45 
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
            #print((self.trading_end_time - self.trading_start_time), "/",self.stages,"=", self.length_of_timestep)
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
        self.transactions 							= [[] for i in range((self.stages+10000)*(self.number_of_minis_per_timeslot+1))]      # Iteratively updated # To do: necessary to add 1? 
        self.closed_buy_bids 						= []
        self.closed_sell_bids 						= []
        self.killed_bids 							= []


    def print_elapsed_time(self, start_time):
        print("Time: " + str(int(10*time.time()-10*start_time)/10) + " seconds")
        start_time = time.time()

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

    # Filter data
    def filter_data(self, data, customer_bids=False):

        if(self.printing_mode == True):
            print("Filter data...")

        
        for xx,bid_array in enumerate(data):
            	# If the timestamps were splitted, indices must be changed
            	# To do: Is this done correctly?
            if(bid_array[-1] == "\n"):
                bid_array = bid_array[:-1]
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
            
            if(bid_array[self.index_of_isBuy] not in ["0","1"]):
                raise ValueError("Not legal bid is buy")
            # Check the length of bid array
            if(len(bid_array) != 31 and len(bid_array) != 30    ):
                print("NBNB: Bid array does not have a supported length.")
                print(len(bid_array))
                print(bid_array)
            #print(xx, bid_array)
            bid_array_timestamp             					= bid_array[self.index_of_timestamp] + " " + bid_array[self.index_of_timestamp + 1] 
            bid_array_timestamp2                            = bid_array[self.index_of_timestamp2] + " " + bid_array[self.index_of_timestamp2 + 1]
            bid_array_is_buy                 					= bid_array[self.index_of_isBuy]
            
            bid_array_price                 					= bid_array[self.index_of_price]
            bid_array_volume                 					= bid_array[self.index_of_volume]
            bid_array_order_id                 					= bid_array[self.index_of_order_id]
            bid_array_zone                                  = bid_array[self.index_of_zone]
            
            if(bid_array_zone not in ["10YFR-RTE------C", "10YAT-APG------L", "10YCH-SWISSGRIDZ", "10YBE----------2", "10YNL----------L"]):
                if(":" in bid_array[self.index_of_delivery_product + 1]):
                    bid_array_dp                     					= bid_array[self.index_of_delivery_product] + " " + bid_array[self.index_of_delivery_product + 1]
                    #print("Got A", len(bid_array_dp), bid_array_dp)
                elif(":" in bid_array[self.index_of_delivery_product]):
                    bid_array_dp                     					= bid_array[self.index_of_delivery_product - 1] + " " + bid_array[self.index_of_delivery_product]
                    print("Got B", len(bid_array_dp), bid_array_dp)
                else:
                    raise ValueError("Don't know where to find delivery product attribute")
                    
                if(":" in bid_array[self.index_of_timestamp + 1]):
                    bid_array_timestamp                     					= bid_array[self.index_of_timestamp] + " " + bid_array[self.index_of_timestamp + 1]
                    #print("Got A", len(bid_array_dp), bid_array_dp)
                elif(":" in bid_array[self.index_of_timestamp]):
                    bid_array_timestamp = bid_array[self.index_of_timestamp - 1] + " " + bid_array[self.index_of_timestamp]
                    print("Got B", len(bid_array_dp), bid_array_dp)
                elif(":" in bid_array[self.index_of_timestamp + 1]):
                    bid_array_timestamp             					= bid_array[self.index_of_timestamp + 1] + " " + bid_array[self.index_of_timestamp + 2]
                else:
                    print(bid_array)
                    raise ValueError("Don't know where to find timestamp attribute")
                    
                try:
                    bid_array_dp                     					= dt.strptime(bid_array_dp.split(".")[0]        , '%Y-%m-%d %H:%M:%S')
                    bid_array_timestamp             					= dt.strptime(bid_array_timestamp.split(".")[0]    , '%Y-%m-%d %H:%M:%S')
                except:
                    bid_array_dp                     					= dt.strptime(bid_array_dp.split(".")[0]        , '%m/%d/%Y %H:%M:%S')
                    bid_array_timestamp             					= dt.strptime(bid_array_timestamp.split(".")[0]    , '%m/%d/%Y %H:%M:%S')
                
                
                if(True or not(float(bid_array_price) > 19899.0 or float(bid_array_price) < -19899.0)):
                    if(bid_array_is_buy == "1"):
                        #                         Price                  Volume                Timestamp              Timestamp2               isBuy            Order ID     Placed by Customer
                        self.bidsB.append(Bid(float(bid_array_price), float(bid_array_volume), bid_array_timestamp, bid_array_timestamp2, bid_array_is_buy, bid_array_order_id, customer_bids, bid_array_zone))
                    elif(bid_array_is_buy == "0"):
                        self.bidsS.append(Bid(float(bid_array_price), float(bid_array_volume), bid_array_timestamp, bid_array_timestamp2, bid_array_is_buy, bid_array_order_id, customer_bids, bid_array_zone))
                    else:
                        raise ValueError("Is buy: " + str(bid_array_is_buy) + str(type(bid_array_is_buy)))

        if(False and self.printing_mode):
            print("Number of buy bids: " + str(len(self.bidsB)))
            print("Number of sell bids: " + str(len(self.bidsS)))
            print("Total number of bids: " + str(len(self.bidsS)+(len(self.bidsB))))

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


    def get_stats(self):
        return self.stats

    ### Support Functions
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
    def create_transactionsv2(self, open_sell_bids, open_buy_bids, time, timeslot, interzonal_trades_allowed):
        if(timeslot > self.trading_end_time):
            return open_sell_bids, open_buy_bids
        
        open_sell_bids.sort(key=lambda x: x.price, reverse=False)
        open_buy_bids.sort(key=lambda x: x.price, reverse=True)
        closed_sell_bids = []
        closed_buy_bids = []
        
        for j,s in enumerate(open_sell_bids):
            #print(j, s.isOpen == True, s.volume > 0, 0 < len(open_buy_bids), s.price < open_buy_bids[0].price, interzonal_trades_allowed == True or s.zone == open_buy_bids[0].zone, len(self.transactions[time]))
            if(self.check_create_transactions and time == 18):
                print(j, s.order_id, "1093838479" in [t.order_id for t in open_sell_bids]  )
            if(self.check_create_transactions and str(s.order_id) == "1093838479" and time == 18):
                print(timeslot, s.volume, s.price, len(open_buy_bids), open_buy_bids[0].price, s.price < open_buy_bids[0].price, interzonal_trades_allowed == True or s.zone == open_buy_bids[0].zone, "1093838564" in [t.order_id for t in open_sell_bids])
            if(s.isOpen == True and s.volume > 0):
                # Loop through the buy bids and look for possible transactions
                iterator = 0
                
                while(iterator < len(open_buy_bids) and s.price < open_buy_bids[iterator].price and s.isOpen == True):
                    
                    # If interzonal trade is allowed or the zones of the compared bids are identical
                    if(open_buy_bids[iterator].isOpen == True):
                        if(interzonal_trades_allowed == True or s.zone == open_buy_bids[iterator].zone):
                            
                            # Create transaction
                            transaction_price = s.price if s.timestamp < open_buy_bids[iterator].timestamp else open_buy_bids[iterator].price
                            transaction_volume = min(s.volume, open_buy_bids[iterator].volume)
                            transaction_timestamp = s.timestamp if s.timestamp > open_buy_bids[iterator].timestamp else open_buy_bids[iterator].timestamp
                            transaction_sell_id = s.order_id
                            transaction_buy_id = open_buy_bids[iterator].order_id
                            try:
                                self.transactions[time].append(Transaction(transaction_sell_id, transaction_buy_id, transaction_price, transaction_volume, transaction_timestamp))
                            except:
                                raise ValueError("Transaction length:", len(self.transactions), time)
                            
                            # Reduce volume of all bids having the order ids above
                            for i,b in enumerate(open_sell_bids):
                                if(b.order_id == transaction_sell_id and b.volume > 0):
                                    b.reduce_volume(transaction_volume, transaction_timestamp)
                                    if(b.volume == 0):
                                        b.isOpen = False
                                        #open_sell_bids.remove(b)
                                        closed_sell_bids.append(b)
                            
                            for i,b in enumerate(open_buy_bids):
                                if(b.order_id == transaction_buy_id and b.volume > 0):
                                    b.reduce_volume(transaction_volume, transaction_timestamp)
                                    if(b.volume == 0):
                                        b.isOpen = False
                                        #open_buy_bids.remove(b)
                                        closed_buy_bids.append(b)
                                        #if(i < iterator):
                                        #    iterator -= 1
                    
                    
                    iterator = iterator + 1
                    # Alternative method (more efficient?? but not working properly atm)
                    #iterator = self.compute_next_iterator(iterator,open_buy_bids)
                    
        
        open_s = []
        open_b = []
        closed_sell_ids = [s.order_id for s in closed_sell_bids]
        closed_buy_ids = [s.order_id for s in closed_buy_bids]
        
        for j,s in enumerate(open_sell_bids):
            if(self.check_create_transactions and time == 18 and str(s.order_id) == "1093838479"):
                print(s.isOpen == True, s not in closed_sell_bids, s.order_id not in closed_sell_ids, s.volume)
            if(s.isOpen == True and s not in closed_sell_bids and s.order_id not in closed_sell_ids):
                open_s.append(s)
        
        for j,s in enumerate(open_buy_bids):
            if(s.isOpen == True and s not in closed_buy_bids and s.order_id not in closed_buy_ids):
                open_b.append(s)
        
        return open_s, open_b
    
    # This method can probably be improved by taking advantage of the fact that
    # the bid arrays are sorted by price. Not necessary to loop through the whole sets
    def create_transactions(self, open_sell_bids, open_buy_bids, time, timeslot, interzonal_trades_allowed):
        # No transaction should occur after trading end time.
        if(timeslot > self.trading_end_time):
            return open_sell_bids, open_buy_bids
        
        open_sell_bids.sort(key=lambda x: x.price, reverse=False)
        open_buy_bids.sort(key=lambda x: x.price, reverse=True)
        closed_sell_bids = []
        closed_buy_bids = []
        
        for j,s in enumerate(open_sell_bids):
            #print(j, s.isOpen == True, s.volume > 0, 0 < len(open_buy_bids), s.price < open_buy_bids[0].price, interzonal_trades_allowed == True or s.zone == open_buy_bids[0].zone, len(self.transactions[time]))
            if(self.check_create_transactions and time == 18):
                print(j, s.order_id, "1093838479" in [t.order_id for t in open_sell_bids]  )
            if(self.check_create_transactions and str(s.order_id) == "1093838479" and time == 18):
                print(timeslot, s.volume, s.price, len(open_buy_bids), open_buy_bids[0].price, s.price < open_buy_bids[0].price, interzonal_trades_allowed == True or s.zone == open_buy_bids[0].zone, "1093838564" in [t.order_id for t in open_sell_bids])
            if(s.isOpen == True and s.volume > 0):
                # Loop through the buy bids and look for possible transactions
                iterator = 0
                
                while(iterator < len(open_buy_bids) and s.price < open_buy_bids[iterator].price and s.isOpen == True):
                    
                    # If interzonal trade is allowed or the zones of the compared bids are identical
                    if(open_buy_bids[iterator].isOpen == True):
                        if(interzonal_trades_allowed == True or s.zone == open_buy_bids[iterator].zone):
                            
                            # Create transaction
                            transaction_price = s.price if s.timestamp < open_buy_bids[iterator].timestamp else open_buy_bids[iterator].price
                            transaction_volume = min(s.volume, open_buy_bids[iterator].volume)
                            transaction_timestamp = s.timestamp if s.timestamp > open_buy_bids[iterator].timestamp else open_buy_bids[iterator].timestamp
                            transaction_sell_id = s.order_id
                            transaction_buy_id = open_buy_bids[iterator].order_id
                            try:
                                self.transactions[time].append(Transaction(transaction_sell_id, transaction_buy_id, transaction_price, transaction_volume, transaction_timestamp))
                            except:
                                raise ValueError("Transaction length:", len(self.transactions), time)
                            
                            # Reduce volume of all bids having the order ids above
                            for i,b in enumerate(open_sell_bids):
                                if(b.order_id == transaction_sell_id and b.volume > 0):
                                    b.reduce_volume(transaction_volume, transaction_timestamp)
                                    if(b.volume == 0):
                                        b.isOpen = False
                                        #open_sell_bids.remove(b)
                                        closed_sell_bids.append(b)
                            
                            for i,b in enumerate(open_buy_bids):
                                if(b.order_id == transaction_buy_id and b.volume > 0):
                                    b.reduce_volume(transaction_volume, transaction_timestamp)
                                    if(b.volume == 0):
                                        b.isOpen = False
                                        #open_buy_bids.remove(b)
                                        closed_buy_bids.append(b)
                                        #if(i < iterator):
                                        #    iterator -= 1
                    
                    
                    iterator = iterator + 1
                    # Alternative method (more efficient?? but not working properly atm)
                    #iterator = self.compute_next_iterator(iterator,open_buy_bids)
                    
        
        open_s = []
        open_b = []
        closed_sell_ids = [s.order_id for s in closed_sell_bids]
        closed_buy_ids = [s.order_id for s in closed_buy_bids]
        
        for j,s in enumerate(open_sell_bids):
            if(self.check_create_transactions and time == 18 and str(s.order_id) == "1093838479"):
                print(s.isOpen == True, s not in closed_sell_bids, s.order_id not in closed_sell_ids, s.volume)
            if(s.isOpen == True and s not in closed_sell_bids and s.order_id not in closed_sell_ids):
                open_s.append(s)
        
        for j,s in enumerate(open_buy_bids):
            if(s.isOpen == True and s not in closed_buy_bids and s.order_id not in closed_buy_ids):
                open_b.append(s)
        
        return open_s, open_b
                    
        
        
    # This method can probably be improved by taking advantage of the fact that
    # the bid arrays are sorted by price. Not necessary to loop through the whole sets
    def create_transactions2(self, open_sell_bids, open_buy_bids, time, timeslot, interzonal_trades_allowed):
        #print("Num order_id:",sum([1 if str(t.order_id) == "1093833502" else 0 for t in open_sell_bids]))
        #print("Num order_id:",sum([t.volume if str(t.order_id) == "1093833502" else 0 for t in open_sell_bids]))
        # Sort by price
        open_sell_bids.sort(key=lambda x: x.price, reverse=False)
        open_buy_bids.sort(key=lambda x: x.price, reverse=True)
        closed_sell_bids = []
        closed_buy_bids = []
        #print([s.price for s in open_sell_bids[:5]])
        #print([s.price for s in open_buy_bids[:5]])
        #print("Number of zero volume bids: ", sum([1 if int(b.volume)==0 else 0 for b in open_sell_bids]))
        
        # Assumptions/features: 
            # Assume bids only arrive in x min blocks
            # Do reduce volume of the same bid in other zones
            # Spread goes to the most recent bid
            # Only partial clearing
            # Killing within the timeslot does not affect bid clearing. Not really a problem when timeslot size -> 0
            # Interzonal / intrazonal trades okay, but currently, no trade is allowed across country borders
        error_check = True
        for j,s in enumerate(open_sell_bids):
            if(s.order_id == "1093895026"):
                print("XXXXX\n*\n*\n*\n*\n*\n*\n*\n*\n*\n*BID INCOMING")
            if(s.price > open_buy_bids[0].price):
                if(error_check):# and s.order_id == "1093838479" and open_buy_bids[0].order_id == "1093831291"):
                    print("For", j, "break immediately.","*", s.order_id, s.isOpen, s.volume > 0, 0 < len(open_buy_bids), s.price, open_buy_bids[0].price)
                break
            if(s.isOpen==False):
                if(error_check):# and s.order_id == "1093850495" and open_buy_bids[0].order_id == "1093833502"):
                    print("For", j, "break immediately bc already closed","*", s.order_id)
                break
            # To be absolutely sure that on cleared or killed bids appear in transactions
            if(s.isOpen == True):
                buy_bid_iterator = 0
                if(False and error_check):# and s.order_id == "1093838479" and open_buy_bids[buy_bid_iterator].order_id == "1093831291"):
                    print("Got here1", s.isOpen, s.volume > 0, 0 < len(open_buy_bids), s.price, open_buy_bids[0].price)
                
                if(False and error_check):# and s.order_id == "1093838479" and open_buy_bids[buy_bid_iterator].order_id == "1093831291"):
                    print("\nExamining bid 1093862967:", s.volume, s.price, open_buy_bids[buy_bid_iterator].price)
                while s.isOpen and s.volume > 0 and buy_bid_iterator < len(open_buy_bids):
                    if(False and error_check):# and s.order_id == "1093838479" and open_buy_bids[buy_bid_iterator].order_id == "1093831291"):
                        print("Gothere2", buy_bid_iterator, len(open_buy_bids), open_buy_bids[buy_bid_iterator].isOpen == True, (interzonal_trades_allowed or open_buy_bids[buy_bid_iterator].zone == s.zone), open_buy_bids[buy_bid_iterator].price >= s.price, ",", interzonal_trades_allowed, s.zone == open_buy_bids[buy_bid_iterator].zone)
                    if(open_buy_bids[buy_bid_iterator].price < s.price):
                        if(error_check):# and s.order_id == "1093838479" and open_buy_bids[buy_bid_iterator].order_id == "1093831291"):
                            print("For",j,"break at",buy_bid_iterator,"bc price too low","*", s.order_id, "*",open_buy_bids[buy_bid_iterator].order_id)
                        break
                    if(False and error_check):# and s.order_id == "1093838479" and open_buy_bids[buy_bid_iterator].order_id == "1093831291"):
                        print(buy_bid_iterator, timeslot, self.trading_end_time, interzonal_trades_allowed, s.price, open_buy_bids[buy_bid_iterator].price)
                        print(open_buy_bids[buy_bid_iterator].isOpen == True, (interzonal_trades_allowed or open_buy_bids[buy_bid_iterator].zone == s.zone), open_buy_bids[buy_bid_iterator].price >= s.price)
                    if(open_buy_bids[buy_bid_iterator].isOpen == True and (interzonal_trades_allowed or open_buy_bids[buy_bid_iterator].zone == s.zone) and open_buy_bids[buy_bid_iterator].price >= s.price):
                        if(False and error_check):# and s.order_id == "1093838479" and open_buy_bids[buy_bid_iterator].order_id == "1093831291"):
                            print(buy_bid_iterator, time, interzonal_trades_allowed, s.price, open_buy_bids[buy_bid_iterator].price)
                            print("Transaction to be created:", s.volume, s.price)
                        
                        if(error_check and False and self.printing_mode):
                            print("Transaction created!")
                        
                        
                        ### Create transaction
                        # Transaction attributes are given as stated below:
                        timestamp               = s.timestamp if s.timestamp > open_buy_bids[buy_bid_iterator].timestamp else open_buy_bids[buy_bid_iterator].timestamp # The transaction timestamp is equal to the timestamp of the most recent bid involved in the transaction
                        transaction_volume      = min(s.volume, open_buy_bids[buy_bid_iterator].volume) # The transaction volume is equal to the smallest residual volume of the involved bids
                        transaction_price       = float(s.price) if s.timestamp == timestamp else float(open_buy_bids[buy_bid_iterator].price) # Transaction price is equal to the price of the earliest placed bid
                        
                        # Add the new transaction to the list of transactions
                        self.transactions[time].append(Transaction(s, open_buy_bids[buy_bid_iterator], transaction_price, transaction_volume, timestamp)) 

                        s.reduce_volume(transaction_volume)
                        
                        if(int(s.volume) == 0):
                            open_sell_bids.remove(s)
                            closed_sell_bids.append(s)
                        
                        # Safety measure: Create a copy of the examined sell and buy bids to avoid unneccessary index errors (easy to make mistakes when removing elements from list etc)
                        comparison_sell_bid = Bid(s.price, s.volume, s.timestamp, s.timestamp2, True, s.order_id, False, s.zone)
                        comparison_buy_bid = Bid(open_buy_bids[buy_bid_iterator].price, open_buy_bids[buy_bid_iterator].volume, open_buy_bids[buy_bid_iterator].timestamp, open_buy_bids[buy_bid_iterator].timestamp2, True, open_buy_bids[buy_bid_iterator].order_id, False, open_buy_bids[buy_bid_iterator].zone)
                        
                        # Identify all bids with same order id
                        # Reduce their volumes and remove them if no residual volume
                        # Hypothesis: Try except is needed as we might remove a bid before trying to access it.
                        for t in open_sell_bids:
                            if(str(t.order_id) == str(comparison_sell_bid.order_id)):
                                t.reduce_volume(transaction_volume)
                                if(int(t.volume) == 0):
                                    open_sell_bids.remove(t)
                                    closed_sell_bids.append(t)
                                
                        for i,t in enumerate(open_buy_bids):
                            if(comparison_buy_bid.order_id == t.order_id):
                                t.reduce_volume(transaction_volume)
                                if(int(t.volume) == 0):
                                    open_buy_bids.remove(t)
                                    closed_buy_bids.append(t)
                                    # Necessary to decrease iterator?
                                    if(i < buy_bid_iterator):
                                        buy_bid_iterator -= 1
    
                    # Increase iterator to go to compare the bid to the next buy bid in the list
                    buy_bid_iterator = buy_bid_iterator + 1
                
                    if(buy_bid_iterator == len(open_buy_bids)):
                        if(error_check):# and s.order_id == "1093838479" and open_buy_bids[buy_bid_iterator].order_id == "1093831291"):
                            print("For",j,"break at end", s.order_id, open_buy_bids[buy_bid_iterator].order_id)
                        break
                    elif(int(s.volume) == 0):
                        if(error_check):# and s.order_id == "1093838479" and open_buy_bids[buy_bid_iterator].order_id == "1093831291"):
                            print("For",j,"break at",buy_bid_iterator,"bc no residual volume", "*",s.order_id,"*", open_buy_bids[buy_bid_iterator].order_id)
                    
                    elif(s.isOpen == False):
                        if(error_check):# and s.order_id == "1093838479" and open_buy_bids[buy_bid_iterator].order_id == "1093831291"):
                            print("For",j,"break at",buy_bid_iterator,"bc not open (Should not happen)","*", s.order_id, "*",open_buy_bids[buy_bid_iterator].order_id)
                        raise ValueError("See above")
                if(int(s.volume) == 0 and s in open_sell_bids):
                    closed_sell_bids.append(s)
                    open_sell_bids.remove(s)
                    
        # Additional layer of safety: fill these datastructures with only the correct bids
        # (No bids that should have been removed)
        open_s = []
        open_b = []
        junk = []
        closed_sell_bids_order_ids = [x.order_id for x in closed_sell_bids]
        closed_buy_bids_order_ids = [x.order_id for x in closed_buy_bids]
        #print("CSBOID:", len(closed_sell_bids_order_ids))
        
        open_sell_bids.sort(key=lambda x: x.price, reverse=False)
        open_buy_bids.sort(key=lambda x: x.price, reverse=True)
        
        if(len(open_sell_bids) > 0 and len(open_buy_bids) > 0 and open_sell_bids[0].price < open_buy_bids[0].price):
            print("Something is wrong... ", interzonal_trades_allowed, open_sell_bids[0].price, open_buy_bids[0].price, open_sell_bids[0].volume, open_buy_bids[0].volume, open_sell_bids[0].zone, open_buy_bids[0].zone, open_sell_bids[0].order_id, open_buy_bids[0].order_id)
        
        
        # Post-transaction processing: if some of the bids have not been removed from the open_*_bids lists, 
        # though being present in closed_*_bids, they are not added in the open_* lists
        for x,s in enumerate(open_sell_bids):
            if(s in closed_sell_bids or s.order_id in closed_sell_bids_order_ids or s.isOpen == False or float(s.volume) == 0.0):
                #print("Not removed properly")
                junk.append(s)
                open_sell_bids.remove(s)
            else:
                open_s.append(s)
                
        for x,s in enumerate(open_buy_bids):
            if(s in closed_buy_bids or s.order_id in closed_buy_bids_order_ids or s.isOpen == False or float(s.volume) == 0.0):
                junk.append(s)
                open_buy_bids.remove(s)
            else:
                open_b.append(s)
        #raise ValueError("End code")
        # Return the new order lists
        return open_s, open_b

    # Not yet implemented.
    def print_bid_curves(self, buy_bids, sell_bids):

        return 0
        # Inspiration
        if(False):
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

	# Removes the killed bids of a bid array
    def remove_killed_bids(self, bids,timestamp):
        # For each bid with zero volume, remove all other bids with identical order id
        #zero_volume_bids     = []
        #zero_volume_order_ids = []
        
        killed_bids         = []
        local_bids = [b for b in bids[:]]
        for b in bids:
            if(float(b.volume) == 0.0):
                killed_bids.append(b)
                local_bids.remove(b)
                b.kill_bid(timestamp)

        # Previous implementation (outdated)
        #for k in zero_volume_bids:
        #    for b in bids:
        #        if(k.order_id == b.order_id):
        #            killed_bids.append(k)
        #            bids.remove(b)
        #            b.kill_bid(timestamp)

        return local_bids, killed_bids
    
    def remove_killed_bids2(self, bids, new_bid):
        killed_bids = []
        temp_bids =[]
        
        for b in bids:
            if(b.order_id == new_bid.order_id):
                b.kill_bid(new_bid.timestamp)
                killed_bids.append(b)
            else:
                temp_bids.append(b)
                
        return temp_bids, killed_bids


    def print_transactions(self, mode):
        for s in range(self.stages):
            for t in self.transactions[s]:
                if(mode=="price"):
                    print(t.price/100)
                else:
                    print("Transaction created " + str(t.timestamp) + " with price: " + str(t.price) + ", transaction volume: " + str(t.volume))

    def write_transactions(self):
        book = xlsxwriter.Workbook(self.transaction_file_name)
        sheet = book.add_worksheet("Transactions")

        # Fill spreadsheet
        headers = ["Timestamp", "Price", "Volume", "Buy bid", "Sell bid"]
        for h in range(len(headers)):
            sheet.write(0, h, headers[h])

        for t in range(self.stages):
            for i in range(len(self.transactions[t])):
                sheet.write(i+1, 0, self.transactions[t][i].timestamp)
                sheet.write(i+1, 1, self.transactions[t][i].price)
                sheet.write(i+1, 2, self.transactions[t][i].volume)
                sheet.write(i+1, 3, self.transactions[i].buy_bid.order_id)
                sheet.write(i+1, 4, self.transactions[i].sell_bid.order_id)
            
        book.close()

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
        
        open_buy_bids = []
        open_sell_bids = []
        
        next_aggregation = self.trading_start_time + datetime.timedelta(minutes=10)
        
        while(buy_bid_iterator < number_of_buy_bids and sell_bid_iterator < number_of_sell_bids):
            
            # Fetch new bid
            if(buy_bid_iterator_most_recently_changed == True):
                new_bid = (buy_bids[buy_bid_iterator])
            else:
                new_bid = (sell_bids[sell_bid_iterator])
                        
            # If this bid has volume 0, kill all bids with equal order id
            if(float(new_bid.volume) == 0.0):
                if(buy_bid_iterator_most_recently_changed == True):
                    open_buy_bids, killed_buy_bids = self.remove_killed_bid2s(open_buy_bids, new_bid.timestamp)
                else:
                    open_sell_bids, killed_sell_bids = self.remove_killed_bids2(open_sell_bids, new_bid.timestamp)
                
            # Otherwise, look for potential transactions involving this bid
            else:
                if(buy_bid_iterator_most_recently_changed == True):
                    open_sell_bids, new_bid_after_transactions = self.create_transactionsv2(open_sell_bids, [new_bid], new_bid.timestamp, new_bid.timestamp < self.trading_end_time)
                else:
                    open_sell_bids, new_bid_after_transactions = self.create_transactionsv2([new_bid], open_buy_bids, new_bid.timestamp, new_bid.timestamp < self.trading_end_time)
                
            
            # Find out which bid to update next
            if(buy_bids[buy_bid_iterator + 1].timestamp < sell_bids[sell_bid_iterator + 1].timestamp):
                buy_bid_iterator += 1
                buy_bid_iterator_most_recently_changed = True
            else:
                sell_bid_iterator += 1
                buy_bid_iterator_most_recently_changed = False
                
            # If the time is right, aggregate the data:
            if(new_bid.timestamp > next_aggregation):
                
                avg_transaction_price
                max_transaction_price
                min_transaction_price
                
                
                unique_buy_orders_in_transactions    = "N/A"
                unique_sell_orders_in_transactions    = "N/A"
                
                self.stats.append([timeslot, avg_transaction_price, max_transaction_price, min_transaction_price, max_open_bid_order_price, min_open_ask_order_price, transaction_volume, open_buy_bid_volumes, open_sell_bid_volumes, killed_buy_volume, killed_sell_volume, len(self.transactions[t]), unique_buy_orders_in_transactions, unique_sell_orders_in_transactions, avg_buy_order_maturity, avg_sell_order_maturity] + open_buy_bids_volumes + open_buy_bids_prices + open_sell_bids_volumes + open_sell_bids_prices)
                
                next_aggregation = next_aggregation + datetime.timedelta(minutes=10)
        
    
    def initandrun(self):
        # Data structure instantiation
        self.trading_timeslots     = self.setup_timeslots(self.trading_start_time, self.trading_end_time, True)  # List of timeslots
        self.main_trading_timeslots     = self.setup_timeslots(self.trading_start_time, self.trading_end_time, False)  # List of timeslots
        data                     = self.read_data(self.bid_file_tag)
        
        if(self.customer_mode == True):
            customer_bid_array = self.read_data(self.customer_bid_file_tag)
        else:
            customer_bid_array     = []

        # Create lists of bids
        buy_bids, sell_bids     = self.filter_data(data, False)
        _, customer_bids         = self.filter_data(customer_bid_array, True)
        
        # Add customer's bids to list of bids
        sell_bids += customer_bids

        # Sort bids by their time blocks
        #print(len(sell_bids), len(buy_bids))
        sell_bid_dict             = self.create_bid_dictionary(sell_bids, self.trading_timeslots)
        buy_bid_dict             = self.create_bid_dictionary(buy_bids, self.trading_timeslots)

        # Stats headers
        self.stats                 = [["Trading timeslot", "Avg transaction price", "Max transaction price", "Min transaction price", "Max open bid order price", "Min open ask order price", "Transaction volume", "Open bid volumes", "Open ask volumes", "Killed buy order volumes", "Killed sell order volumes", "Number of transactions", "Number of unique bid orders in transactions", "Number of unique ask orders in transactions", "Average buy order maturity", "Average sell order maturity"] + ["B.V"+str(i) for i in range(1,6)] + ["B.P"+str(i) for i in range(1,6)]+ ["S.V"+str(i) for i in range(1,6)] + ["S.P"+str(i) for i in range(1,6)]]
        #print(sum([len(sell_bid_dict[k]) for k in list(sell_bid_dict.keys())]))
        #print(sum([len(buy_bid_dict[k]) for k in list(buy_bid_dict.keys())]))
        for t, timeslot in enumerate(self.main_trading_timeslots):
            n_transactions = sum([len(self.transactions[(t-1)*self.number_of_minis_per_timeslot + mini_timeslot]) for mini_timeslot in range(self.number_of_minis_per_timeslot)])
            print("Timeslot: ",timeslot, len(self.open_sell_bids), sum([len(sell_bid_dict[timeslot + self.mini_time_inc * mini_timeslot]) for mini_timeslot in range(self.number_of_minis_per_timeslot)]),len(self.open_buy_bids), sum([len(buy_bid_dict[timeslot + self.mini_time_inc * mini_timeslot]) for mini_timeslot in range(self   .number_of_minis_per_timeslot)]), n_transactions)
            if(False and t == 18):
                print("\n\n\nTimeslot: " + str(timeslot) + " (Iteration " + str(t) + ")")
                
            if(self.mini_time_inc_mode == True):
                for mini_timeslot in range(self.number_of_minis_per_timeslot):
                    #for mini_t in range()
                    self.open_sell_bids += sell_bid_dict[timeslot + self.mini_time_inc * mini_timeslot]
                    self.open_buy_bids += buy_bid_dict[timeslot + self.mini_time_inc * mini_timeslot]
                    #print("A",len(self.open_sell_bids), len(self.open_buy_bids))
                    ### Remove killed bids
                    self.open_sell_bids, killed_sell_bids     = self.remove_killed_bids(self.open_sell_bids, timeslot + self.mini_time_inc * mini_timeslot)
                    self.open_buy_bids, killed_buy_bids        = self.remove_killed_bids(self.open_buy_bids, timeslot + self.mini_time_inc * mini_timeslot)
                    
                    ### Print bid curves
                    self.print_bid_curves(self.open_buy_bids, self.open_sell_bids)
                    
                    ### Create transactions and update bid portfolio
                    #print("Creating transactions:", "1093838479" in [b.order_id for b in self.open_sell_bids])
                    self.open_sell_bids, self.open_buy_bids = self.create_transactions(self.open_sell_bids, self.open_buy_bids, t * self.number_of_minis_per_timeslot + mini_timeslot, timeslot + self.mini_time_inc * mini_timeslot, timeslot + self.mini_time_inc * mini_timeslot < self.trading_end_time)
                    #print("B",len(self.open_sell_bids), len(self.open_buy_bids))
                    #Testing            
                    if(self.testing_mode):
                        self.open_sell_bids.sort(key=lambda x: x.volume, reverse=False)
                        for x in self.open_buy_bids:
                            if(x.volume == 0):
                                print("ERROR: Zero volume bid in open_sell_bids after killing and clearing")
                                print(x.timestamp, x.timestamp2, x.volume, x.price, x.zone, x.isBuy)
                            elif(x.volume > 0):
                                break
                        
                        for x in self.open_sell_bids:
                            if(x.volume == 0):
                                print("ERROR: Zero volume bid in open_sell_bids after killing and clearing")
                                print(x.timestamp, x.timestamp2, x.volume, x.price, x.zone, x.isBuy)
                            elif(x.volume > 0):
                                break
                    
                    ### Sort bid arrays
                    self.open_buy_bids.sort(key=lambda x: x.price, reverse=True)
                    self.open_sell_bids.sort(key=lambda x: x.price, reverse=False)
                                
                    
                    ### Print bid curves
                    self.print_bid_curves(self.open_buy_bids, self.open_sell_bids)
                    if(False and self.printing_mode):
                        print("Timestep " + str(timeslot) + " bid order depth " + str(len(self.closed_sell_bids))+"/"+str(len(self.open_sell_bids)) +"-"+ str(len(self.closed_buy_bids))+"/"+str(len(self.open_buy_bids)))
                    if(self.collect_aggregate_data == False):
                        ### Collect data for table
                        # If no transactions are created, set transaction attributes equal to N/A
                        
                        if(len(self.transactions[t * self.number_of_minis_per_timeslot + mini_timeslot]) > 0):
                            avg_transaction_price            = sum(self.transactions[t * self.number_of_minis_per_timeslot + mini_timeslot][i].price             for i in range(len(self.transactions[t * self.number_of_minis_per_timeslot + mini_timeslot]))) / len(self.transactions[t * self.number_of_minis_per_timeslot + mini_timeslot])
                            min_transaction_price            = min(self.transactions[t * self.number_of_minis_per_timeslot + mini_timeslot][i].price             for i in range(len(self.transactions[t * self.number_of_minis_per_timeslot + mini_timeslot])))
                            max_transaction_price            = max(self.transactions[t * self.number_of_minis_per_timeslot + mini_timeslot][i].price             for i in range(len(self.transactions[t * self.number_of_minis_per_timeslot + mini_timeslot])))
                            transaction_volume                = sum(self.transactions[t * self.number_of_minis_per_timeslot + mini_timeslot][i].volume             for i in range(len(self.transactions[t * self.number_of_minis_per_timeslot + mini_timeslot])))
                            
                        else:
                            avg_transaction_price            = "N/A"
                            min_transaction_price            = "N/A"
                            max_transaction_price            = "N/A"
                            transaction_volume                = "N/A"
            
                        if(len(self.open_buy_bids) > 0):
                            
                            self.open_buy_bids.sort(key=lambda x: x.price, reverse=True)
                            
                            max_open_bid_order_price         = float(self.open_buy_bids[0].price)
                            open_buy_bid_volumes            = sum(b.volume                                     for b in self.open_buy_bids)
                            avg_buy_order_maturity            = sum(b.compute_maturity(timeslot + mini_timeslot * self.mini_time_inc).total_seconds()                 for b in self.open_buy_bids)    / len(self.open_buy_bids)
                            
                            open_buy_bids_prices = []
                            open_buy_bids_volumes = []
                            order_ids_of_used_bids = []
                            i = 0
                            
                            while (len(open_buy_bids_prices) <= 5):
                                try:
                                    if(self.open_buy_bids[i].order_id not in order_ids_of_used_bids):
                                        open_buy_bids_prices.append(self.open_buy_bids[i].price)
                                        open_buy_bids_volumes.append(self.open_buy_bids[i].volume)
                                except:
                                    open_buy_bids_prices.append("N/A")
                                    open_buy_bids_volumes.append("N/A")
                                    order_ids_of_used_bids.append("N/A")
                                finally:
                                    i += 1
                            
                        
                        else:
                            max_open_bid_order_price        = "N/A"
                            open_buy_bid_volumes            = "N/A"
                            avg_buy_order_maturity            = "N/A"
                            open_buy_bids_prices = ["N/A" for b in range(5)]
                            open_buy_bids_volumes = ["N/A" for b in range(5)]
                            
                        if(len(self.open_sell_bids) > 0):
                            self.open_sell_bids.sort(key=lambda x: x.price, reverse=False)
                            
                            min_open_ask_order_price         = float(self.open_sell_bids[0].price)
                            open_sell_bid_volumes            = sum(b.volume                                     for b in self.open_sell_bids)
                            avg_sell_order_maturity            = sum(b.compute_maturity(timeslot + mini_timeslot * self.mini_time_inc).total_seconds()                for b in self.open_sell_bids)     / len(self.open_sell_bids)
                            
                            open_sell_bids_prices = []
                            open_sell_bids_volumes = []
                            order_ids_of_used_bids = []
                            i = 0
                            
                            while (len(open_sell_bids_prices) <= 5):
                                try:
                                    if(self.open_buy_bids[i].order_id not in order_ids_of_used_bids):
                                        open_sell_bids_prices.append(self.open_sell_bids[i].price)
                                        open_sell_bids_volumes.append(self.open_sell_bids[i].volume)
                                except:
                                    open_sell_bids_prices.append("N/A")
                                    open_sell_bids_volumes.append("N/A")
                                    order_ids_of_used_bids.append("N/A")
                                finally:
                                    i += 1
                            
                        
                        else:
                            min_open_ask_order_price        = "N/A"
                            open_sell_bid_volumes            = "N/A"
                            avg_sell_order_maturity            = "N/A"
                            open_sell_bids_prices = ["N/A" for b in range(5)]
                            open_sell_bids_volumes = ["N/A" for b in range(5)]
                        
                        #print(sum(1 for x in self.open_buy_bids) / max(1,sum(1 for y in self.open_sell_bids)))
                        
                        if(max_open_bid_order_price != "N/A" and min_open_ask_order_price != "N/A"):
                            if(float(max_open_bid_order_price) > float(min_open_ask_order_price)):
                                if(self.open_buy_bids[0].zone == self.open_sell_bids[0].zone or timeslot < self.trading_timeslots[-1] - datetime.timedelta(minutes=self.interzonal_gc)):
                                    print("\n\n\n\n\n\n interzonal bids allowed:", timeslot + mini_timeslot * self.mini_time_inc, timeslot + mini_timeslot * self.mini_time_inc < self.trading_timeslots[-1] - datetime.timedelta(minutes=self.interzonal_gc) or self.open_buy_bids[0].zone == self.open_sell_bids[0].zone, len(self.open_sell_bids), len(self.open_buy_bids), "Buy OD:", sum(float(x.volume) for x in self.open_buy_bids))
                                    print("Max buy bid:", self.open_buy_bids[0].volume, self.open_buy_bids[0].price, self.open_buy_bids[0].isOpen, self.open_buy_bids[0].zone, self.open_buy_bids[0].order_id)
                                    print("Min sell bid:", self.open_sell_bids[0].volume, self.open_sell_bids[0].price, self.open_sell_bids[0].isOpen, self.open_sell_bids[0].zone, self.open_sell_bids[0].order_id)
                        
                        if(len(self.open_sell_bids) > 0 and len(self.open_buy_bids) > 0):
                            #if(max_open_bid_order_price >= min_open_ask_order_price and self.open_sell_bids[0]):
                            #    print("\nSell:",self.open_sell_bids[0].order_id, self.open_sell_bids[0].timestamp, self.open_sell_bids[0].volume, self.open_sell_bids[0].price, self.open_sell_bids[0].isOpen, self.open_sell_bids[0].zone)
                            #    print("Buy:",self.open_buy_bids[0].order_id, self.open_buy_bids[0].timestamp, self.open_buy_bids[0].volume, self.open_buy_bids[0].price, self.open_buy_bids[0].isOpen, self.open_buy_bids[0].zone)
                            top_10_buy_bid_prices = []
                            sell_bids_with_better_price_than_top_10_buy_bids = []
            
            
                        no_killed_buy_bids                    = len(killed_buy_bids)
                        no_killed_sell_bids                    = len(killed_sell_bids)
                        
                        if(no_killed_buy_bids > 0):
                            killed_buy_volume                 = sum(b.volume                                     for b in killed_buy_bids)
                        
                        else:
                            killed_buy_volume                = 0
            
                        if(no_killed_sell_bids > 0):
                            killed_sell_volume                = sum(b.volume                                     for b in killed_sell_bids)
                        
                        else:
                            killed_sell_volume                 = 0
                        
                        unique_buy_orders_in_transactions    = "N/A"
                        unique_sell_orders_in_transactions    = "N/A"
                        
                        self.stats.append([timeslot, avg_transaction_price, max_transaction_price, min_transaction_price, max_open_bid_order_price, min_open_ask_order_price, transaction_volume, open_buy_bid_volumes, open_sell_bid_volumes, killed_buy_volume, killed_sell_volume, len(self.transactions[t]), unique_buy_orders_in_transactions, unique_sell_orders_in_transactions, avg_buy_order_maturity, avg_sell_order_maturity] + open_buy_bids_volumes + open_buy_bids_prices + open_sell_bids_volumes + open_sell_bids_prices)
                if(self.collect_aggregate_data == True):
                    ### Collect data for table
                    # If no transactions are created, set transaction attributes equal to N/A
                    number_of_transactions = sum([len(self.transactions[t * self.number_of_minis_per_timeslot + mini_timeslot]) for mini_timeslot in range(self.number_of_minis_per_timeslot)])
                    if(number_of_transactions > 0):
                        avg_transaction_price            = sum(self.transactions[t * self.number_of_minis_per_timeslot + mini_timeslot][i].price             for mini_timeslot in range(self.number_of_minis_per_timeslot) for i in range(len(self.transactions[t * self.number_of_minis_per_timeslot + mini_timeslot]))) / number_of_transactions
                        min_transaction_price            = min(self.transactions[t * self.number_of_minis_per_timeslot + mini_timeslot][i].price             for mini_timeslot in range(self.number_of_minis_per_timeslot) for i in range(len(self.transactions[t * self.number_of_minis_per_timeslot + mini_timeslot])))
                        max_transaction_price            = max(self.transactions[t * self.number_of_minis_per_timeslot + mini_timeslot][i].price             for mini_timeslot in range(self.number_of_minis_per_timeslot) for i in range(len(self.transactions[t * self.number_of_minis_per_timeslot + mini_timeslot])))
                        transaction_volume                = sum(self.transactions[t * self.number_of_minis_per_timeslot + mini_timeslot][i].volume           for mini_timeslot in range(self.number_of_minis_per_timeslot) for i in range(len(self.transactions[t * self.number_of_minis_per_timeslot + mini_timeslot])))
                        
                    else:
                        avg_transaction_price            = "N/A"
                        min_transaction_price            = "N/A"
                        max_transaction_price            = "N/A"
                        transaction_volume                = "N/A"
        
                    if(len(self.open_buy_bids) > 0):
                        
                        self.open_buy_bids.sort(key=lambda x: x.price, reverse=True)
                        
                        max_open_bid_order_price         = float(self.open_buy_bids[0].price)
                        open_buy_bid_volumes            = sum(b.volume                                     for b in self.open_buy_bids)
                        avg_buy_order_maturity            = sum(b.compute_maturity(timeslot + mini_timeslot * self.mini_time_inc).total_seconds()             for mini_timeslot in range(self.number_of_minis_per_timeslot) for b in self.open_buy_bids)    / len(self.open_buy_bids)
                        
                        open_buy_bids_prices = []
                        open_buy_bids_volumes = []
                        order_ids_of_used_bids = []
                        i = 0
                        
                        while (len(open_buy_bids_prices) <= 5):
                            try:
                                if(self.open_buy_bids[i].order_id not in order_ids_of_used_bids):
                                    open_buy_bids_prices.append(self.open_buy_bids[i].price)
                                    open_buy_bids_volumes.append(self.open_buy_bids[i].volume)
                            except:
                                open_buy_bids_prices.append("N/A")
                                open_buy_bids_volumes.append("N/A")
                                order_ids_of_used_bids.append("N/A")
                            finally:
                                i += 1
                        
                    
                    else:
                        max_open_bid_order_price        = "N/A"
                        open_buy_bid_volumes            = "N/A"
                        avg_buy_order_maturity            = "N/A"
                        open_buy_bids_prices = ["N/A" for b in range(5)]
                        open_buy_bids_volumes = ["N/A" for b in range(5)]
                        
                    if(len(self.open_sell_bids) > 0):
                        self.open_sell_bids.sort(key=lambda x: x.price, reverse=False)
                        
                        min_open_ask_order_price         = float(self.open_sell_bids[0].price)
                        open_sell_bid_volumes            = sum(b.volume                                     for b in self.open_sell_bids)
                        avg_sell_order_maturity            = sum(b.compute_maturity(timeslot + mini_timeslot * self.mini_time_inc).total_seconds()               for mini_timeslot in range(self.number_of_minis_per_timeslot) for b in self.open_sell_bids)     / len(self.open_sell_bids)
                        
                        open_sell_bids_prices = []
                        open_sell_bids_volumes = []
                        order_ids_of_used_bids = []
                        i = 0
                        
                        while (len(open_sell_bids_prices) <= 5):
                            try:
                                if(self.open_buy_bids[i].order_id not in order_ids_of_used_bids):
                                    open_sell_bids_prices.append(self.open_sell_bids[i].price)
                                    open_sell_bids_volumes.append(self.open_sell_bids[i].volume)
                            except:
                                open_sell_bids_prices.append("N/A")
                                open_sell_bids_volumes.append("N/A")
                                order_ids_of_used_bids.append("N/A")
                            finally:
                                i += 1
                        
                    
                    else:
                        min_open_ask_order_price        = "N/A"
                        open_sell_bid_volumes            = "N/A"
                        avg_sell_order_maturity            = "N/A"
                        open_sell_bids_prices = ["N/A" for b in range(5)]
                        open_sell_bids_volumes = ["N/A" for b in range(5)]
                    
                    #print(sum(1 for x in self.open_buy_bids) / max(1,sum(1 for y in self.open_sell_bids)))
                    
                    if(max_open_bid_order_price != "N/A" and min_open_ask_order_price != "N/A"):
                        if(float(max_open_bid_order_price) > float(min_open_ask_order_price)):
                            if(self.open_buy_bids[0].zone == self.open_sell_bids[0].zone or timeslot + mini_timeslot * self.mini_time_inc < self.trading_timeslots[-1] - datetime.timedelta(minutes=self.interzonal_gc)):
                                print("\n\n\n\n\n\n Interzonal bids allowed:", timeslot + mini_timeslot * self.mini_time_inc, timeslot + mini_timeslot * self.mini_time_inc < self.trading_timeslots[-1] - datetime.timedelta(minutes=self.interzonal_gc) or self.open_buy_bids[0].zone == self.open_sell_bids[0].zone, len(self.open_sell_bids), len(self.open_buy_bids), "Buy OD:", sum(float(x.volume) for x in self.open_buy_bids))
                                print("Max buy bid:", self.open_buy_bids[0].volume, self.open_buy_bids[0].price, self.open_buy_bids[0].isOpen, self.open_buy_bids[0].zone, self.open_buy_bids[0].order_id)
                                print("Min sell bid:", self.open_sell_bids[0].volume, self.open_sell_bids[0].price, self.open_sell_bids[0].isOpen, self.open_sell_bids[0].zone, self.open_sell_bids[0].order_id)
                    
                    if(len(self.open_sell_bids) > 0 and len(self.open_buy_bids) > 0):
                        #if(max_open_bid_order_price >= min_open_ask_order_price and self.open_sell_bids[0]):
                        #    print("\nSell:",self.open_sell_bids[0].order_id, self.open_sell_bids[0].timestamp, self.open_sell_bids[0].volume, self.open_sell_bids[0].price, self.open_sell_bids[0].isOpen, self.open_sell_bids[0].zone)
                        #    print("Buy:",self.open_buy_bids[0].order_id, self.open_buy_bids[0].timestamp, self.open_buy_bids[0].volume, self.open_buy_bids[0].price, self.open_buy_bids[0].isOpen, self.open_buy_bids[0].zone)
                        top_10_buy_bid_prices = []
                        sell_bids_with_better_price_than_top_10_buy_bids = []
        
        
                    no_killed_buy_bids                    = len(killed_buy_bids)
                    no_killed_sell_bids                    = len(killed_sell_bids)
                    
                    if(no_killed_buy_bids > 0):
                        killed_buy_volume                 = sum(b.volume                                     for b in killed_buy_bids)
                    
                    else:
                        killed_buy_volume                = 0
        
                    if(no_killed_sell_bids > 0):
                        killed_sell_volume                = sum(b.volume                                     for b in killed_sell_bids)
                    
                    else:
                        killed_sell_volume                 = 0
                    
                    unique_buy_orders_in_transactions    = "N/A"
                    unique_sell_orders_in_transactions    = "N/A"
                    
                    self.stats.append([timeslot, avg_transaction_price, max_transaction_price, min_transaction_price, max_open_bid_order_price, min_open_ask_order_price, transaction_volume, open_buy_bid_volumes, open_sell_bid_volumes, killed_buy_volume, killed_sell_volume, len(self.transactions[t]), unique_buy_orders_in_transactions, unique_sell_orders_in_transactions, avg_buy_order_maturity, avg_sell_order_maturity] + open_buy_bids_volumes + open_buy_bids_prices + open_sell_bids_volumes + open_sell_bids_prices)

            else:
                self.open_sell_bids += sell_bid_dict[timeslot]
                self.open_buy_bids += buy_bid_dict[timeslot]
    
                ### Remove killed bids
                self.open_sell_bids, killed_sell_bids     = self.remove_killed_bids(self.open_sell_bids, timeslot)
                self.open_buy_bids, killed_buy_bids        = self.remove_killed_bids(self.open_buy_bids, timeslot)
                
                ### Print bid curves
                self.print_bid_curves(self.open_buy_bids, self.open_sell_bids)
                
                ### Create transactions and update bid portfolio
                self.open_sell_bids, self.open_buy_bids = self.create_transactions(self.open_sell_bids, self.open_buy_bids, t, timeslot, timeslot < self.trading_end_time)
    
                #Testing            
                if(self.testing_mode):
                    self.open_sell_bids.sort(key=lambda x: x.volume, reverse=False)
                    for x in self.open_buy_bids:
                        if(x.volume == 0):
                            print("ERROR: Zero volume bid in open_sell_bids after killing and clearing")
                            print(x.timestamp, x.timestamp2, x.volume, x.price, x.zone, x.isBuy)
                        elif(x.volume > 0):
                            break
                    
                    for x in self.open_sell_bids:
                        if(x.volume == 0):
                            print("ERROR: Zero volume bid in open_sell_bids after killing and clearing")
                            print(x.timestamp, x.timestamp2, x.volume, x.price, x.zone, x.isBuy)
                        elif(x.volume > 0):
                            break
                
                ### Sort bid arrays
                self.open_buy_bids.sort(key=lambda x: x.price, reverse=True)
                self.open_sell_bids.sort(key=lambda x: x.price, reverse=False)
                            
                
                ### Print bid curves
                self.print_bid_curves(self.open_buy_bids, self.open_sell_bids)
                if(True and self.printing_mode):
                    print("Timestep " + str(timeslot) + " bid order depth " + str(len(self.closed_sell_bids))+"/"+str(len(self.open_sell_bids)) +"-"+ str(len(self.closed_buy_bids))+"/"+str(len(self.open_buy_bids)))
    
                ### Collect data for table
                # If no transactions are created, set transaction attributes equal to N/A
                if(len(self.transactions[t]) > 0):
                    avg_transaction_price            = sum(self.transactions[t][i].price             for i in range(len(self.transactions[t]))) / len(self.transactions[t])
                    min_transaction_price            = min(self.transactions[t][i].price             for i in range(len(self.transactions[t])))
                    max_transaction_price            = max(self.transactions[t][i].price             for i in range(len(self.transactions[t])))
                    transaction_volume                = sum(self.transactions[t][i].volume             for i in range(len(self.transactions[t])))
                    
                else:
                    avg_transaction_price            = "N/A"
                    min_transaction_price            = "N/A"
                    max_transaction_price            = "N/A"
                    transaction_volume                = "N/A"
    
                if(len(self.open_buy_bids) > 0):
                    
                    self.open_buy_bids.sort(key=lambda x: x.price, reverse=True)
                    
                    max_open_bid_order_price         = float(self.open_buy_bids[0].price)
                    open_buy_bid_volumes            = sum(b.volume                                     for b in self.open_buy_bids)
                    avg_buy_order_maturity            = sum(b.compute_maturity(timeslot).total_seconds()                 for b in self.open_buy_bids)    / len(self.open_buy_bids)
                    
                    open_buy_bids_prices = []
                    open_buy_bids_volumes = []
                    order_ids_of_used_bids = []
                    i = 0
                    
                    while (len(open_buy_bids_prices) <= 5):
                        try:
                            if(self.open_buy_bids[i].order_id not in order_ids_of_used_bids):
                                open_buy_bids_prices.append(self.open_buy_bids[i].price)
                                open_buy_bids_volumes.append(self.open_buy_bids[i].volume)
                        except:
                            open_buy_bids_prices.append("N/A")
                            open_buy_bids_volumes.append("N/A")
                            order_ids_of_used_bids.append("N/A")
                        finally:
                            i += 1
                    
                
                else:
                    max_open_bid_order_price        = "N/A"
                    open_buy_bid_volumes            = "N/A"
                    avg_buy_order_maturity            = "N/A"
                    open_buy_bids_prices = ["N/A" for b in range(5)]
                    open_buy_bids_volumes = ["N/A" for b in range(5)]
                    
                if(len(self.open_sell_bids) > 0):
                    self.open_sell_bids.sort(key=lambda x: x.price, reverse=False)
                    
                    min_open_ask_order_price         = float(self.open_sell_bids[0].price)
                    open_sell_bid_volumes            = sum(b.volume                                     for b in self.open_sell_bids)
                    avg_sell_order_maturity            = sum(b.compute_maturity(timeslot).total_seconds()                for b in self.open_sell_bids)     / len(self.open_sell_bids)
                    
                    open_sell_bids_prices = []
                    open_sell_bids_volumes = []
                    order_ids_of_used_bids = []
                    i = 0
                    
                    while (len(open_sell_bids_prices) <= 5):
                        try:
                            if(self.open_buy_bids[i].order_id not in order_ids_of_used_bids):
                                open_sell_bids_prices.append(self.open_sell_bids[i].price)
                                open_sell_bids_volumes.append(self.open_sell_bids[i].volume)
                        except:
                            open_sell_bids_prices.append("N/A")
                            open_sell_bids_volumes.append("N/A")
                            order_ids_of_used_bids.append("N/A")
                        finally:
                            i += 1
                    
                
                else:
                    min_open_ask_order_price        = "N/A"
                    open_sell_bid_volumes            = "N/A"
                    avg_sell_order_maturity            = "N/A"
                    open_sell_bids_prices = ["N/A" for b in range(5)]
                    open_sell_bids_volumes = ["N/A" for b in range(5)]
                
                #print(sum(1 for x in self.open_buy_bids) / max(1,sum(1 for y in self.open_sell_bids)))
                
                if(max_open_bid_order_price != "N/A" and min_open_ask_order_price != "N/A"):
                    if(float(max_open_bid_order_price) > float(min_open_ask_order_price)):
                        if(self.open_buy_bids[0].zone == self.open_sell_bids[0].zone or timeslot < self.trading_timeslots[-1] - datetime.timedelta(minutes=self.interzonal_gc)):
                            print("\n\n\n\n\n\n Interzonal bids allowed:", timeslot, timeslot < self.trading_timeslots[-1] - datetime.timedelta(minutes=self.interzonal_gc) or self.open_buy_bids[0].zone == self.open_sell_bids[0].zone, len(self.open_sell_bids), len(self.open_buy_bids), "Buy OD:", sum(float(x.volume) for x in self.open_buy_bids))
                            print("Max buy bid:", self.open_buy_bids[0].volume, self.open_buy_bids[0].price, self.open_buy_bids[0].isOpen, self.open_buy_bids[0].zone, self.open_buy_bids[0].order_id)
                            print("Min sell bid:", self.open_sell_bids[0].volume, self.open_sell_bids[0].price, self.open_sell_bids[0].isOpen, self.open_sell_bids[0].zone, self.open_sell_bids[0].order_id)
                
                if(len(self.open_sell_bids) > 0 and len(self.open_buy_bids) > 0):
                    #if(max_open_bid_order_price >= min_open_ask_order_price and self.open_sell_bids[0]):
                    #    print("\nSell:",self.open_sell_bids[0].order_id, self.open_sell_bids[0].timestamp, self.open_sell_bids[0].volume, self.open_sell_bids[0].price, self.open_sell_bids[0].isOpen, self.open_sell_bids[0].zone)
                    #    print("Buy:",self.open_buy_bids[0].order_id, self.open_buy_bids[0].timestamp, self.open_buy_bids[0].volume, self.open_buy_bids[0].price, self.open_buy_bids[0].isOpen, self.open_buy_bids[0].zone)
                    top_10_buy_bid_prices = []
                    sell_bids_with_better_price_than_top_10_buy_bids = []
    
    
                no_killed_buy_bids                    = len(killed_buy_bids)
                no_killed_sell_bids                    = len(killed_sell_bids)
                
                if(no_killed_buy_bids > 0):
                    killed_buy_volume                 = sum(b.volume                                     for b in killed_buy_bids)
                
                else:
                    killed_buy_volume                = 0
    
                if(no_killed_sell_bids > 0):
                    killed_sell_volume                = sum(b.volume                                     for b in killed_sell_bids)
                
                else:
                    killed_sell_volume                 = 0
                
                unique_buy_orders_in_transactions    = "???"
                unique_sell_orders_in_transactions    = "???"
                
                self.stats.append([timeslot, avg_transaction_price, max_transaction_price, min_transaction_price, max_open_bid_order_price, min_open_ask_order_price, transaction_volume, open_buy_bid_volumes, open_sell_bid_volumes, killed_buy_volume, killed_sell_volume, len(self.transactions[t]), unique_buy_orders_in_transactions, unique_sell_orders_in_transactions, avg_buy_order_maturity, avg_sell_order_maturity] + open_buy_bids_volumes + open_buy_bids_prices + open_sell_bids_volumes + open_sell_bids_prices)





            
            if(self.visualization_mode == True):
                time.sleep(self.time_lag)
            
        #print(sum([len(self.transactions[i]) for i in range(len(self.transactions))]))


    def get_customer_transactions(self):
        customer_transactions = []
        for t in self.transactions:
            if(t.sell_bid.isCustomer == True):
                customer_transactions.append(t)

        return customer_transactions

        
    def main(self):
        self.initandrun()
        
        print("Done running the market!")
        if(self.printing_mode):
            print("Number of transactions: " + str(len(self.transactions)))
            self.print_transactions(mode="price")
        if(self.write_transactions_to_file):
            self.write_transactions()
        print("\nDone\n\n")
    if __name__ == "__main__":
        main()
