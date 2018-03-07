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
    def __init__(self, dp, bid_file_tag, customer_bid_file_tag, write_transactions_to_file=False, printing_mode=False, stages=136):
        super(Market, self).__init__()
        self.delivery_product             			= dp
        self.bid_file_tag                 			= bid_file_tag
        self.customer_bid_file_tag         			= customer_bid_file_tag
        self.write_transactions_to_file 			= write_transactions_to_file
        self.printing_mode               			= printing_mode
        self.visualization_mode       			  	= False
        self.customer_mode                 			= customer_bid_file_tag != "N/A"

        ### ----------- System Parameters -----------
        self.print_output                   		= True
        self.default_parameters          			= False
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
            self.time_from_gc_to_d        			= 30
            self.stages                     		= stages
            
            # Indices
            self.index_of_timestamp         		= 8
            self.index_of_isBuy            			= 9
            self.index_of_price             		= 11
            self.index_of_volume             		= 12
            self.index_of_order_id            		= 13
            self.index_of_delivery_product   		= 18
            
            
            # Trading window parameters
            self.trading_start_time         		= self.delivery_product - datetime.timedelta(days=1) + datetime.timedelta(hours=13) - datetime.timedelta(hours=self.delivery_product.hour, minutes=self.delivery_product.minute)
            self.trading_end_time             		= self.delivery_product - datetime.timedelta(minutes=self.time_from_gc_to_d)

            if(self.stages == 136):
                self.length_of_timestep     		= 10                         # Number of minutes per timestep
            else:
                self.length_of_timestep        		= datetime.timedelta(self.trading_end_time, self.trading_start_time) / self.stages


        # Data structure initialization
        self.data 									= []                                        # All bids in /filename
        self.bidsB 									= []                                        # List of buy bid objects of the delivery product described above
        self.bidsS 									= []                                        # List of sell bid objects of the delivery product described above
        self.trading_timeslots 						= []                                     	# List of timeslots
        self.open_buy_bids 							= []                                        # Iteratively updated
        self.open_sell_bids 						= []                                        # Iteratively updated
        self.transactions 							= [[] for i in range(self.stages+100)]      # Iteratively updated
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

        for bid_array in data:
        	# If the timestamps were splitted, indices must be changed
        	# To do: Is this done correctly?
            if(len(bid_array) == 30):
                # Indices
                self.index_of_timestamp  						= 10
                self.index_of_isBuy             				= 12
                self.index_of_price            					= 15
                self.index_of_volume             				= 16
                self.index_of_order_id             				= 17
                self.index_of_delivery_product     				= 23 
            elif(len(bid_array) == 31):
                # Indices
                self.index_of_timestamp         				= 8
                self.index_of_isBuy             				= 10
                self.index_of_price             				= 16
                self.index_of_volume             				= 17
                self.index_of_order_id             				= 18
                self.index_of_delivery_product     				= 24


            bid_array_timestamp             					= bid_array[self.index_of_timestamp] + " " + bid_array[self.index_of_timestamp + 1] 
            bid_array_is_buy                 					= bid_array[self.index_of_isBuy]
            bid_array_price                 					= bid_array[self.index_of_price]
            bid_array_volume                 					= bid_array[self.index_of_volume]
            bid_array_order_id                 					= bid_array[self.index_of_order_id]
            bid_array_dp                     					= bid_array[self.index_of_delivery_product] + " " + bid_array[self.index_of_delivery_product + 1] 
            
            bid_array_dp                     					= dt.strptime(bid_array_dp.split(".")[0]        , '%Y-%m-%d %H:%M:%S')
            bid_array_timestamp             					= dt.strptime(bid_array_timestamp.split(".")[0]    , '%Y-%m-%d %H:%M:%S')
            
            if(bid_array_dp == self.delivery_product):
                if(bid_array_is_buy == "1"):
                    #                         Price                  Volume                Timestamp            isBuy            Order ID     Placed by Customer
                    self.bidsB.append(Bid(bid_array_price, float(bid_array_volume), bid_array_timestamp, bid_array_is_buy, bid_array_order_id, customer_bids))
                else:
                    self.bidsS.append(Bid(bid_array_price, float(bid_array_volume), bid_array_timestamp, bid_array_is_buy, bid_array_order_id, customer_bids))

        if(False and self.printing_mode):
            print("Number of buy bids: " + str(len(self.bidsB)))
            print("Number of sell bids: " + str(len(self.bidsS)))
            print("Total number of bids: " + str(len(self.bidsS)+(len(self.bidsB))))

        if(self.printing_mode == True):
            self.print_elapsed_time(self.start_time)
        
        return self.bidsB, self.bidsS

    # Setup timeslots
    def setup_timeslots(self, start, end, length):
        timeslots = []
        time_iterator = start
        step_length = datetime.timedelta(minutes=length)

        while True:

            if(time_iterator + step_length > end + datetime.timedelta(minutes=self.length_of_timestep+60)):
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

    # This method can probably be improved by taking advantage of the fact that
    # the bid arrays are sorted by price. Not necessary to loop through the whole sets
    def create_transactions(self, open_sell_bids, open_buy_bids, time):
        # Sort by price
        open_sell_bids.sort(key=lambda x: x.price, reverse=True)
        open_buy_bids.sort(key=lambda x: x.price, reverse=True)

        # Assumptions/features: 
            # Assume bids only arrive in x min blocks
            # Do reduce volume of the same bid in other zones
            # Spread goes to the most recent bid
            # Only partial clearing
            # Killing within the timeslot does not affect bid clearing. Not really a problem when timeslot size -> 0
        for s in open_sell_bids:
            buy_bid_iterator = 0
            while s.volume > 0 and buy_bid_iterator < len(open_buy_bids):
                if(open_buy_bids[buy_bid_iterator].price >= s.price):
                    if(False and self.printing_mode):
                        print("Transaction created!")
                    
                    ### Create transaction
                    # Transaction attributes are given as stated below:
                    timestamp               = s.timestamp if s.timestamp > open_buy_bids[buy_bid_iterator].timestamp else open_buy_bids[buy_bid_iterator].timestamp # The transaction timestamp is equal to the timestamp of the most recent bid involved in the transaction
                    transaction_volume      = min(s.volume, open_buy_bids[buy_bid_iterator].volume) # The transaction volume is equal to the smallest residual volume of the involved bids
                    transaction_price       = float(s.price) if s.timestamp == timestamp else float(open_buy_bids[buy_bid_iterator].price) # Transaction price is equal to the price of the earliest placed bid
                    
                    # Add the new transaction to the list of transactions
                    self.transactions[time].append(Transaction(s, open_buy_bids[buy_bid_iterator], transaction_price, transaction_volume, timestamp)) 
                    
                    # Identify all bids with same order id
                    # Reduce their volumes and remove them if no residual volume
                    # Hypothesis: Try except is needed as we might remove a bid before trying to access it.
                    for t in open_sell_bids:
                        try:
                            if(t.order_id == s.order_id):
                                t.reduce_volume(transaction_volume)
                                if(t.volume == 0):
                                    open_sell_bids.remove(t)
                                    #closed_sell_bids.append(t)
                        except: 
                            continue

                    for t in open_buy_bids:
                        #print(buy_bid_iterator, len(open_buy_bids))
                        try:
                            if(open_buy_bids[buy_bid_iterator].order_id == t.order_id):
                                t.reduce_volume(transaction_volume)
                                
                                if(t.volume == 0):
                                    open_buy_bids.remove(t)
                                    #closed_buy_bids.append(t)
                                    # Necessary to decrease iterator?
                                    #buy_bid_iterator -= 1
                        except:
                            continue

                    # Increase iterator
                    buy_bid_iterator = buy_bid_iterator + 1

                else:
                    break

        
        return open_sell_bids, open_buy_bids

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
    def remove_killed_bids(self, bids):
        # For each bid with zero volume, remove all other bids with identical order id
        zero_volume_bids     = []
        killed_bids         = []
        for b in bids:
            if(b.volume == 0):
                zero_volume_bids.append(b)

        for k in zero_volume_bids:
            for b in bids:
                if(k.order_id == b.order_id):
                    killed_bids.append(k)
                    bids.remove(b)

        return bids, killed_bids


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


    def initandrun(self):
        # Data structure instantiation
        self.trading_timeslots     = self.setup_timeslots(self.trading_start_time, self.trading_end_time, self.length_of_timestep)  # List of timeslots
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
        sell_bid_dict             = self.create_bid_dictionary(sell_bids, self.trading_timeslots)
        buy_bid_dict             = self.create_bid_dictionary(buy_bids, self.trading_timeslots)

        # Stats headers
        self.stats                 = [["Trading timeslot", "Avg transaction price", "Max transaction price", "Min transaction price", "Max open bid order price", "Min open ask order price", "Transaction volume", "Open bid volumes", "Open ask volumes", "Killed buy order volumes", "Killed sell order volumes", "Number of transactions", "Number of unique bid orders in transactions", "Number of unique ask orders in transactions", "Average buy order maturity", "Average sell order maturity"] + ["B.V"+str(i) for i in range(1,6)] + ["B.P"+str(i) for i in range(1,6)]+ ["S.V"+str(i) for i in range(1,6)] + ["S.P"+str(i) for i in range(1,6)]]

        for t, timeslot in enumerate(self.trading_timeslots):
            if(False and self.printing_mode):
                print("\n\n\nTimeslot: " + str(timeslot) + " (Iteration " + str(t) + ")")

            ### Retrieve all incoming bids
            self.open_sell_bids += sell_bid_dict[timeslot]
            self.open_buy_bids += buy_bid_dict[timeslot]

            ### Remove killed bids
            self.open_sell_bids, killed_sell_bids     = self.remove_killed_bids(self.open_sell_bids)
            self.open_buy_bids, killed_buy_bids        = self.remove_killed_bids(self.open_buy_bids)
            
            ### Print bid curves
            self.print_bid_curves(self.open_buy_bids, self.open_sell_bids)
            
            ### Create transactions and update bid portfolio
            self.open_sell_bids, self.open_buy_bids = self.create_transactions(self.open_sell_bids, self.open_buy_bids, t)

            ### Sort bid arrays
            
            self.open_buy_bids.sort(key=lambda x: x.price, reverse=True)
            
            
            
            ### Print bid curves
            self.print_bid_curves(self.open_buy_bids, self.open_sell_bids)
            if(False and self.printing_mode):
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
                open_buy_bids_prices = [b.price for b in self.open_buy_bids[1:6]]
                open_buy_bids_volumes = [b.volume for b in self.open_buy_bids[1:6]]
                max_open_bid_order_price         = self.open_buy_bids[0]
                open_buy_bid_volumes            = sum(b.volume                                     for b in self.open_buy_bids)
                avg_buy_order_maturity            = sum(b.compute_maturity(timeslot).total_seconds()                 for b in self.open_buy_bids)    / len(self.open_buy_bids)
            
            else:
                max_open_bid_order_price        = "N/A"
                open_buy_bid_volumes            = "N/A"
                avg_buy_order_maturity            = "N/A"
                open_buy_bids_prices = [0 for b in range(5)]
                open_buy_bids_volumes = [0 for b in range(5)]
                
            if(len(self.open_sell_bids) > 0):
                self.open_sell_bids.sort(key=lambda x: x.price, reverse=False)
                
                min_open_ask_order_price         = self.open_sell_bids[0]
                open_sell_bids_prices = [b.price for b in self.open_sell_bids[1:6]]
                open_sell_bids_volumes = [b.volume for b in self.open_sell_bids[1:6]]
                open_sell_bid_volumes            = sum(b.volume                                     for b in self.open_sell_bids)
                avg_sell_order_maturity            = sum(b.compute_maturity(timeslot).total_seconds()                for b in self.open_sell_bids)     / len(self.open_sell_bids)
            
            else:
                min_open_ask_order_price        = "N/A"
                open_sell_bid_volumes            = "N/A"
                avg_sell_order_maturity            = "N/A"
                open_sell_bids_prices = [0 for b in range(5)]
                open_sell_bids_volumes = [0 for b in range(5)]

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

            # ****** YOUR CODE HERE ******
            
            # ****** YOUR CODE HERE ******

    def get_customer_transactions(self):
        customer_transactions = []
        for t in self.transactions:
            if(t.sell_bid.isCustomer == True):
                customer_transactions.append(t)

        return customer_transactions

        
    def main(self):
        self.initandrun()
        
        print("Gothere")
        if(self.printing_mode):
            print("Number of transactions: " + str(len(self.transactions)))
            self.print_transactions(mode="price")
        if(self.write_transactions_to_file):
            self.write_transactions()
        print("\nDone\n\n")
    if __name__ == "__main__":
        main()
