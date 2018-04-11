# -*- coding: utf-8 -*-
"""
Created on Wed Apr  4 09:15:18 2018

@author: sverrak
"""

from datetime import datetime as dt
import datetime
import numpy

from TimeslotState import TimeslotState
from TransitionMatrixIOHandler import TransitionMatrixIOHandler
from multiprocessing import Pool as ThreadPool
import multiprocessing

class TransitionMatrixLearner(object):
    """docstring for MarketRunner"""
    def __init__(self, date_range, price_range, price_increment, include_slots_without_transactions=False, directory="",category_matrix=False, input_price="", output_price=""):
        super(TransitionMatrixLearner, self).__init__()
        
        self.printing_mode = True
        
        self.date_range                             = date_range
        self.dates_str, self.dates                     = self.date_strings_between(self.date_range[0], self.date_range[1])
        self.timeslots                                 = [str(i) if i>9 else "0"+str(i) for i in range(24)]
        self.directories = [directory + "ConstantStep5/Orderbook_stats_time_range_" + date_str + ".xlsx" for date_str in self.dates_str]
        
        self.ub_price = price_range[1]
        self.lb_price = price_range[0]
        self.input_price = input_price
        self.output_price = output_price
        self.price_increment = price_increment                                                                  # To do: This is not generalizing well at the moment. See rounding function 
        self.error_value = -999990.0
        #self.transition_matrix_size = (self.ub_price - self.lb_price) * 10 + 2                                 # To do when generalizing: generalize here
        self.include_slots_without_transactions = include_slots_without_transactions
        self.index_of_price = self.setup_index_of_price()
        self.category_matrix_mode = category_matrix
        if(self.category_matrix_mode == True):
            self.dp_categories = [7,11,16,24]
            self.dp_category_labels = ["Night", "Morning", "Day", "Evening"]
            self.trading_categories = [7, 17, 24]
            self.trading_category_labels = ["Night", "Day", "Evening"]
            
            if(self.include_slots_without_transactions == True):
                self.transition_matrix_size = int((self.ub_price - self.lb_price) / self.price_increment) + 1                                 # To do when generalizing: generalize here
            else:
                self.transition_matrix_size = int((self.ub_price - self.lb_price) / self.price_increment) + 2                                 # To do when generalizing: generalize here
            
            self.transition_matrix = [[0.0 for x in range(self.transition_matrix_size)] for y in range(len(self.trading_categories) * len(self.dp_categories) * self.transition_matrix_size)]
    
        else:
        
            if(self.include_slots_without_transactions == True):
                self.transition_matrix_size = int((self.ub_price - self.lb_price) / self.price_increment) + 1                                 # To do when generalizing: generalize here
            else:
                self.transition_matrix_size = int((self.ub_price - self.lb_price) / self.price_increment) + 2                                 # To do when generalizing: generalize here
            
            self.transition_matrix = [[0.0 for x in range(self.transition_matrix_size)] for y in range(self.transition_matrix_size)]
        
        self.io = TransitionMatrixIOHandler()
        
        
    def setup_index_of_price(self):
        if(self.include_slots_without_transactions):
            dictionary = {-999990.0: 0}
        else:
            dictionary = {}
            
        for i,p in enumerate(range(self.lb_price, self.ub_price)):
            if(self.price_increment < 1.0):
                number_of_increments_per_integer = int(1.0/float(self.price_increment))
                dictionary[float(p)] = number_of_increments_per_integer * i + 1
                for x in range(1,number_of_increments_per_integer):
                    dictionary[float(p)+float(x)/float(number_of_increments_per_integer)] = number_of_increments_per_integer*i+x+1
            else:
                dictionary[float(p)] = i + 1
        
        dictionary[self.ub_price] = len(list(dictionary.keys()))
        
        return dictionary
        
    def setup_matrix(self):
        return 
    
    
    def fetch_matrix_for_date(self, directory):
        
        if(self.printing_mode == True):
            print("Currently examining",directory)
            
        current_timeseries = self.io.read_file(input_file=directory,analysis_mode=True)
        matrix = [[0.0 for x in range(self.transition_matrix_size)] for y in range(self.transition_matrix_size)]
        priors = [0.0 for x in range(self.transition_matrix_size)]
        for dp, dp_str in enumerate(self.timeslots):
            
            delta_matrix, delta_priors = self.count_occurrences(dp, current_timeseries[dp])
            #print(len(delta_matrix), len(delta_matrix[0]))
            for i in range(len(self.transition_matrix)):
                priors[i] += delta_priors[i]
                for j in range(len(self.transition_matrix[0])):
                    matrix[i][j] += delta_matrix[i][j]
        #print(len(matrix), len(matrix[0]))
        return matrix, priors
    
    def learn_matrix_parallel(self):
        #print(len(self.directories))
        pool = ThreadPool(len(self.directories))
        results = []
        results.append(pool.map(self.fetch_matrix_for_date, self.directories))
        
        print("Aggregating results...")
        matrix = [[0.0 for x in range(self.transition_matrix_size)] for y in range(self.transition_matrix_size)]
        
        for daily_matrix in results:
            #print(len(daily_matrix), len(daily_matrix[0]))
            for i in range(len(self.transition_matrix)):
                for j in range(len(self.transition_matrix[0])):
                    matrix[i][j] += daily_matrix[i][j]
                    
        self.transition_matrix = matrix
            
            
        
    
    def learn_matrix(self):
        self.priors = [0.0 for x in range(len(self.trading_categories) * len(self.dp_categories) * self.transition_matrix_size)]
        for d, date_str in enumerate(self.dates_str):
            if(self.printing_mode == True):
                print("Currently examining",date_str)
            
            current_timeseries = self.io.read_file(input_file=self.directories[d], error_value=self.error_value,analysis_mode=True)
            
            
            if(self.category_matrix_mode == True):
                for dp, dp_str in enumerate(self.timeslots):
                    
                    delta_matrix, delta_priors = self.count_occurrences_with_categories(dp, current_timeseries[dp])
                    for i in range(len(self.transition_matrix)):
                        self.priors[i] += delta_priors[i]
                        for j in range(len(self.transition_matrix[0])):
                            self.transition_matrix[i][j] += delta_matrix[i][j]
            else:
                for dp, dp_str in enumerate(self.timeslots):
                    
                    delta_matrix, delta_priors = self.count_occurrences(dp, current_timeseries[dp])
                    for i in range(len(self.transition_matrix)):
                        self.priors[i] += delta_priors[i]
                        for j in range(len(self.transition_matrix[0])):
                            self.transition_matrix[i][j] += delta_matrix[i][j]
            
            
        
    # The logic here is to create a zero matrix where we store the event data (what happened to the price etc).
    # The first row and column corresponds to those timeslots where no transaction occured and those where no transaction occured in the upcoming timeslot
    # Thus, we must add 1 to the timeslot indices to find out where to store the event
    def count_occurrences(self, dp, timeseries, include_slots_without_transactions=False):
        
        delta_matrix = [[0.0 for x in range(self.transition_matrix_size)] for y in range(self.transition_matrix_size)]
        priors = [0.0 for x in range(self.transition_matrix_size)]
        
        if(include_slots_without_transactions == True):
            for i, slot in enumerate(timeseries[:-1]):
                current_price = max(self.lb_price, min(self.ub_price, self.round_to_closest(slot.base_price)))          # Round and bound the current price by the matrix UB and LB
                next_price = max(self.lb_price, min(self.ub_price, self.round_to_closest(timeseries[i+1].base_price)))  # Round and bound the next price by the matrix UB and LB
            
                # If no transaction occurred in the current or next timeslot, the event is stored in the first row or column.
                if(slot.base_price == -999990.0 and next_price == -999990.0):    
                    delta_matrix[0][0] += 1        # To do when generalizing: generalize here
                elif(slot.base_price == -999990.0 and next_price != -999990.0):
                    delta_matrix[0][self.index_of_price[next_price]] += 1        # To do when generalizing: generalize here
                elif(slot.base_price != -999990.0 and next_price == -999990.0):
                    delta_matrix[self.index_of_price[current_price]][0] += 1        # To do when generalizing: generalize here
                else:
                    try: # Try/except should not be necessary anymore, but does not do any harm here
                        delta_matrix[self.index_of_price[current_price]][self.index_of_price[next_price]] += 1        # To do when generalizing: generalize here
                    except:
                        raise ValueError("Timeslot",i,":",self.index_of_price[current_price],"or",self.index_of_price[next_price],"not in delta_matrix", len(delta_matrix), len(delta_matrix[0]))
        else:
            for i, slot in enumerate(timeseries[:-1]):
                if(self.input_price == "Base price"):
                    current_price = max(self.lb_price, min(self.ub_price, self.round_to_closest(slot.base_price)))          # Round and bound the current price by the matrix UB and LB
                elif(self.input_price == "Best buy price"):
                    current_price = max(self.lb_price, min(self.ub_price, self.round_to_closest(slot.best_buy_price)))  # Round and bound the next price by the matrix UB and LB

                if(self.output_price == "Base price"):
                    next_price = max(self.lb_price, min(self.ub_price, self.round_to_closest(timeseries[i+1].base_price)))  # Round and bound the next price by the matrix UB and LB
                
                elif(self.output_price == "Best buy price"):
                    next_price = max(self.lb_price, min(self.ub_price, self.round_to_closest(timeseries[i+1].best_buy_price)))  # Round and bound the next price by the matrix UB and LB
                elif(self.output_price == "High transacion"):
                    next_price = max(self.lb_price, min(self.ub_price, self.round_to_closest(timeseries[i+1].best_buy_price)))  # Round and bound the next price by the matrix UB and LB
                elif(self.output_price == "Low transaction"):
                    next_price = max(self.lb_price, min(self.ub_price, self.round_to_closest(timeseries[i+1].best_buy_price)))  # Round and bound the next price by the matrix UB and LB
                
                
                priors[self.index_of_price[current_price]] += 1.0
                delta_matrix[self.index_of_price[current_price]][self.index_of_price[next_price]] += 1
        
        return delta_matrix, priors
    
    # The logic here is to create a zero matrix where we store the event data (what happened to the price etc).
    # The first row and column corresponds to those timeslots where no transaction occured and those where no transaction occured in the upcoming timeslot
    # Thus, we must add 1 to the timeslot indices to find out where to store the event
    def count_occurrences_with_categories(self, dp, timeseries, include_slots_without_transactions=False): 
        no_dp_categories = len(self.dp_categories)        
        no_trading_categories = len(self.trading_categories)
        delta_matrix = [[0.0 for x in range(self.transition_matrix_size)] for y in range(no_dp_categories * no_trading_categories * self.transition_matrix_size)]
        priors = [0.0 for x in range(no_dp_categories * no_trading_categories * self.transition_matrix_size)]
        
        
        if(include_slots_without_transactions == True):
            raise ValueError("Transition matrices with categories cannot be combined with transactionless slots handling")
            for i, slot in enumerate(timeseries[:-1]):
                current_price = max(self.lb_price, min(self.ub_price, self.round_to_closest(slot.base_price)))          # Round and bound the current price by the matrix UB and LB
                next_price = max(self.lb_price, min(self.ub_price, self.round_to_closest(timeseries[i+1].base_price)))  # Round and bound the next price by the matrix UB and LB
            
                # If no transaction occurred in the current or next timeslot, the event is stored in the first row or column.
                if(slot.base_price == -999990.0 and next_price == -999990.0):    
                    delta_matrix[0][0] += 1        # To do when generalizing: generalize here
                elif(slot.base_price == -999990.0 and next_price != -999990.0):
                    delta_matrix[0][self.index_of_price[next_price]] += 1        # To do when generalizing: generalize here
                elif(slot.base_price != -999990.0 and next_price == -999990.0):
                    delta_matrix[self.index_of_price[current_price]][0] += 1        # To do when generalizing: generalize here
                else:
                    try: # Try/except should not be necessary any more, but does not do any harm here
                        delta_matrix[self.index_of_price[current_price]][self.index_of_price[next_price]] += 1        # To do when generalizing: generalize here
                    except:
                        raise ValueError("Timeslot",i,":",self.index_of_price[current_price],"or",self.index_of_price[next_price],"not in delta_matrix", len(delta_matrix), len(delta_matrix[0]))
        else:
            for i, slot in enumerate(timeseries[:-1]):
                
                dp_category = self.find_pos_in_vector(dp, mode="dp")
                timeslot_category = self.find_pos_in_vector(slot.timeslot, mode="time")
                
                if(self.input_price == "Base price"):
                    current_price = max(self.lb_price, min(self.ub_price, self.round_to_closest(slot.base_price)))          # Round and bound the current price by the matrix UB and LB
                elif(self.input_price == "Best buy price"):
                    current_price = max(self.lb_price, min(self.ub_price, self.round_to_closest(slot.best_buy_price)))  # Round and bound the next price by the matrix UB and LB
                elif(self.input_price == "Best sell price"):
                    current_price = max(self.lb_price, min(self.ub_price, self.round_to_closest(slot.best_sell_price)))  # Round and bound the next price by the matrix UB and LB

                if(self.output_price == "Base price"):
                    next_price = max(self.lb_price, min(self.ub_price, self.round_to_closest(timeseries[i+1].base_price)))  # Round and bound the next price by the matrix UB and LB
                elif(self.output_price == "High transaction price"):
                    if(timeseries[i+1].high_transaction_price == self.error_value):
                        next_price = max(self.lb_price, min(self.ub_price, self.round_to_closest(max(timeseries[i+1].best_buy_price, slot.best_buy_price))))  # Round and bound the next price by the matrix UB and LB
                    else:
                        next_price = max(self.lb_price, min(self.ub_price, self.round_to_closest(max(timeseries[i+1].high_transaction_price, timeseries[i+1].best_buy_price, slot.best_buy_price))))  # Round and bound the next price by the matrix UB and LB
                elif(self.output_price == "Low transaction price"):
                    if(timeseries[i+1].low_transaction_price == self.error_value):
                        next_price = max(self.lb_price, min(self.ub_price, self.round_to_closest(min(timeseries[i+1].best_sell_price, slot.best_sell_price))))  # Round and bound the next price by the matrix UB and LB
                    else:
                        next_price = max(self.lb_price, min(self.ub_price, self.round_to_closest(min(timeseries[i+1].low_transaction_price, timeseries[i+1].best_sell_price, slot.best_sell_price))))  # Round and bound the next price by the matrix UB and LB
                
                elif(self.output_price == "Best buy price"):
                    next_price = max(self.lb_price, min(self.ub_price, self.round_to_closest(timeseries[i+1].best_buy_price)))  # Round and bound the next price by the matrix UB and LB
                
                if(False and self.index_of_price[next_price] == 1):
                    print(dp, self.output_price,slot.timeslot, next_price,timeseries[i+1].high_transaction_price, timeseries[i+1].best_buy_price, slot.best_buy_price)
                priors[(dp_category * no_trading_categories + timeslot_category) * self.transition_matrix_size + self.index_of_price[current_price]] += 1.0
                delta_matrix[(dp_category * no_trading_categories + timeslot_category) * self.transition_matrix_size + self.index_of_price[current_price]][self.index_of_price[next_price]] += 1
        
        return delta_matrix, priors

    def find_pos_in_vector(self, value, mode=None):
        if(mode == "dp"):
            for i, d in enumerate(self.dp_categories):
                if(value < d):
                    return i
        elif(mode == "time"):
            for i, t in enumerate(self.trading_categories):
                if(value.hour < t):
                    return i
        else:
            raise ValueError(mode,"not supported")
        
    
    def round_to_closest(self, number):
        return numpy.round(number, decimals=0)
    
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
        
    def normalize_matrix(self):
        for row in self.transition_matrix:
            row_sum = sum(row)
            for j, col in enumerate(row):
                if(row_sum > 0.0):
                    row[j] = col / row_sum
    
    def get_matrix(self):
        return self.transition_matrix
    
import xlsxwriter

def write_variables_to_file(name, data):
    book = xlsxwriter.Workbook(name)
    sheet = book.add_worksheet("Results")

    # Fill spreadsheet
    for i,row in enumerate(data):
        for j,col in enumerate(row):
            sheet.write(i+5,j,col)

    book.close()

    
if __name__ == '__main__':
    multiprocessing.freeze_support()
    date_range = ["2016-03-01", "2017-02-28"]
    #date_range = ["2016-03-01", "2016-03-05"]
    price_range = [-10,120]
    size = price_range[1] - price_range[0]
    cat_mat = True
    in_prices = ["Best buy price", "Best sell price"]
    out_prices = ["High transaction price", "Low transaction price"]
    file_names = ["DataForAnalysis/CPR_ASK.xlsx", "DataForAnalysis/CPR_BUY.xlsx"]
    
    for j,p in enumerate(in_prices):
        
        matrix_learner = TransitionMatrixLearner(date_range, price_range, 1, category_matrix=cat_mat, input_price=in_prices[j], output_price=out_prices[j])
        matrix_learner.learn_matrix()
        
        print("\n\n\n Normalizing matrix")
        matrix_learner.normalize_matrix()
        
        out_matrix = []
        
        if(cat_mat):
            price = price_range[0]
            for i, row in enumerate(matrix_learner.get_matrix()):
                dp_index = int(i/(matrix_learner.transition_matrix_size*len(matrix_learner.trading_categories)))
                timeslot_index = int(i/matrix_learner.transition_matrix_size) - dp_index * len(matrix_learner.trading_categories)
                
                try:
                    out_matrix.append([matrix_learner.dp_category_labels[dp_index]] + [matrix_learner.trading_category_labels[timeslot_index]] + [price] + [matrix_learner.priors[i]] + row) 
                    print(matrix_learner.dp_category_labels[dp_index], matrix_learner.trading_category_labels[timeslot_index], price, matrix_learner.priors[i], row)
                except:
                    raise ValueError(int(i/(matrix_learner.transition_matrix_size*len(matrix_learner.trading_categories))),len(matrix_learner.dp_categories), int(i/matrix_learner.transition_matrix_size), len(matrix_learner.trading_categories))
                    
                if(price > price_range[1]):
                    price = price_range[0]
                else:
                    price += 1
        else:
            for k, row in enumerate(matrix_learner.get_matrix()):
                print(row)
        
        print(file_names, i)
        write_variables_to_file(file_names[j], out_matrix)
    print("")
    print(matrix_learner.priors)
     
    