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
    def __init__(self, date_range, price_range, price_increment, include_slots_without_transactions=False, directory=""):
        super(TransitionMatrixLearner, self).__init__()
        
        self.printing_mode = True
        
        self.date_range                             = date_range
        self.dates_str, self.dates                     = self.date_strings_between(self.date_range[0], self.date_range[1])
        self.timeslots                                 = [str(i) if i>9 else "0"+str(i) for i in range(24)]
        self.directories = [directory + "DynamicStep/Orderbook_stats_time_range_" + date_str + ".xlsx" for date_str in self.dates_str]
        
        self.ub_price = price_range[1]
        self.lb_price = price_range[0]
        self.input_price = "Base price"
        self.output_price = "Best buy price"
        self.price_increment = price_increment                                                                  # To do: This is not generalizing well at the moment. See rounding function 
        #self.transition_matrix_size = (self.ub_price - self.lb_price) * 10 + 2                                 # To do when generalizing: generalize here
        self.include_slots_without_transactions = include_slots_without_transactions
        self.index_of_price = self.setup_index_of_price()
        
        if(self.include_slots_without_transactions == True):
            self.transition_matrix_size = int((self.ub_price - self.lb_price) / self.price_increment) + 1                                 # To do when generalizing: generalize here
        else:
            self.transition_matrix_size = int((self.ub_price - self.lb_price) / self.price_increment) + 2                                 # To do when generalizing: generalize here
        
        self.transition_matrix = [[0.0 for x in range(self.transition_matrix_size)] for y in range(self.transition_matrix_size)]
        
        self.io = TransitionMatrixIOHandler()
        
        
    def setup_index_of_price(self):
        dictionary = {-999990.0: 0}
        
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
        
        for dp, dp_str in enumerate(self.timeslots):
            
            delta_matrix = self.count_occurrences(current_timeseries[dp])
            #print(len(delta_matrix), len(delta_matrix[0]))
            for i in range(len(self.transition_matrix)):
                for j in range(len(self.transition_matrix[0])):
                    matrix[i][j] += delta_matrix[i][j]
        #print(len(matrix), len(matrix[0]))
        return matrix
    
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
        
        for d, date_str in enumerate(self.dates_str):
            if(self.printing_mode == True):
                print("Currently examining",date_str)
            
            current_timeseries = self.io.read_file(input_file=self.directories[d], error_value=-999990.0,analysis_mode=True)
            
            for dp, dp_str in enumerate(self.timeslots):
                
                delta_matrix = self.count_occurrences(current_timeseries[dp])
                for i in range(len(self.transition_matrix)):
                    for j in range(len(self.transition_matrix[0])):
                        self.transition_matrix[i][j] += delta_matrix[i][j]
            
            
        
    # The logic here is to create a zero matrix where we store the event data (what happened to the price etc).
    # The first row and column corresponds to those timeslots where no transaction occured and those where no transaction occured in the upcoming timeslot
    # Thus, we must add 1 to the timeslot indices to find out where to store the event
    def count_occurrences(self, timeseries, include_slots_without_transactions=False):
        
        delta_matrix = [[0.0 for x in range(self.transition_matrix_size)] for y in range(self.transition_matrix_size)]
        
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
                    try: # Try/except should not be necessary any more, but does not do any harm here
                        delta_matrix[self.index_of_price[current_price]][self.index_of_price[next_price]] += 1        # To do when generalizing: generalize here
                    except:
                        raise ValueError("Timeslot",i,":",self.index_of_price[current_price],"or",self.index_of_price[next_price],"not in delta_matrix", len(delta_matrix), len(delta_matrix[0]))
        else:
            for i, slot in enumerate(timeseries[:-1]):
                if(self.input_price == "Base price"):
                    current_price = max(self.lb_price, min(self.ub_price, self.round_to_closest(slot.base_price)))          # Round and bound the current price by the matrix UB and LB
                
                if(self.output_price == "Base price"):
                    next_price = max(self.lb_price, min(self.ub_price, self.round_to_closest(timeseries[i+1].base_price)))  # Round and bound the next price by the matrix UB and LB
                elif(self.output_price == "Best buy price"):
                    #print(timeseries[i+1].best_buy_price)
                    next_price = max(self.lb_price, min(self.ub_price, self.round_to_closest(timeseries[i+1].best_buy_price)))  # Round and bound the next price by the matrix UB and LB
                
                #print(current_price, next_price)
                delta_matrix[self.index_of_price[current_price]][self.index_of_price[next_price]] += 1
        
        return delta_matrix

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
    
if __name__ == '__main__':
    multiprocessing.freeze_support()
    #date_range = ["2016-09-01", "2017-02-28"]
    date_range = ["2016-09-01", "2017-02-28"]
    price_range = [30,100]
    matrix_learner = TransitionMatrixLearner(date_range, price_range, 1)
    matrix_learner.learn_matrix()
    
    print("\n\n\n Normalizing matrix")
    matrix_learner.normalize_matrix()
    
    for row in matrix_learner.get_matrix():
        print(row)
     
    