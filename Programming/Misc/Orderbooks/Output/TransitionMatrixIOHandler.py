# -*- coding: utf-8 -*-
"""
Created on Wed Apr  4 09:22:19 2018

@author: sverrak
"""

import openpyxl as xlsxreader
from TimeslotState import TimeslotState

class TransitionMatrixIOHandler(object):
    """docstring for MarketRunner"""
    def __init__(self):
        super(TransitionMatrixIOHandler, self).__init__()
        self.index_of_timeslot = 0
        self.index_of_base_price = 1
        self.index_of_high_transaction_price = 2
        self.index_of_low_transaction_price = 3
        self.index_of_buy_price = 29
        self.index_of_sell_price = 39
        self.index_of_buy_order_depth = 9
        self.index_of_sell_order_depth = 10
        
        
        
        
    # Convert the input file into a list of lists [dp][timeslot]
    def read_file(self, input_file, analysis_mode=False, return_headers=False, error_value=-999990.0):  
        workbook = xlsxreader.load_workbook(input_file)
        sheet_names = workbook.sheetnames
        content = [[] for sheet_name in sheet_names]
        
        if(return_headers == True):
            headers = []
            
        for x, sheet_name in enumerate(sheet_names):
            
            
            sheet = workbook[sheet_name]
            if(return_headers == True):
                headers.append([""])
            
            
            for i,row in enumerate(list(sheet.rows)[1:]):
                
                
                timeslot = row[self.index_of_timeslot].value
                base_price = row[self.index_of_base_price].value
                high_transaction_price = row[self.index_of_high_transaction_price].value
                low_transaction_price = row[self.index_of_low_transaction_price].value
                
                if(return_headers == True):
                    headers[-1].append(timeslot)
                
                # If no transaction has occured, we raise a flag (base_price = -999990.0)
                try:
                    base_price = float(base_price)
                    high_transaction_price = float(high_transaction_price)
                    low_transaction_price = float(low_transaction_price)
                except:
                    #print("Value print", input_file, [c.value for c in row], base_price, high_transaction_price, low_transaction_price)
                    base_price = error_value
                    high_transaction_price = error_value
                    low_transaction_price = error_value
                
                if(analysis_mode == True):
                    buy_order_volume = float(row[self.index_of_buy_order_depth].value)
                    sell_order_volume = float(row[self.index_of_sell_order_depth].value)
                    
                    try:

                        best_buy_price = float(row[self.index_of_buy_price].value)
                        best_sell_price = float(row[self.index_of_sell_price].value)
                        spread = best_sell_price - best_buy_price

                    except:

                        best_buy_price = error_value
                        best_sell_price = error_value
                        spread = error_value
                    
                    # Add a new TimeslotState object to the content list
                    
                    content[x].append(TimeslotState(timeslot, base_price, spread=spread, buy_order_depth=buy_order_volume, sell_order_depth=sell_order_volume, best_buy_price=best_buy_price, best_sell_price=best_sell_price, high_transaction_price=high_transaction_price, low_transaction_price=low_transaction_price))
                else:
                    # Add a new TimeslotState object to the content list
                    content[x].append(TimeslotState(timeslot, base_price))
        if(return_headers == True):
            return content, headers
        else:
            return content
    
    def write_matrix(self, output_file):
        return
