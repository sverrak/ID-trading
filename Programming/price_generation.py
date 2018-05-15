# -*- coding: utf-8 -*-
"""
Created on Sun May 13 19:50:06 2018

@author: sverrak
"""

import numpy as np
import time
import openpyxl as xlsxreader
from number_generation import NumberGenerator
class PriceGenerator:

    def __init__(self, number_of_dps, number_of_scenarios, number_of_stages, gate_closures=None, correlation_matrix=None, transition_matrix=None):
        ng = NumberGenerator(number_of_dps, number_of_scenarios, number_of_stages)
        self.correlated_variables = ng.get_corr_vars()
        print(ng.cholesky_matrix)
        for x in range(100):
            print(x,[self.correlated_variables[0][x]])
        
        self.number_of_dps = number_of_dps
        self.number_of_scenarios = number_of_scenarios
        self.number_of_stages = number_of_stages
        
        if(gate_closures == None):
            self.gate_closures = [75+4*i for i in range(self.number_of_dps)]    
        else:
            self.gate_closures = gate_closures
        
        
        
        
        if(transition_matrix == None):
            self.transitions_pdf = self.default_transition_matrix()    
        else:
            self.transitions_pdf = transition_matrix
        
        self.price_level_bounds = [-10,120]
        self.size_of_price_range = len(self.transitions_pdf)
        
        self.create_cdf()
        
        
    def create_cdf(self):
        self.transitions_cdf = [[0.0 for x in range(self.size_of_price_range)] for y in range(self.size_of_price_range)]        # Transition CDFs. 
        #print(len(self.transitions_pdf), len(self.transitions_pdf[0]))
        for i in range(0,self.size_of_price_range):
            cumulative_sum = 0.0
        
            for j in range(0,self.size_of_price_range):
                
                cumulative_sum += self.transitions_pdf[i/2 + self.price_level_bounds[0]][j]
                self.transitions_cdf[i][j] = cumulative_sum
            
            
    # Samples the upcoming price based on current price, a random variable and the transition CDFs of the current price
    def calculate_next_price(self, current_price, random_variable, s,dp,t):
        index = min((int((2*current_price - 2*self.price_level_bounds[0] - 1))+1),len(self.transitions_cdf)-1)
        #print(current_price, index, len(self.transitions_cdf), len(self.transitions_cdf[0]))
        for j in range(len(self.transitions_cdf[index])):
            if(self.transitions_cdf[index][j] >= random_variable):
                #print(current_price, random_variable,index, j,float(float(j)/2.0 + self.price_level_bounds[0]) )
                return float(float(j)/2.0 + self.price_level_bounds[0])
        #print(self.transitions_cdf[index])
        return float(self.base_prices[s][t-2][dp])
        raise ValueError(random_variable, current_price)
        
        
    # Generates base prices using the calculate_next_price function
    def generate_price_processes(self):
        
        self.base_prices = [[[0 for x in range(self.number_of_dps)] for y in range(self.number_of_stages)] for z in range(self.number_of_scenarios)]
        
        for s in range(self.number_of_scenarios):
            for dp in range(self.number_of_dps):
                # Setup market open prices
                self.base_prices[s][0][dp] = 45
                
                for t in range(1, self.gate_closures[dp]):
                    #print(dp,t, base_prices[s][t-1][dp])
                    self.base_prices[s][t][dp] = self.calculate_next_price(self.base_prices[s][t-1][dp], self.correlated_variables[s][t, dp], s,dp,t)
                
                for t in range(self.gate_closures[dp],self.number_of_stages):
                    self.base_prices[s][t][dp] = "#N/A"
        return self.base_prices
    
    def default_transition_matrix(self):
        return self.read_file()
    

    
    # Convert the input file into a list of lists [dp][timeslot]
    def read_file(self, input_file="transition_matrix.xlsx"):  
        for r in range(3):
            try:
                workbook = xlsxreader.load_workbook(input_file)
                break
            except:
                print("Could not access workbook. Trying again in 30 s")
                time.sleep(30)
        
        sheet_names = workbook.sheetnames
        content = {}
        
        for x, sheet_name in enumerate(sheet_names):
            if(sheet_name == "Sheet1"):
                sheet = workbook[sheet_name]
                
                
                for i,row in enumerate(list(sheet.rows)):
                    content[row[0].value] = [cell.value for cell in row[1:]]
            else:
                break
                
                
                
        
        workbook.close()
        
        return content
    
        