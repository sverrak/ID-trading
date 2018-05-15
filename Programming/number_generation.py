# -*- coding: utf-8 -*-
"""
Created on Sun May 13 20:15:53 2018

@author: sverrak
"""
import numpy as np


class NumberGenerator:
    
    def __init__(self, number_of_dps, number_of_scenarios, number_of_stages, correlation_matrix=None, transition_matrix=None):
        self.number_of_dps = number_of_dps
        self.number_of_scenarios = number_of_scenarios
        self.number_of_stages = number_of_stages
        
        if(correlation_matrix == None):
            self.correlation_matrix = self.default_correlation_matrix()    
        else:
            self.correlation_matrix = correlation_matrix
        
        self.cholesky(self.correlation_matrix)
        
        self.uncorr_vars = self.get_uncorrelated_variables() 
        self.corr_vars = self.correlate_variables(self.uncorr_vars)
        self.scale_variables()
        
    def get_uncorr_vars(self):
        return self.uncorr_vars
    
    def get_corr_vars(self):
        return self.corr_vars
    
    def default_correlation_matrix(self):
        return np.array([[1,0.7,0.63,0.58,0.61],[0.7,1.0,0.72,0.64,0.65],[0.63,0.72,1.0,0.74,0.59],[0.58,0.64,0.74,1.0,0.72],[0.61,0.65,0.59,0.72,1.0]])
    
    def cholesky(self, matrix,default=False):
        
        if(default):
            
            self.cholesky_matrix = np.array([	[1,	0.698973820597299,	0.625139489532387,	0.575083429190292,	0.611398027516624]	,
                                                	[0.698973820597299,	1,	0.724076876280519,	0.6414406907441,	0.653803129408514]	,
                                                	[0.625139489532387,	0.724076876280519,	1,	0.740289571310393,	0.591902382497435]	,
                                                	[0.575083429190292,	0.6414406907441,	0.740289571310393,	1,	0.719889620181269]	,
                                                	[0.611398027516624,	0.653803129408514,	0.591902382497435,	0.719889620181269,	1]	]
)
    
        else:
            
            self.cholesky_matrix = np.transpose(np.linalg.cholesky(matrix))
            
            
    def get_uncorrelated_variables(self):
        #return [[[0.4 + 0.2*np.random.uniform(0, 1) for y in range(self.number_of_dps)] for x in range(self.number_of_stages)] for z in range(self.number_of_scenarios)]
        return [[[np.random.uniform(0, 1) for y in range(self.number_of_dps)] for x in range(self.number_of_stages)] for z in range(self.number_of_scenarios)]
    
    def correlate_variables(self, variables):
        return [np.dot(variables[x], self.cholesky_matrix) for x in range(self.number_of_scenarios)]
    
    def scale_variables(self):
        factors = [1.0,1.41,1.69,1.92,2.02]

        for z in range(self.number_of_scenarios):
            for x in range(self.number_of_stages):
                for y in range(self.number_of_dps):
                    self.corr_vars[z][x,y] = self.corr_vars[z][x,y] / factors[y]

        max_value = max([np.amax(self.corr_vars[z]) for z in range(self.number_of_scenarios)])
        #print("m,ax",max_value)
        for z in range(self.number_of_scenarios):
            for x in range(self.number_of_stages):
                for y in range(self.number_of_dps):
                    #print(type(self.corr_vars), type(self.corr_vars[z]), type(self.corr_vars[z][y,x]))
                    
                    temp_value = self.corr_vars[z][x, y] / max_value
                    self.corr_vars[z][x, y] = temp_value
        
    
    
