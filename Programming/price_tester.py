# -*- coding: utf-8 -*-
"""
Created on Sun May 13 20:35:04 2018

@author: sverrak
"""

from price_generation import PriceGenerator
from limit_order_generation import CPRIndicatorGenerator

#pg = PriceGenerator(5,1,100)

print("A" if False else "B" if False else "C")

#base_prices = pg.generate_price_processes()

for t in range(100):    
    a =0 
#    print(base_prices[0][t])

log = CPRIndicatorGenerator(5,1,100,True)

#log.printer()
