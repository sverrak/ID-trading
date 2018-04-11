# -*- coding: utf-8 -*-
"""
Created on Wed Apr  4 09:17:03 2018

@author: sverrak
"""

class TimeslotState(object):
    """docstring for MarketRunner"""
    def __init__(self, timeslot, base_price, spread=0, buy_order_depth=0, sell_order_depth=0, best_buy_price=0, best_sell_price=0,low_transaction_price=0,high_transaction_price=0):
        super(TimeslotState, self).__init__()

        # Model parameters
        self.timeslot = timeslot
        self.base_price = base_price
        self.spread = spread
        self.buy_order_depth = buy_order_depth
        self.sell_order_depth = sell_order_depth
        self.best_buy_price = best_buy_price
        self.best_sell_price = best_sell_price
        self.high_transaction_price = high_transaction_price
        self.low_transaction_price = low_transaction_price
        
        