# Bid class 
import datetime

class Bid(object):
    """docstring for Bid"""
    def __init__(self, price, volume, timestamp, timestamp2, isBuy, order_id, isCustomer, zone):
        super(Bid, self).__init__()
        self.price = price
        self.volume = volume
        self.timestamp = timestamp
        self.timestamp2 = timestamp2
        self.isOpen = True
        self.isBuy = isBuy
        self.order_id = order_id
        self.isCustomer = isCustomer
        self.zone = zone
        self.cleared_timestamp = "N/A"
        self.killed_timestamp = "N/A"

    def compute_maturity(self, now):
        
        return (now - self.timestamp)

    def reduce_volume(self, volume, timestamp=None):
        if(self.volume > volume):
            self.volume = self.volume - volume
            return volume
        else:
            self.volume = 0
            self.isOpen = False
            if(timestamp != None):
                self.cleared_timestamp = timestamp
            return self.volume


    def kill_bid(self,timestamp):
        self.isOpen = False
        self.killed_timestamp = timestamp

    def get_timestamp(self):
        return self.timestamp







