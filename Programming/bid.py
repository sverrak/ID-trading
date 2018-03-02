# Bid class 
import datetime

class Bid(object):
	"""docstring for Bid"""
	def __init__(self, price, volume, timestamp, isBuy, order_id, isCustomer):
		super(Bid, self).__init__()
		self.price = price
		self.volume = volume
		self.timestamp = timestamp
		self.isOpen = True
		self.isBuy = isBuy
		self.order_id = order_id
		self.isCustomer = isCustomer

	def compute_maturity(self, now):
		return datetime.timedelta(now, self.timestamp)

	def reduce_volume(self, volume):
		if(self.volume > volume):
			self.volume = self.volume - volume
			return volume
		else:
			self.volume = 0
			self.isOpen = False
			return self.volume


	def kill_bid(self):
		self.isOpen = False

	def get_timestamp(self):
		return self.timestamp






