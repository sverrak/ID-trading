import time
class Transaction(object):
	"""docstring for Transaction"""
	def __init__(self, sell_bid, buy_bid, price, volume, timestamp):
		super(Transaction, self).__init__()
		self.sell_bid = sell_bid
		self.buy_bid = buy_bid
		self.price = price
		self.volume = volume
		self.timestamp = timestamp
		