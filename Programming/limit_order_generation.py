from number_generation import NumberGenerator

# How to interpret results:
# - 0: CPR level 1
# - 1: CPR level 2
# - 2: CPR level 3
# - 3: 
class CPRIndicatorGenerator:

    def __init__(self, number_of_dps, number_of_scenarios, number_of_stages, buy_indicator, number_of_levels=4, gate_closures=None, correlation_matrix=None, transition_matrix=None):
        ng = NumberGenerator(number_of_dps, number_of_scenarios, number_of_stages)
        self.uncorrelated_variables = ng.get_uncorr_vars()

        self.number_of_dps = number_of_dps
        self.number_of_scenarios = number_of_scenarios
        self.number_of_stages = number_of_stages
        self.number_of_price_levels = number_of_levels
        self.cpr_indicators = []
        self.is_buy = buy_indicator == "buy"
        
        if(self.is_buy == True):
        	self.price_level_probabilities = [0.2,0.39,0.67,1.0]

        else:
        	self.price_level_probabilities = [0.2,0.38,0.64,1.0]
        
        self.setup_indicators()


    def setup_indicators(self):
    	self.cpr_indicators = [[[0 for x in range(self.number_of_dps)] for y in range(self.number_of_stages)] for z in range(self.number_of_scenarios)]

    	for s in range(self.number_of_scenarios):
    		for y in range(self.number_of_stages):
    			for x in range(self.number_of_dps):
    				for p in range(self.number_of_price_levels):
    					if(self.uncorrelated_variables[s][y][x] <= self.price_level_probabilities[p]):
    						self.cpr_indicators[s][y][x] = p
    						break


    def get_indicators(self):
    	return self.cpr_indicators
    def printer(self):
    	for s in range(self.number_of_scenarios):
    		for y in range(self.number_of_stages):
    			print(self.cpr_indicators[s][y])

