# Status: Incomplete
# Description: This file is an implementation of the Approximate Dynamic Programming method

import numpy as np

# Parameters
params = 0
number_of_stages = 12*3
number_of_iterations = 100


# Data structures
states = np.ndarray(params)
stages = np.ndarray([number_of_stages])
value_table = 0
v_hat = np.ndarray([])

### Step 0
# Initialization

value_table = np.ndarray(params) # Value_table is the data structure containing the value table
current_state = choose_initial_state() # Current state



### ADP loop

for n in range(number_of_iterations);
### Step 1
path = []



### Step 2
# Forward pass

for stage in stages:
	x[stage], v_hat[stage] = find_best_action(params)

# Backward pass
for stage in stages[::-1]:
	return 0








### Support functions
def choose_initial_state():
	return 0

def find_best_action(params):
	return 0,0

