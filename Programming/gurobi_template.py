# Status: 
# Description: Initial implementation of the hydropower producer's trading problem

### ----------- External Packages -----------
from gurobipy import *

### ----------- Model Initialization -----------
model = Model('Intraday_Trading_Problem')


### ----------- System Parameters -----------
print_output = True
default_parameters = True
parameter_file = "hydro_parameters.txt"


### ----------- Model Parameters -----------
if (default_parameters == True):
  # Set model parameters
  params = 0
  number_of_trading_stages = 12


else:
  # Retrieve parameters from file
  params = itphelper.read_parameters(parameter_file)




### ----------- Datastructures -----------


### ----------- Variables -----------

### ----------- Constraints -----------

### ----------- Objective Function -----------
# Inspiration:

#model.setObjective(end_products.prod(end_product_profit), GRB.MAXIMIZE)

#print(v_c)
model.setObjective((sum(p_c[i][j]*v_c[i][j] for j in range(len(p_c[i])) for i in range(len(p_c))) - sum(c_c*v_c[i][j] for j in range(len(p_c[i])) for i in range(len(p_c))) - c_p*q - (p_bm_pos*v_bm_pos - p_bm_neg*v_bm_neg)), GRB.MAXIMIZE)


### ----------- Optimization -----------
model.optimize()



### ----------- Output Results -----------
for v in model.getVars():
    if v.X != 0:
        print("%s %f" % (v.Varname, v.X))

model.write("itp_output.sol")

### ----------- Support Functions -----------


