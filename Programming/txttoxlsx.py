# External packages
import time
import xlsxwriter
import re

# Parameters
share=0.05 # Share of data to be included in spreadsheet
date = "2014-07-01"
dps = [i for i in range(24)]
strdps = [str(i) if i>9 else "0"+str(i) for i in range(24)]
str_delivery_products = [date+" "+strdp+":00:00.0000000" for strdp in strdps]
xlsx_filenames = ["Data/Orderbook"+date+"dp"+strdp+".txt" for strdp in strdps]
#xlsx_filename = "Orderbook"+date+"dp"+strdp+".xlsx"
filename = "Data/ComXervOrderbooks_2014_07.txt"
start = time.time()
number_of_columns = 19
index_of_dp = 18
index_of_duration = 15

def read_data(filename):
	print("\nReading data...")
	start = time.time()
	# File handling
	with open(filename) as f:
	    data = f.readlines()
	print("Time: " + str(int(10*time.time()-10*start)/10) + " seconds")
	return data[:int(share*len(data))]


def write_dp_to_file(dp, xlsxfile, data):
	book = xlsxwriter.Workbook("Data/"+xlsxfile)
	sheet = book.add_worksheet("Data")
	print("Processing data...")
	start = time.time()
	processed_data = []
	for i in range(int(len(data))):
		processed_data.append(re.split(r'\t+', data[i]))
	print("Time: " + str(int(10*time.time()-10*start)/10) + " seconds")
	print("Writing data...")
	start= time.time()

	# Fill spreadsheet
	headers = ['RevisionNo ', 'PxDir', 'TotalQty', 'CxDlvryAreaPID', 'LastPx', 'LastQty', 'LastTrdTime', 'IsDelta', 'ServerUtcTimestamp', 'IsBuy', 'OrdrEntryTime', 'Px', 'Qty', 'OrdrId', 'OrdrExeRestriction', 'Duration', 'Predefined', 'DlvryEnd', 'DlvryStart', 'LongName', 'Prod', 'ContractId', 'EndTime', 'ShareOfBidExecuted', 'AverageExecutionPrice', 'ResidualVolume' 'MaxExecutionPrice', 'LastTradedTime']
	for h in range(len(headers)):
		sheet.write(0, h, headers[h])


	for i in range(len(processed_data)):
		for j in range(number_of_columns):
			try:
				sheet.write(i+1, j, float(processed_data[i][j]))
			except:
				sheet.write(i+1, j, processed_data[i][j])

	book.close()

	print("Time: " + str(int(10*time.time()-10*start)/10) + " seconds")

def separate_data(data):
	start = time.time()
	keys = strdps
	bid_dict = {}
	for k in keys:
		bid_dict[k] = []

	for i,d in enumerate(data):
		for j,k in enumerate(keys):
			dt = re.split(r'\t+', d)
			if(dt[index_of_duration] == "1"):
				if dt[18] == str_delivery_products[j]:
					bid_dict[k].append(dt)

	print("Time: " + str(int(10*time.time()-10*start)/10) + " seconds")
	return bid_dict

data = read_data(filename)
bid_dict = separate_data(data)

start = time.time()
for i,k in enumerate(bid_dict.keys()):
	print(str(i) + ": " + str(len(bid_dict[k])))
	
	with open(xlsx_filenames[i], 'w') as f:
		for j,d in enumerate(bid_dict[k]):
			f.write('\t'.join(bid_dict[k][j]) + '\n')

print("Time: " + str(int(10*time.time()-10*start)/10) + " seconds")
	
	#write_dp_to_file(k, xlsx_filenames[i], bid_dict[k])



