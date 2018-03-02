# Status: 
# Description: A tool for collecting data from epexspot.com

### ----------- External Packages -----------
from bs4 import BeautifulSoup
import requests
import xlwt
import datetime
from datetime import datetime as datet

### ----------- System Parameters -----------
print_output = True
default_parameters = True
parameter_file = "hydro_parameters.txt"
output_file = "Data/ID_average_prices.xls"
url_prefix = "https://www.epexspot.com/en/market-data/intradaycontinuous/intraday-table/"
url_suffix = "/DE"


### ----------- Model Parameters -----------
if (default_parameters == True):
	# Set model parameters
	params = 0
	start_date_str = "2015-02-01 00:00:00"
	end_date_str = "2017-10-30 00:00:00"
	start_date = datet.strptime(str(start_date_str), '%Y-%m-%d %H:%M:%S')
	end_date = datet.strptime(str(end_date_str), '%Y-%m-%d %H:%M:%S')

else:
  # Retrieve parameters from file
  do_nothing = 0

### ----------- Support Functions -----------
def init_dps(start, end, dt):
	# Local datastructures
	iterator = start
	dates = []
	delivery_products = []
	hours = [str(i) for i in range(24)]
	suffix = ":00:00"

	# Post processing to get hours right
	for i, h in enumerate(hours):
		if(len(h) == 1):
			hours[i] = "0" + h

	# Loop through all dates and fill the dates and dp lists
	while iterator <= end_date:
		temp_list = [datet.strptime((iterator.strftime('%Y-%m-%d')+" "+str(i)+suffix), '%Y-%m-%d %H:%M:%S') for i in hours]
		delivery_products += temp_list
		dates.append(iterator)
		iterator = iterator + datetime.timedelta(days=dt)

	return dates, delivery_products


def create_tables(dates):
	book 			= xlwt.Workbook(encoding="utf-8")
	sheet 			= book.add_sheet("Data")
	suffix 			= ":00:00"
	tab 			= []
	index_of_low 	= 1
	index_of_high 	= 2
	index_of_last 	= 3
	index_of_avg 	= 4 
	index_of_buy 	= 8
	index_of_sell	= 9

	for d in dates:
		print("Currently examining date " + str(d))
		url = url_prefix + d.strftime('%Y-%m-%d') + url_suffix

		r  = requests.get(url)

		data = r.text

		soup = BeautifulSoup(data, "lxml")

		table = soup.find('table').find_all("tr")
		
		tds = []
		was = []

		for i in range(len(table)):
			
			cells = []
			k = table[i].find("td")
			row = []
			#print(str(k)[31:44])
			
			
			#print(len(table[i]))
			#print(table[i].find_all("td"))
			tds_temp = table[i].find_all("td", class_="title")
			wa_temp = table[i].find_all("td")
			for k in tds_temp:
				str_temp = k.get_text()[10:-6].strip()
#				print(str_temp)
				if(len(str_temp) < 10 and len(str_temp) > 1):
					tds.append(str_temp)

					hour = str_temp[0:2]
					if(hour != "me"):
						row.append((d.strftime('%Y-%m-%d')+" "+hour+suffix))

			# Loop through all records (DPs), and collect the cell values
			for index, wa in enumerate(wa_temp):
				if(str(wa)[0:18] == """<td class="title">"""):
					if(len(str(wa)[20:-6].strip()) < 10):
						text = str(wa_temp[index+4])
						colon = text.find(".")
						
						# For each column, collect the cell value correctly
						# Some effort was needed to get the decimals right
						for x in range(1,index_of_sell+1):
							try:	
								
								text = str(wa_temp[index+x])
								if(x == index_of_sell or x == index_of_buy):
									raise ValueError
								was.append(float(text[4:colon])+float(text[colon+1:-5])/100)
								row.append(float(text[4:colon])+float(text[colon+1:-5])/100)

							except:
								text = str(wa_temp[index+x])
								#print("index", x, "value", text)
								try:
									begin_index = text.find(">") + 1
									end_index = (text[1:]).find("<")
									if(text.find(".") >= 0):
										dot = text.find(".")
									else:
										dot = text.end_index

									integer = (text[begin_index:dot])
									int_list = list(integer)
									if(integer.find(",") >= 0):
										int_list.remove(",")
										
									integer = int(''.join(int_list))
									#print("A")
									#print(dot, end_index)
									decimal = float(text[dot+1:end_index+1])/10
									#print(decimal)
									#print("A")
									#print(integer, decimal)
									was.append(float(integer)+decimal)
									row.append(float(integer)+decimal)
								except:
									was.append(-99999)
									row.append(-99999)
									#print("B")

			tab.append(row)
	
	# Write to file	
	sheet.write(0, 0, "Delivery Product")
	sheet.write(0, 1, "Low Price")
	sheet.write(0, 2, "High Price")
	sheet.write(0, 3, "Last Price")
	sheet.write(0, 4, "Avg Price")
	
	while [] in tab:
		tab.remove([])
	for i, l in enumerate(tab):
		print(l)
		for j, col in enumerate(l):
		#	print(col)
			sheet.write(i+1, j, col)
				
	print("File " + " is created.")

	book.save(output_file)


### ----------- Datastructures -----------
dates, delivery_products = init_dps(start_date, end_date, 1)
create_tables(dates)