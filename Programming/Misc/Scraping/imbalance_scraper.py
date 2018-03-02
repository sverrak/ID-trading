
from bs4 import BeautifulSoup

import requests
import xlwt
import datetime

numdays = 1370-365
start_date = datetime.datetime.strptime(str("2015-01-01 00:00:00"), '%Y-%m-%d %H:%M:%S')
base = start_date
#base = datetime.date.today()
date_list = [base + datetime.timedelta(days=x) for x in range(0, numdays)]
dates = []
dates_out = []

for i in range(len(date_list)):
	dates.append(date_list[i].strftime('%d.%m.%Y'))
	dates_out.append(date_list[i].strftime('%Y-%m-%d'))


def create_tables():
	book = xlwt.Workbook(encoding="utf-8")
	tab = []

	for x, d in enumerate(dates):
		print(d)
		#"https://transparency.entsoe.eu/balancing/r2/imbalance/show?name=&defaultValue=true&viewType=TABLE&areaType=MBA&atch=false&dateTime.dateTime=25.09.2017+00:00|CET|DAYTIMERANGE&dateTime.endDateTime=25.09.2017+00:00|CET|DAYTIMERANGE&marketArea.values=CTY|10Y1001A1001A83F!MBA|10Y1001A1001A82H&dateTime.timezone=CET_CEST&dateTime.timezone_input=CET+(UTC+1)+/+CEST+(UTC+2)
		url = 	"https://transparency.entsoe.eu/balancing/r2/imbalance/show?name=&defaultValue=true&viewType=TABLE&areaType=MBA&atch=false&dateTime.dateTime="+d+"+00:00|CET|DAYTIMERANGE&dateTime.endDateTime="+d+"+00:00|CET|DAYTIMERANGE&marketArea.values=CTY|10Y1001A1001A83F!MBA|10Y1001A1001A82H&dateTime.timezone=CET_CEST&dateTime.timezone_input=CET+(UTC+1)+/+CEST+(UTC+2)"

		r  = requests.get(url)

		data = r.text

		soup = BeautifulSoup(data, "lxml")
		
		try:
			table = soup.find('table').find_all("tr")
			
			for i in range(3,len(table)):
				
				cells = []
				k = table[i].find("td")
				row = []
				row.append(dates_out[x] + " " + str(k)[31:44])
				
				#print(len(table[i]))
				#print(table[i].find_all("td"))
				tds = table[i].find_all("td")
				
				
				#print([str(x) for x in tds])
				#print(len(tds))
				for td in range(1, len(tds)):
					cet = str(tds[td]).find("CET")
					if(cet == None or cet == -1):
						continue
					else:
						
						
						first = int(str(tds[td]).find(">"))
						
						snd = str(tds[td])[first+1:].find(">")
						start = int(str(tds[td])[first+snd+1:].find(">"))
						
						
						end = int(str(tds[td])[first+snd+start:].find("<"))
						cell = str(tds[td])[first+snd+start+2:first+snd+start+end].strip()
						if(cell == "N/A"):
							row.append("-1")
							continue
						elif( cell != "" and cell != "Intermediate" and cell!="Final"):
							row.append(cell)
							continue
						elif(cell == ""):
							row.append("-1")
							continue
						
						#print(str(tds[td])[first+snd+start+2:first+snd+start+end].strip())
						#row.append(int(str(tds[td])[first+snd+start+2:first+snd+start+end].strip()))
						#print((row))

						#print("hei: " + str(tds[td])[first+start+snd+2:first+snd+start+end].strip())
						

				tab.append(row)
		except:	
			print("Something happened")
		
	# Writing to new sheet
	sheet = book.add_sheet("Data")
	sheet.write(0, 0, "TimeOfDay")
	sheet.write(0, 1, "PosImbalancePrice")
	sheet.write(0, 2, "NegImbalancePrice")
	sheet.write(0, 3, "TotalImbalance")
	sheet2 = book.add_sheet("Data2")
	sheet2.write(0, 0, "TimeOfDay")
	sheet2.write(0, 1, "PosImbalancePrice")
	sheet2.write(0, 2, "NegImbalancePrice")
	sheet2.write(0, 3, "TotalImbalance")
	sheet3 = book.add_sheet("Data3")
	sheet3.write(0, 0, "TimeOfDay")
	sheet3.write(0, 1, "PosImbalancePrice")
	sheet3.write(0, 2, "NegImbalancePrice")
	sheet3.write(0, 3, "TotalImbalance")
	#sheet.write(5, 4, "Sum")

	for i, l in enumerate(tab[:50000]):
		#print(l)
		for j, col in enumerate(l):
		#	print(col)
			if(j==0 or col=="-1"):
				sheet.write(i+1, j, col)
			else:
				#print(col)
				sheet.write(i+1, j, float(col))
	try:
		for i, l in enumerate(tab[50000:100000]):
			#print(l)
			for j, col in enumerate(l):
			#	print(col)
				if(j==0 or col=="-1"):
					sheet2.write(i+1, j, col)
				else:
					#print(col)
					sheet2.write(i+1, j, float(col))
	except:
		do_nothing = 0 
	try:
		for i, l in enumerate(tab[100000:]):
			#print(l)
			for j, col in enumerate(l):
			#	print(col)
				if(j==0 or col=="-1"):
					sheet3.write(i+1, j, col)
				else:
					#print(col)
					sheet3.write(i+1, j, float(col))
	except:
		do_nothing = 0


		#sheet.write(i+6, j+1, sum(float(x) for x in l[1:]))
	print("Market imbalance for " + str(d) + " is created.")

	book.save('Data/imbalance.xls')
create_tables()