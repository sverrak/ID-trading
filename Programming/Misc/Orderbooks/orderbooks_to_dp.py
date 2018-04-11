    # Description: The purpose of this program is to split the data from the original EPEX self.Orderbooks from Markedskraft, where 15-31 days where bundled together,
# so that all orders of a day are saved in a separate file. Also, some effort has been made in order to try to split each delivery product.
# Status: Not working properly. Hypothesis: Change in data complicates the process from sept-16

### External packages
from datetime import datetime as dt
import xlsxwriter
import datetime
import time
import os

class Data_Organizer(object):
    """docstring for Data_Organizer"""
    def __init__(self, generate_orderbook_urls=False):
        super(Data_Organizer, self).__init__()

        # Datastructure instantiation. Used to 
        self.years                             = ["2016"]
        self.months                         = [str(i) if i>9 else "0"+str(i) for i in range(3,5)]
        self.days_of_months                    = [0,31,28,31,30,31,30,31,31,30,31,30,31]
        self.orderbooks                     = {}
        self.program_starting_time             = time.time()
        self.generate_orderbook_urls        = generate_orderbook_urls
        self.data                             = []
        self.hours                             = [str(i) if i>9 else "0"+str(i) for i in range(0,24)]                    # hours of the day. Sheet names
        self.dps                                 = [(str(hh) + ":00:00") for hh in self.hours]                    # Delivery products
        
        

        # Generate orderbook URLs
        if(self.generate_orderbook_urls == True):
            for y in self.years:
                for i,m in enumerate(self.months):
                    if(int(y) < 2016 and int(m) < 10):

                        self.orderbooks[(y,m,0)]     = ("ComXervOrderbooks_" + y + "_" + m + ".txt")
                        print(("ComXervOrderbooks_" + y + "_" + m + ".txt"))
                    else:
                        if(y != "2016" or m != "09" and (y != "2017" or m != "02")):
                            self.orderbooks[(y,m,1)]     = ("ComXervOrderbooks_" + y + "_" + m + "_01-" + y + "_" + m + "_15"".txt")
                            self.orderbooks[(y,m,2)]     = ("ComXervOrderbooks_" + y + "_" + m + "_16-" + y + "_" + m + "_"+str(self.days_of_months[int(m)])+".txt")
                        elif((y == "2016" and m == "09") or (y == "2016" and m == "11") or (y == "2016" and m == "10")):
                            self.orderbooks[(y,m,2)]     = ("ComXervOrderbooks_" + y + "_" + m + "_16-" + y + "_" + m + "_"+str(self.days_of_months[int(m)])+".txt")
            
        # Custom orderbook URL
        else:
            if(True):
                self.orderbooks[("2016","08",2)]         = "ComXervOrderbooks_2016_08_16-2016_08_31.txt"
            else:    
                self.orderbooks[("2016","03",1)]         = "ComXervOrderbooks_2016_03_01-2016_03_15.txt"
                self.orderbooks[("2016","04",1)]         = "ComXervOrderbooks_2016_04_01-2016_04_15.txt"
                self.orderbooks[("2016","07",1)]         = "ComXervOrderbooks_2016_07_01-2016_07_15.txt"
                self.orderbooks[("2016","08",1)]         = "ComXervOrderbooks_2016_08_01-2016_08_15.txt"
                self.orderbooks[("2016","09",1)]         = "ComXervOrderbooks_2016_09_00-2016_09_15.txt"
                self.orderbooks[("2016","10",1)]         = "ComXervOrderbooks_2016_10_01-2016_10_15.txt"
                self.orderbooks[("2016","11",1)]         = "ComXervOrderbooks_2016_11_01-2016_11_15.txt"
                self.orderbooks[("2016","12",1)]         = "ComXervOrderbooks_2016_12_01-2016_12_15.txt"
                self.orderbooks[("2017","01",1)]         = "ComXervOrderbooks_2017_01_01-2017_01_15.txt"
                self.orderbooks[("2017","02",1)]         = "ComXervOrderbooks_2017_02_01-2017_02_15.txt"
                #self.orderbooks[("2016","05",1)]         = "ComXervOrderbooks_2016_05_01-2016_05_15.txt"
                #self.orderbooks[("2016","05",2)]         = "ComXervOrderbooks_2016_05_16-2016_05_31.txt"
                #self.orderbooks[("2016","06",1)]         = "ComXervOrderbooks_2016_06_01-2016_06_15.txt"
                #self.orderbooks[("2016","06",2)]         = "ComXervOrderbooks_2016_06_16-2016_06_30.txt"
                #self.orderbooks[("2016","07",1)]         = "ComXervOrderbooks_2016_07_01-2016_07_15.txt"
            

    # Takes a start and an end day and returns a list containing all the dates between these two days
    def date_strings_between(self, start,end):
        start_date             = dt.strptime(start + " " + "00:00:00", '%Y-%m-%d %H:%M:%S')
        end_date             = dt.strptime(end     + " " + "00:00:00", '%Y-%m-%d %H:%M:%S')
        dates_between_str    = []
        
        current_date        = start_date
        
        while(current_date    <= end_date):
            for dp in self.dps:
                dates_between_str.append(dt.strftime(current_date,'%Y-%m-%d') + " " + dp)
            current_date     = current_date + datetime.timedelta(days=1)

        return dates_between_str

    # Returns the content of file_name in a matrix. Actual_run=False runs this method in testing mode
    def read_data(self, file_name, actual_run=True):
        start = time.time()
        print("Reading data. ")

        if(actual_run):
            try:
                with open(file_name) as f:
                    data     = f.readlines()
                print("Elapsed time: " + str(time.time() - start))
                self.data     = data
            except:
                print("Could not find ", file_name)
                self.data     = []
        else:
            self.data         = [[i for i in range(20)] for j in range(30)]

    # Splits the orders into a dictionary with dates as keys and orders of that day as values
    def split_data(self, data, date_range,month=8):

        index_of_dp                 = 18
        out_data                     = {}
        start_date                    = date_range[0]
        end_date                    = date_range[1]
        dates                         = self.date_strings_between(start_date, end_date)
        
        # Create date keys in out_data dictionary
        for x,d in enumerate(dates):
            out_data[d]             = []
            
            if(x == len(dates) - 1):
                #print(d,dt.strftime(dt.strptime(d, '%Y-%m-%d %H:%M:%S') + datetime.timedelta(hours=1),'%Y-%m-%d %H:%M:%S'))
                additional_dp = dt.strftime(dt.strptime(d, '%Y-%m-%d %H:%M:%S') + datetime.timedelta(hours=1),'%Y-%m-%d %H:%M:%S')
                out_data[additional_dp] = []
        
        # Loop through the data and add the order to its correct key (date)
        # The following code is rather messy. 
        for i,line in enumerate(data):
            #if(i % 200000 == 0):
            #    print(i, data, i/len(data))
            if(month == 8):
                
                if(i % 200000 == 0):
                    print("\t* Progression (splitting): ", int(float(i)/float(len(data))*1000)/10, "%")
                
                
                line_list = line.split()
                if(len(line_list) < 9):
                    temp = []
                    for sub_list in line_list:
                        for cell in sub_list.split(";"):
                            temp.append(cell)
                    line_list = temp[:]
                    #line_list = [cell for cell in sub_list.split(";") for sub_list in line_list]
                try:
                    
                    d = line_list[20]
                    t = line_list[21][0:line_list[21].find(".")]
                    date_str = d + " " + t
                    index_of_colon = date_str.find(":")
                    mm = str(date_str[index_of_colon + 1: index_of_colon + 3])
                    
                    
                    if(date_str in out_data.keys() and mm in [00, "00"]):
                        out_data[date_str].append(line_list)
                    else:
                        pass
                    
                except:
                    print("Error at",i,":",line_list)
                    
                
                
                    
                    
            else:
                if(i % 200000 == 0):
                    print("\t* Progression (splitting): ", int(float(i)/float(len(data))*1000)/10, "%")
                line_list                 = [i for i in line.split(";")]
                try:
                    
                    index_of_dp_ub = min(4, len(line_list[index_of_dp]))
                    index_24_ub = min(4, len(line_list[23]))
                    index_25_ub = min(4, len(line_list[24]))
                    if(index_of_dp_ub >= 4 and line_list[index_of_dp][:4] in self.years):
                        try:
                            date_str            = str(list(line_list)[index_of_dp][:line_list[index_of_dp].find(".")])
                        except:
                            print("Error A")
                    elif(index_24_ub >= 4 and line_list[23][:4] in self.years):
                        date_str            = str(list(line_list)[23]) + " " + str(list(line_list)[24][:line_list[24].find(".")])
                    elif(index_25_ub >= 4 and line_list[24  ][:4] in self.years):
                        date_str            = str(list(line_list)[24]) + " " + str(list(line_list)[25][:line_list[25].find(".")])
                    else:
                        print("None of the alternatives are correct")
                except:
                    #print("Something happened dp index out of range mp")
                    date_str = "N/A"
                    pass
                                
                try:
                    out_data[date_str].append(line_list)
                except:
                    try:
                        index = 0
                        date_str = ""
                        #print(line_list)
                        line_l = []
                        for line in line_list:
                            line2 = line.split(";")
                            for x in line2:
                                line_l.append(x)
                                
                        
                        while(len(date_str) != 19):
                            if(True):
                                date_str = line_l[18][0:19]
                        
                            
                            elif(len(line_l[18+index][0:line_l[18+index].find(".")]) == 19):
                                date_str = line_l[18 + index]
                                index += 1
                            else:
                                #print(index, len(line_l))
                                #print(str(line_l[18+index])+" "+str(line_l[19+index])[:line_l[19+index].find(".")])
                                date_str            = str(line_l[18+index])+" "+str(line_l[19+index])[:line_l[19+index].find(".")]                    
                                index += 1
                        
                        index_of_colon = date_str.find(":")
                        mm = str(date_str[index_of_colon + 1: index_of_colon + 3])
                        #print(mm)
                        if(mm not in ["15", "30", "45", 15, 30, 45]):
                            #print(line_list)
                            if(date_str not in out_data.keys()):
                                out_data[date_str] = []
                            out_data[date_str].append(line_list)
                    except:
                        print("Error")
                        pass
                    
           

        return out_data


    # Creates a file in the folder 'folder' for date 'date' containing all orders organized by their delivery products.
    # Two modes are supported: is_xlsx=True (creates one xlsx file per DP) and is_xlsx=False (creates one txt file per DP)
    def create_file(self, folder, date, orders, is_xlsx=False):
        out_data                             = {}                                                                    # Dictionary containing all orders on a per-DP level
        index_of_dp                            = 18                                                                    # The index of the dp in the order list
        dps_of_day                              = [(str(date) + " " + dp) for dp in self.dps]
            
        if(is_xlsx):
            book                                 = xlsxwriter.Workbook(folder + "/" + date+".xlsx")                        # .xlsx file in folder 'folder' with date as file name
            # Create keys in dictionary (delivery products)
            for dp in dps_of_day:
                out_data[dp]                 = []

            # Loop through all orders and append order to its proper value list
            for i,order_str in enumerate(orders):
                if(i % 200000 == 0):
                    print("\t* Progression (organizing): ", int(float(i)/float(len(orders))*1000)/10, " %")
                order_list = order_str
                try:
                    out_data[order_list[index_of_dp][:19]].append(order_list)
                except:
                    print("Key error: ", order_list[index_of_dp][:19])

            # For each dp, create a new sheet in the book and add all orders for that dp to the list
            for dp in (out_data.keys()):
                print("Creating sheet for: ", date, dp)
                sheet = book.add_worksheet(str(dp)[11:13])
                for i,order in enumerate(out_data[dp]):
                    for j,attr in enumerate(order):
                        sheet.write(i, j, attr)

            # Finishing the process
            book.close()
            print("\tFile created for date ", date, ".")
        else:
            len_out_data = len(orders)
            print(os.stat(folder + dt.strftime(dt.strptime(date, '%Y-%m-%d %H:%M:%S'), '%Y-%m-%d_%H-%M-%S') + ".txt").st_size)
            if(os.stat(folder + dt.strftime(dt.strptime(date, '%Y-%m-%d %H:%M:%S'), '%Y-%m-%d_%H-%M-%S') + ".txt").st_size <= 0.1):    
                with open(folder + dt.strftime(dt.strptime(date, '%Y-%m-%d %H:%M:%S'), '%Y-%m-%d_%H-%M-%S') + ".txt", "w") as f:
                    if(len_out_data >= 1):
                        string = ""
                        for i,r in enumerate(orders):
                            if(i % 30000 == 0):
                                print("\t\t* Progression (organizing): ", int(float(i)/float(len(orders))*1000)/10, " %")
                            temp_string = "\t".join(r)
                            string = string + temp_string + "\n"
                        f.write(string)
                        del string
                    elif(len_out_data == 1):
                        string = ""
                        for r in out_data[dp]:
                            temp_string = ""
                            for c in r:
                                temp_string = temp_string + c + "\t" 
                            string = string + temp_string + "\n"
                        f.write(string)

    # Fetch and split the data
    def organize(self):

        number_of_keys = len(self.orderbooks.keys())
        print("Orderbook set: ", self.orderbooks.keys())
        # For each orderbook URL, split the orderbook and save the orders by their DP in separate files
        for key in list(self.orderbooks.keys()):
            print("Currently examining orderbook", key, "/", number_of_keys)
            y = key[0]      # Year
            m = key[1]      # Month
            x = key[2]      # Technical variable indicating if the current dataset is a full month (0), first half (1) or second half (2)
            
            # Read the data
            self.read_data(self.orderbooks[key], actual_run=True)
            print("Number of rows in",key,":",len(self.data))
            
            if(len(self.data) > 0):
                # Create the date bounds for the data based on the file name
                # Full month
                if(x == 0):
                    date_range = [y + "-" + m + "-" + "01", y + "-" + m + "-" + str(self.days_of_months[int(m)])]
                
                # First half of month
                elif(x == 1):
                    date_range = [y + "-" + m + "-" + "01", y + "-" + m + "-15"]
               
                # Second half of month
                else:
                    date_range = [y + "-" + m + "-" + "16", y + "-" + m + "-" + str(self.days_of_months[int(m)])]
                
                # Split the orders by their DP. Stored in a dictionary (splitted_data) with DPs as keys
                print("\n\t***** SPLITTING PHASE *****")
                splitted_data = self.split_data(self.data, date_range)
                print("\tDone splitting data.")
                
                # Create a file for each DP in the examined file
                print("\tCreate files.")
                for i,day in enumerate(splitted_data.keys()):
                    try:
                        
                        time_now = time.time()
                        print("\tCurrent DP: ", day, ". Total progression within orderbook URL:", int(100*float(i)/len(splitted_data.keys())), "%")
                        self.create_file("Data/",day, splitted_data[day])
                        print("\tElapsed time:",(time.time() - time_now), "s.\n")
                    except:
                        pass
                        
                
                # Remove data from memory
                del self.orderbooks[key]
                del splitted_data
                del self.data
                  
        print("Total running time: ", time.time() - self.program_starting_time)
    


if __name__ == '__main__':
    # Create a Data_Organizer object and organize()
    organizer = Data_Organizer(generate_orderbook_urls=False)
    organizer.organize()

