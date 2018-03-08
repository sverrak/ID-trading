    # Description: The purpose of this program is to split the data from the original EPEX self.Orderbooks from Markedskraft, where 15-31 days where bundled together,
# so that all orders of a day are saved in a separate file. Also, some effort has been made in order to try to split each delivery product.
# Status: Not working properly. Hypothesis: Change in data complicates the process from sept-16

### External packages
from datetime import datetime as dt
import xlsxwriter
import datetime
import time

class Data_Organizer(object):
    """docstring for Data_Organizer"""
    def __init__(self, generate_orderbook_urls=False):
        super(Data_Organizer, self).__init__()

        # Datastructure instantiation. Used to 
        self.years                             = ["2014","2015","2016","2017"]
        self.months                         = [str(i) if i>9 else "0"+str(i) for i in range(1,13)]
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
                        if(y != "2016" or m != "09"):
                            self.orderbooks[(y,m,1)]     = ("ComXervOrderbooks_" + y + "_" + m + "_01-" + y + "_" + m + "_15"".txt")
                            self.orderbooks[(y,m,2)]     = ("ComXervOrderbooks_" + y + "_" + m + "_16-" + y + "_" + m + "_"+str(self.days_of_months[int(m)])+".txt")
            
        # Custom orderbook URL
        else:
            self.orderbooks[("2016","09",0)]         = "ComXervOrderbooks_2016_09_01-2016_09_15.txt"

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
    def split_data(self, data, date_range):
        
        index_of_dp                 = 18
        out_data                     = {}
        start_date                    = date_range[0]
        end_date                    = date_range[1]
        dates                         = self.date_strings_between(start_date, end_date)
        
        # Create date keys in out_data dictionary
        for d in dates:
            out_data[d]             = []

        # Loop through the data and add the order to its correct key (date)
        for i,line in enumerate(data):
            
            if(i % 200000 == 0):
                print("\t* Progression (splitting): ", int(float(i)/float(len(data))*1000)/10, "%")
            line_list                 = [i for i in line.split()]
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
                
                # If the line (order) is not a quarterly delivery product, we carry the order forward
                mm = date_str[date_str.find(":") + 1] + date_str[date_str.find(":") + 2]
               
                if(mm == "00"):
                    try:
                        out_data[date_str].append(line_list)
                    except:
                        #print("KeyError line 59: ", date_str, " not in out_data (should be a date).")
                        date_str            = str(line_list[24]) + " " + str(line_list[25])
                        if(str(date_str[14:16]) not in [15, 30, 45]):
                            out_data[date_str].append(line_list)
            except:
                print("Something happened.")

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
        for key in list(self.orderbooks.keys())[::-1]:
            print("Currently examining orderbook", key, "/", number_of_keys)
            y = key[0]      # Year
            m = key[1]      # Month
            x = key[2]      # Technical variable indicating if the current dataset is a full month (0), first half (1) or second half (2)
            
            # Read the data
            self.read_data(self.orderbooks[key], actual_run=True)
            
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
                    time_now = time.time()
                    print("\tCurrent DP: ", day, ". Total progression within orderbook URL:", int(100*float(i)/len(splitted_data.keys())), "%")
                    self.create_file("Data/",day, splitted_data[day])
                    print("\tElapsed time:",(time.time() - time_now), "s.\n")
                
                # Remove data from memory
                del self.orderbooks[key]
                del splitted_data
                del self.data
                  
        print("Total running time: ", time.time() - self.program_starting_time)
    


if __name__ == '__main__':
    # Create a Data_Organizer object and organize()
    organizer = Data_Organizer(generate_orderbook_urls=True)
    organizer.organize()

