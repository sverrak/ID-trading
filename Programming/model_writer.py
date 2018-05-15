import xlsxwriter

class ModelWriter(object):
	"""docstring for ModelWriter"""
	def __init__(self, file_name, variables):
		super(ModelWriter, self).__init__()
		self.variables_file_name = file_name
		self.variables = variables
		self.write_variables_to_file()


		### ----------- Support Functions -----------
	def write_variables_to_file(self, variables):
		book = xlsxwriter.Workbook(self.variables_file_name)
		sheet = book.add_worksheet("Variables")

		# Fill spreadsheet
		counter = 0
		for v in self.variables:
			sheet.write(counter, 0, str(v.Varname) + " " + str(v.X))
			counter += 1

		book.close()


		