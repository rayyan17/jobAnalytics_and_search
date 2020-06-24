"""Base Class Module"""

from pyspark.sql.functions import udf


class DataUtil:
	"""Base Utitly Class with common functions to inherit"""

	def __init__(self, spark_session):
		self.spark = spark_session
		self.main_df = None

	def read_data_from_source(self):
		"""Read Data from the source path and assign it to main df attribute"""
		self.main_df = self.spark.read.csv(self.source_path, 
											header=True, 
											sep=",", 
											multiLine=True, escape='"')

	def unload_dataframe(self):
		"""Free data from Memory"""
		try:
			del self.main_df
		except KeyError:
			pass
		finally:
			self.main_df = None

	@staticmethod
	@udf
	def _get_city(x):
		"""Get city from the string of specific pattern: city, state, country
		Args:
			x(str): text

		Return: (str) city name

		"""
		if len(x.split(",")) >= 1:
			return x.split(",")[0]

		return None

	@staticmethod
	@udf
	def _get_state(x):
		"""Get states from the string of specific pattern: city, state, country
		Args:
			x(str): text

		Return: (str) state name

		"""
		if len(x.split(",")) >= 2:
			return x.split(",")[1]

		return None

	@staticmethod
	@udf
	def _get_country(x):
		"""Get country from the string of specific pattern: city, state, country
		Args:
			x(str): text

		Return: (str) country name

		"""
		if len(x.split(",")) >= 3:
			return x.split(",")[2]

		return None

	@staticmethod
	@udf
	def _get_year(x):
		"""Get year from the string of specific pattern: yyyy-mm-dd
		Args:
			x(str): text

		Return: (str) year

		"""
		return x.split("-")[0]

	@staticmethod
	@udf
	def _get_month(x):
		"""Get month from the string of specific pattern: yyyy-mm-dd
		Args:
			x(str): text

		Return: (str) month

		"""
		return x.split("-")[1]

	@staticmethod
	@udf
	def _get_day(x):
		"""Get day from the string of specific pattern: yyyy-mm-dd
		Args:
			x(str): text

		Return: (str) day

		"""
		return x.split("-")[2]
