from pyspark.sql.functions import udf


class DataUtil:

	def __init__(self, spark_session):
		self.spark = spark_session
		self.main_df = None

	def read_data_from_source(self):
		self.main_df = self.spark.read.csv(self.source_path, 
											header=True, 
											sep=",", 
											multiLine=True, escape='"')

	def unload_dataframe(self):
		try:
			del self.main_df
		except KeyError:
			pass
		finally:
			self.main_df = None

	@staticmethod
	@udf
	def _get_city(x):
		if len(x.split(",")) >= 1:
			return x.split(",")[0]

		return None

	@staticmethod
	@udf
	def _get_state(x):
		if len(x.split(",")) >= 2:
			return x.split(",")[1]

		return None

	@staticmethod
	@udf
	def _get_country(x):
		if len(x.split(",")) >= 3:
			return x.split(",")[2]

		return None

	@staticmethod
	@udf
	def _get_year(x):
		return x.split("-")[0]

	@staticmethod
	@udf
	def _get_month(x):
		return x.split("-")[1]

	@staticmethod
	@udf
	def _get_day(x):
		return x.split("-")[2]
