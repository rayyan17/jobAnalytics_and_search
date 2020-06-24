"""LinkedIn Data Transformer Module"""

import os

from pyspark.sql.functions import col
from pyspark.sql.functions import lit
from pyspark.sql.functions import lower
from pyspark.sql.functions import udf

from data_lake.data_util import DataUtil


class LinkedInJobs(DataUtil):
	"""LinkedIn Data Transformer"""

	def __init__(self, spark_session, source_path):
		super().__init__(spark_session)

		self.source = "linkedin"
		self.source_path = source_path


	def generate_jobs_table(self, write_path):
		"""Generate data for jobs table"""
		job_cols = ["Job_Title as job_title", "Description as job_description", 
					"Company as company", "Location as location", 
					"date_data_created as source_fetch_date"]

		df_jobs = self.main_df.selectExpr(*job_cols)
		df_jobs = df_jobs.withColumn("job_title", lower(df_jobs.job_title))
		df_jobs = df_jobs.withColumn("source", lit(self.source))

		w_path = os.path.join(write_path, f"df_jobs_{self.source}.csv")
		df_jobs.toPandas().to_csv(w_path, index=False)


	def generate_date_description_table(self, write_path):
		"""Generate data for time_details table"""
		date_cols = ["date_data_created as source_fetch_date"]
		df_jobs_source = self.main_df.selectExpr(*date_cols)
		df_jobs_source = df_jobs_source.withColumn("source", lit(self.source))
		df_jobs_source = df_jobs_source.withColumn("source_year", self._get_year("source_fetch_date"))
		df_jobs_source = df_jobs_source.withColumn("source_month", self._get_month("source_fetch_date"))
		df_jobs_source = df_jobs_source.withColumn("source_day", self._get_day("source_fetch_date"))
		df_jobs_source = df_jobs_source.dropDuplicates()

		w_path = os.path.join(write_path, f"df_job_date_fetch_{self.source}.csv")
		df_jobs_source.toPandas().to_csv(w_path, index=False)

	def generate_location_description(self, write_path):
		"""Generate data for company_location table"""
		location_cols = ["Company as company", "Location as location"]
		df_job_location = self.main_df.selectExpr(*location_cols)
		df_job_location = df_job_location.withColumn("city", self._get_city(df_job_location.location))
		df_job_location = df_job_location.withColumn("state", self._get_state(df_job_location.location))
		df_job_location = df_job_location.withColumn("country", self._get_country(df_job_location.location))
		df_job_location = df_job_location.dropDuplicates()

		w_path = os.path.join(write_path, f"df_job_location_{self.source}.csv")
		df_job_location.toPandas().to_csv(w_path, index=False)
