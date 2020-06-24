"""Glassdoor Data Transformer Module"""

import os

from pyspark.sql.functions import col
from pyspark.sql.functions import lit
from pyspark.sql.functions import lower
from pyspark.sql.functions import udf


from data_lake.data_util import DataUtil


class GlassdoorJobs(DataUtil):
	"""Glassdoor Data Transformer"""

	def __init__(self, spark_session, source_path):
		super().__init__(spark_session)

		self.source = "glassdoor"
		self.source_path = source_path
		

	def generate_jobs_table(self, write_path):
		"""Generate data for jobs table"""
		df_jobs = self.main_df.select(col("Job Title").alias("job_title"), 
									col("Job Description").alias("job_description"), 
									col("Company Name").alias("company"), 
									col("Location").alias("location"), 
									col("date_data_created").alias("source_fetch_date"))

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
		df_job_location = self.main_df.select(col("Company Name").alias("company"),
											 col("Location").alias("location"))

		df_job_location = df_job_location.withColumn("city", self._get_city(df_job_location.location))
		df_job_location = df_job_location.withColumn("state", self._get_state(df_job_location.location))
		df_job_location = df_job_location.withColumn("country", self._get_country(df_job_location.location))
		df_job_location = df_job_location.dropDuplicates()

		w_path = os.path.join(write_path, f"df_job_location_{self.source}.csv")
		df_job_location.toPandas().to_csv(w_path, index=False)

	def generate_job_reviews(self, write_path):
		"""Generate data for job_rating table"""
		df_job_reviews = self.main_df.select(col("Job Title").alias("job_title"),
											 col("Company Name").alias("company"), 
											 col("Rating").alias("rating"))

		df_job_reviews = df_job_reviews.withColumn("max_rating", lit(5))
		df_job_reviews = df_job_reviews.dropDuplicates()


		w_path = os.path.join(write_path, f"df_job_reviews_{self.source}.csv")
		df_job_reviews.toPandas().to_csv(w_path, index=False)

	def generate_job_salary(self, write_path):
		"""Generate data for job_salary table"""
		df_job_salary = self.main_df.select(col("Job Title").alias("job_title"),
											 col("Company Name").alias("company"), 
											 col("Salary Estimate").alias("estimated_salary"))
		
		df_job_salary = df_job_salary.withColumn("estimated_salary", self.get_est_salary(df_job_salary.estimated_salary))
		df_job_salary = df_job_salary.dropDuplicates()


		w_path = os.path.join(write_path, f"df_job_salary_{self.source}.csv")
		df_job_salary.toPandas().to_csv(w_path, index=False)

	def generate_job_sector(self, write_path):
		"""Generate data for job_sector table"""
		df_job_sector = self.main_df.select(col("Job Title").alias("job_title"), 
											col("Sector").alias("sector"))
		
		df_job_sector = df_job_sector.dropDuplicates()

		w_path = os.path.join(write_path, f"df_job_sectors_{self.source}.csv")
		df_job_sector.toPandas().to_csv(w_path, index=False)


	@staticmethod
	@udf
	def get_est_salary(salary_range):
		"""Remove redundant info from salary text
		Args:
			salary_range(str): salary information

		Return: (str) with extra info removed


		"""
		to_repl = "(Glassdoor Est.)"
		return salary_range.replace(to_repl, "").strip()
