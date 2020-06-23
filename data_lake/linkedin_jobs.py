import os

from pyspark.sql.functions import col
from pyspark.sql.functions import lit
from pyspark.sql.functions import lower
from pyspark.sql.functions import udf

from data_lake.data_util import DataUtil


class LinkedInJobs(DataUtil):

	def __init__(self, spark_session, source_path):
		super().__init__(spark_session)

		self.source = "linkedin"
		self.source_path = source_path


	def generate_jobs_table(self, write_path):
		job_cols = ["Job_Title as job_title", "Description as job_description", 
					"Company as company", "Location as location", 
					"date_data_created as source_fetch_date"]

		df_jobs = self.main_df.selectExpr(*job_cols)
		df_jobs = df_jobs.withColumn("job_title", lower(df_jobs.job_title))
		df_jobs = df_jobs.withColumn("source", lit(self.source))

		w_path = os.path.join(write_path, f"df_jobs_{self.source}.csv")
		df_jobs.toPandas().to_csv(w_path, index=False)


	def generate_date_description_table(self, write_path):
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
		location_cols = ["Company as company", "Location as location"]
		df_job_location = self.main_df.selectExpr(*location_cols)
		df_job_location = df_job_location.withColumn("city", self._get_city(df_job_location.location))
		df_job_location = df_job_location.withColumn("state", self._get_state(df_job_location.location))
		df_job_location = df_job_location.withColumn("country", self._get_country(df_job_location.location))
		df_job_location = df_job_location.dropDuplicates()

		w_path = os.path.join(write_path, f"df_job_location_{self.source}.csv")
		df_job_location.toPandas().to_csv(w_path, index=False)



# from pyspark.sql import SparkSession
# def create_spark_session():
#     spark = SparkSession \
#         .builder \
#         .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
#         .getOrCreate()
#     return spark



# from pyspark.sql import SparkSession


# if __name__ == "__main__":
# 	spark = create_spark_session()

# 	l = LinkedInJobs(spark, source_path="/home/rayyan/Projects/Udacity_Capstone_Project_Data_Science_Jobs/final_data/Data_Science_Jobs_Linkedin/*.csv")
# 	l.read_data_from_source()
# 	l.generate_jobs_table("/home/rayyan/Projects/Udacity_Capstone_Project_Data_Science_Jobs/final_data/output/job_data")
# 	l.generate_location_description("/home/rayyan/Projects/Udacity_Capstone_Project_Data_Science_Jobs/final_data/output/job_location")
# 	l.generate_date_description_table("/home/rayyan/Projects/Udacity_Capstone_Project_Data_Science_Jobs/final_data/output/job_date_details")
# 	l.unload_dataframe()

# 	spark.stop()
