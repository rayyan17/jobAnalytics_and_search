import os

from pyspark.sql.functions import col
from pyspark.sql.functions import lit
from pyspark.sql.functions import lower
from pyspark.sql.functions import udf

from data_lake.data_util import DataUtil


class StackOverflowDev(DataUtil):

	def __init__(self, spark_session, source_path):
		super().__init__(spark_session)
		
		self.source = "stackoverflow"
		self.source_path = source_path

	def generate_developer_details(self, write_path):
		resp_cols = ["Respondent as person_id", "Hobby as hobby",
					 "OpenSource as open_source_contrib", 
					 "Country as country", "Student as student", 
					 "Employment as employment", "FormalEducation as main_education", 
					 "DevType as development_area", "LastNewJob as latest_job", 
					 "TimeFullyProductive as productive_hours", "Gender as gender", 
					 "Age as age"]

		df_dev = self.main_df.selectExpr(*resp_cols)

		w_path = os.path.join(write_path, f"df_devloper_{self.source}.csv")
		df_dev.toPandas().to_csv(w_path, index=False)


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

# 	l = StackOverflowDev(spark, source_path="../final_data/Data_Science_Jobs_StackOverflow/*.csv")
# 	l.read_data_from_source()
# 	l.generate_developer_details("/home/rayyan/Projects/Udacity_Capstone_Project_Data_Science_Jobs/final_data/output/developers")

# 	spark.stop()
