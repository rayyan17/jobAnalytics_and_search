"""Project Entry Point"""

import configparser
import os


from data_lake.glassdoor_jobs import GlassdoorJobs
from data_lake.indeed_jobs import IndeedJobs
from data_lake.linkedin_jobs import LinkedInJobs
from data_lake.stackoverflow_dev import StackOverflowDev
from pyspark.sql import SparkSession


def create_spark_session():
	"""Creates Spark Session in Local Mode"""
	spark = SparkSession \
		.builder \
		.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
		.getOrCreate()

	return spark


def etl(spark, s_paths, w_paths):
	"""Process data coming from Multiple Sources

	Args:
		spark (Spark): spark session instance
		s_paths (dict): A dictionary with source name and its path
		w_paths (dict): A dictionary with destination name and its path

	"""

	l = LinkedInJobs(spark, source_path=s_paths["LINKEDIN_PATH"])
	l.read_data_from_source()
	l.generate_jobs_table(w_paths["JOBS_TABLE"])
	l.generate_location_description(w_paths["LOCATION_TABLE"])
	l.generate_date_description_table(w_paths["DATE_TABLE"])

	i = IndeedJobs(spark, source_path=s_paths["INDEED_PATH"])
	i.read_data_from_source()
	i.generate_jobs_table(w_paths["JOBS_TABLE"])
	l.generate_location_description(w_paths["LOCATION_TABLE"])
	i.generate_date_description_table(w_paths["DATE_TABLE"])
	i.generate_job_reviews(w_paths["JOB_REVIEWS"])

	g = GlassdoorJobs(spark, source_path=s_paths["GLASSDOOR_PATH"])
	g.read_data_from_source()
	g.generate_jobs_table(w_paths["JOBS_TABLE"])
	g.generate_location_description(w_paths["LOCATION_TABLE"])
	g.generate_date_description_table(w_paths["DATE_TABLE"])
	g.generate_job_reviews(w_paths["JOB_REVIEWS"])
	g.generate_job_salary(w_paths["JOB_SALARY"])
	g.generate_job_sector(w_paths["JOB_SECTOR"])

	s = StackOverflowDev(spark, source_path=s_paths["STACKOVERFLOW_PATH"])
	s.read_data_from_source()
	s.generate_developer_details(w_paths["DEVELOPER_DETAILS"])


def main():
	"""Main Entry Function"""
	config_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "lake.cfg")
	config = configparser.ConfigParser()
	config.read(config_path)

	os.environ['AWS_ACCESS_KEY_ID']=config["AWS"]['AWS_ACCESS_KEY_ID']
	os.environ['AWS_SECRET_ACCESS_KEY']=config["AWS"]['AWS_SECRET_ACCESS_KEY']


	spark = create_spark_session()

	etl(spark, config["SOURCE_PATH"], config["WRTIE_PATH"])

	spark.stop()


if __name__ == "__main__":
	main()
