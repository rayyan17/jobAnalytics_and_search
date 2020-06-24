# 1. PROJECT (Job Search and Analysis System):
Job Search and Analysis Platform is a system which fetches data from various job posting platforms like LinkedIn, GlassDoor, Indeed etc. It extractes the useful information for example, job details, salaries offered, location of job, job types, company ratings and related departments. The system also gets information from Developer Community platforms like StackOverflow so that Buisness Analytics team can estimate which job is saturating and which needs more skilled workers. This system can also perform geo based analysis i.e. which country/city is offering most jobs and in which sector.

## 2. Data Resources:
In this pipeline we are using the following data resources:<br>

<p align="center">
<img src="readme_files/company_logos.png" width="50%" height="40%">
</p>

1. **LinkedIn** (https://www.kaggle.com/joshmantovani/data-science-jobs)
<br> <i>df_all_linkedin.csv</i> contains the info:<br>
<p align="left">
<img src="readme_files/linkedin_data_schema.png" width="50%" height="40%">
</p>
2. **Indeed** (https://www.kaggle.com/joshmantovani/data-science-jobs)
<br> <i>df_all_indeed.csv</i> contains the info:<br>
<p align="left">
<img src="readme_files/indeed_data_schema.png" width="50%" height="40%">
</p>
3. **Glassdoor** (https://www.kaggle.com/rkb0023/glassdoor-data-science-jobs)
<br> <i>df_all_glassdoor.csv</i> contains the info:<br>
<p align="left">
<img src="readme_files/glassdoor_data_schema.png" width="50%" height="20%"> 
</p>
4. **StackOverflow** (https://www.kaggle.com/stackoverflow/stack-overflow-2018-developer-survey)
<br> <i>
survey_results_public.csv</i> contains information about developers on StackOverflow. It has 129 columns with various useful info like his hobbies, skillset, education, employment etc.<br>

## 3. Choice of Tools and Technologies:
In order to explain the choice of tools and technologies for this project it is important to understand the data resources.

1. Data Resources are not limited to LinkedIn, Indeed, Glassdoor or StackOverflow. As the project grows further we will be adding more data sources like monster.com or frm other job info platforms.
2. Each of these resources have a complex schema with raw data that needs extensive processing and cleaning. 

Both of the above 2 points can be much easily cater by the **PySpark**. With spark architecture we can deal with heavy loads of data, pre-processing and cleaning because of Lazy Evaluation. It also has an NLP based packages that can help in text processing. Also as soon as we need more computation we can just increase the slave nodes.

**S3** will acts as our staging platform as well as a permanent storage for the data coming from multiple resources. S3 is a cheap storage and will be best to use in this data extensive pipeline.

**Redshift** will be used to shift processed data  by PySpark to corresponding tables. This way Business Analytics team can easily use OLAP queries on those tables. 

**Airflow** will play a very important role in keeping our data stores updated. Each day we get thousands of jobs update in platforms like LinkedIn, Glassdoor etc. Airflow will make sure that we maintain most recent data in our data stores.


## 4. Project Flow:
The project is divided into multiple modules:<br>

1. **ARCHITECTURE**:
<p align="center">
<img src="readme_files/job_analysis_architecture.png" width="100%" height="60%">
</p>

2. **Fetch Resources**:
<br> Currently, data is being downloaded from all these 4 resources and then moved towards the company dedicated storage on **S3**. We have maintained a bucket named <i>jobs-bucket</i> where multiple directories are available for data fetching and writing.
    1. LinkedIn data is uploaded to <a>s3://jobs-bucket/jobs_linkedin/</a>
    2. Glassdoor data is uploaded to <a>s3://jobs-bucket/jobs_glassdoor/</a>
    3. Indeed data is uploaded to <a>s3://jobs-bucket/jobs_indeed/</a>
    4. StackOverflow data is uploaded to <a>s3://jobs-bucket/stackoverflow/</a>
<br><br>In each of these soruces an additional columns is added **date_data_created**. This column contains the date when each time a data is downloaded form these sources.


3. **Data Lake**:
<br> Data Lake module majorly plays the role in interacting with the raw files coming from all the 4 sources. There are four transformers available in `data_lake` directory:
    1. GlassdoorJobs
    2. IndeedJobs
    3. LinkedInJobs
    4. StackOverflowDev

Each of these 4 transformer extracts useful information from **S3** bucket and generates 7 useful structures:

* Job_Details:<br>
<p align="center">
<img src="readme_files/job_table_file.png" width="100%" height="100%">
</p>

* Time_Details:<br>
<p align="center">
<img src="readme_files/fetch_details_table.png" width="70%" height="60%">
</p>

* Company_Location:<br>
<p align="center">
<img src="readme_files/job_location_table.png" width="70%" height="60%">
</p>

* Job_Rating:<br>
<p align="center">
<img src="readme_files/job_rating_table.png" width="60%" height="60%">
</p>

* Job_Salary<br>
<p align="center">
<img src="readme_files/job_salary_table.png" width="70%" height="60%">
</p>

* Job_Sector<br>
<p align="center">
<img src="readme_files/job_sector_table.png" width="50%" height="60%">
</p>

* Developers<br>
<p align="center">
<img src="readme_files/developer_tables.png" width="100%" height="60%">
</p>
Each of these files are shift into corresponding **S3** directories

<br>

4. **Shift Data Into Redshift**:
<p>All these files in S3 are then shited to the RedShift Tables.</p>

## 5. Data Model

1. **Schema Type**:
    <br>Data Model has 2 main sections: Jobs and developers.  Jobs section is following **Star Scehma** with jobs table as a fact table and (job_rating, company_location, time_details, job_salary, job_sector) as Dimension Tables. Developer section is a separate section in which Business Analytics team can use columns like country or development area to find the related jobs for the candidate.

2.  **Schema Goal**:
    <br> The schema is designed in a way to handle multiple job related queires like what is the most recent job, what is the salary range of a particular job_title in a company, location details for the posted job, or search jobs belongs to a particular sector
<p align="center">
<img src="readme_files/data_model.png" width="100%" height="60%">
</p>
3. **Sample Queries for BA team:**<br>
    Check Salary variations by a Company at different locations:
    ```sql
    SELECT company_location.country, company_location.city, job_salary.estimated_salary FROM job_salary 
    JOIN company_location on (job_salary.company = company_location.company);
    ```
    Show the most recent fetched jobs:
    ```sql
    SELECT jobs.job_title, jobs.company, jobs.location, 
    time_details.source_year, time_details.source_month, time_details.source_day 
    FROM jobs 
    JOIN time_details 
    on (jobs.source_fetch_date = time_details.source_fetch_date)
    order by source_year desc, source_month desc, source_day desc;
    ```


## 6. Data Pipeline:
Data Pipeline is available in directory `data_pipeline`. It consists of the following steps:<br>

1. Process Data in Data Lake
2. Create Tables for Business analytics team
3. Move New Data from S3 to the corresponding Tables
4. Perform Data Quality tests

<p align="center">
<img src="readme_files/data_pipeline.png" width="100%" height="100%">
</p>

## 7. Running The Project:
**Create Virtual Environment**
```commandline
# create python virtual environment
$ virtualenv .venv
$ source .venv/bin/activate

# Install Requirements
$ pip install -r requirements.txt
```

**Install Data Lake as a Package**
```commandline
# Do add your AWS credentials in data_lake/lake.cfg first
# make sure you are in the main directory where setup.py exists
$ pip install .

# This will install the package process-data-from-lake
# Run process-data-from-lake to just process data on lake
$ process-data-from-lake
```

**Give airflow script executable permissions**
```commandline
# make sure you are in the main directory where setup.py exists
$ chmod +x script/airflow_start.sh
```

**Running the Project**
```commandline
# make sure you are in the main directory where setup.py exists
# Run the script
$ script/airflow_start.sh
```

**Configurations**
```commandline
# Setup Redshift Connection:
-> Conn Type: Postgres
-> Host: Redshift Database URL
-> Schema: DB_NAME
-> Login: DB_USER
-> Password: DB_PASSWORD
-> Port: DB_PORT

# Setup AWS Connection:
-> AWS_ACCESS_KEY_ID: AWS_Key
-> AWS_SECRET_ACCESS_KEY: AWS_Secret_Access
```


After this go to <a>http://localhost:3001/</a> and run the DAG (jobs_analysis). Make sure to add all the credentials of Redshift and S3 in Admin panel before running the DAG.

## 8. Directory Structure:
```
/jobAnalytics_and_search

    - data_lake/
        - data_util.py
        - glassdoor_jobs.py
        - indeed_jobs.py
        - linkedin_jobs.py
        - stackoverflow_dev.py
        - process_data.py
        - lake.cfg
        
    - data_pipeline/
        - dags/
            - jobs_dag.py
        - plugins/
            - operators/
                - copy_redshift.py
                - data_quality.py
        - create_tables.sql

    - setup.py

"data_lake" folder includes code for fetching raw_data from S3, process it, and move it to S3

"data_pipeline" folder includes code for airflow pipeline to run the whole project daily

- setup.py builds command `process-data-from-lake` to run data lake task from bash

```

## 9. Additional Questions:

1. **The data was increased by 100x**:
<br>For data fetching and processing we are using Pyspark. So even if the data is increased that much it will still successfully process it with the cost of some time delay because of Spark Lazy Evaluation property. The time delay can also be handle by increasing more number of slave server nodes.
2. **The pipelines would be run on a daily basis by 7 am every day.**:
<br>The pipeline is set to 2 days ago date. In order to start it by 7am just switch on the DAG by 7am clock and it will keep its daily iteration by 7am daily.
3. **The database needed to be accessed by 100+ people.**:
<br>If the pipeline is get accessed by 100+ people that our 2 modules will get the major hits:

* **Amazon Redshift**, since a lot of people will be requesting OLAP queires at a time. In order to configure concurrecny scaling on Redshift we will route eligible queires to new, dedicated clusters. The total number of clusters that should be used for concurrency scaling can be set by the parameter max_concurrency_scaling_clusters.  Increasing the value of this parameter provisions additional standby clusters.

* 100+ people accessing the pipeline at once means thousands of transactions per second in request performance when uploading and retrieving storage from Amazon **S3**. Amazon S3 automatically scales to high request rates. An application can achieve 3000 to 5000 requests per second per prefix in a bucket. We can increase the number of prefixes in our bucket. One best way is to store the files in parquet format with appropriate Partition Keys.
