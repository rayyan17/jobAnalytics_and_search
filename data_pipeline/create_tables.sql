DROP TABLE IF EXISTS jobs;
DROP TABLE IF EXISTS time_details;
DROP TABLE IF EXISTS company_location;
DROP TABLE IF EXISTS job_rating;
DROP TABLE IF EXISTS job_sector;
DROP TABLE IF EXISTS job_salary;
DROP TABLE IF EXISTS developers;



CREATE TABLE IF NOT EXISTS jobs (
	job_title varchar (500) NOT NULL,
	job_description varchar,
	company varchar,
	location varchar,
	source varchar(256),
	source_fetch_date TIMESTAMP
);


CREATE TABLE IF NOT EXISTS time_details (
	source_fetch_date TIMESTAMP NOT NULL,
	source varchar(256) NOT NULL,
	source_year int NOT NULL,
	source_month int NOT NULL,
	source_day int NOT NULL
);


CREATE TABLE IF NOT EXISTS company_location (
	company varchar NOT NULL,
	location varchar,
	city varchar(256),
	state varchar(256),
	country varchar(256)
);


CREATE TABLE IF NOT EXISTS job_rating (
	job_title varchar (500) NOT NULL,
	company varchar,
	rating int,
	max_rating int
);


CREATE TABLE IF NOT EXISTS job_sector (
	job_title varchar (500) NOT NULL,
	sector varchar
);



CREATE TABLE IF NOT EXISTS job_salary (
	job_title varchar (500) NOT NULL,
	company varchar,
	estimated_salary varchar (256)
);


CREATE TABLE IF NOT EXISTS developers (
	person_id int NOT NULL,
	hobby varchar(256),
	open_source_contrib varchar(256),
	country varchar(256),
	student varchar(256),
	employment varchar(256),
	main_education varchar(256),
	development_area varchar,
	latest_job varchar,
	productive_hours varchar,
	gender varchar(256),
	age varchar(256)
);
