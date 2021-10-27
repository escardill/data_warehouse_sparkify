# data_warehouse_sparkify
## Description
Move user logs and songs database from sparkify from S3 to a database hosted on Redshift.
The data is loaded from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.

## Data 
The data for this project is on S3 on AWS. Configurations can be seen in the dwh.cfg file:
    [S3]
    LOG_DATA=s3://udacity-dend/log_data
    LOG_JSONPATH=s3://udacity-dend/log_json_path.json
    SONG_DATA=s3://udacity-dend/song_data

## Purpose of the database
The main goal is to load data from diferent json stores on S3 buckets to a redshift database organized with Facts and dimensions tables. That allows for an easier understanding of the data by the end user (BI, analyst team) and also the results of the queries are more eficient.

## Database Schema
The database is organized as follow:
- One fact table: the song plays events
- User dimention table with all the user information
- time dimention table 
- song dimension table with all information related to the songs, duration, etc..
- artist dimension table with all the information related to the artists

## Files of the repository
### dwh.cfg
Configuration file for cluster, S3 and IAM roles

### settings.py
Reads the AWS KEY, SECRET and database password from a .env file

### create_tables.py
Connects to database reading the configuration from dwh.cfg file.
Drop tables if they exist and the create them.

### etl.py
Connects to database reading the configuration from dwh.cfg file.
Implements the logic to load data from S3 to staging tables on Redshift.
Implements logic to load data from staging tables to analytics tables on Redshift.

### SQL queries 
Where all the queries are defined for use in the files described above.

### AWS_resources_creation
IaC to create a redshift cluster an IAM role and get the cluster end point and role ARN.

### delete_respources
IaC to delete all the above created resources

## RUN 
Make sure to Launch a redshift cluster and create an IAM role that has read access to S3.
Add redshift database and IAM role info to dwh.cfg.
Test by running etl.py after running create_tables.py and running the analytic queries on your Redshift database to compare your results with the expected results.
Delete your redshift cluster when finished.
