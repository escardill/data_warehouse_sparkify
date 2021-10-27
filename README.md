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

## Database shema and tables 


## ETL pipiline 
Logic to load data from S3 to staging tables on Redshift.
Logic to load data from staging tables to analytics tables on Redshift.

## SQL queries 


## RUN 
Make sure to Launch a redshift cluster and create an IAM role that has read access to S3.
Add redshift database and IAM role info to dwh.cfg.
Test by running etl.py after running create_tables.py and running the analytic queries on your Redshift database to compare your results with the expected results.
Delete your redshift cluster when finished.