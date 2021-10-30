# data_warehouse_sparkify

The purpose of this project is to build an ETL pipeline that will be able to extract song data from an S3 bucket and transform that data to make it suitable for analysis. This data can be used with business intelligence and visualization apps that will help the analytics team to better understand what songs are commonly listened to on the app.

## Data 
The data for this project is on S3 on AWS. Configurations can be seen in the dwh.cfg file:
    [S3]
    LOG_DATA=s3://udacity-dend/log_data
    LOG_JSONPATH=s3://udacity-dend/log_json_path.json
    SONG_DATA=s3://udacity-dend/song_data
    
The datasets used are retrieved from the s3 bucket and are in the JSON format. There are two datasets namely 'log_data' and 'song_data'. The song_data dataset is a subset of the the Million Song Dataset while the 'log_data' contains generated log files based on the songs in 'song_data'.

## Purpose of the database
The main goal is to load data from diferent json stores on S3 buckets to a redshift database organized with Facts and dimensions tables. That allows for an easier understanding of the data by the end user (BI, analyst team) and also the results of the queries are more eficient.

## The final Database Schema
The database is organized as a star schema with tables as follow:
- One fact table: the song plays events
- User dimention table with all the user information
- time dimention table 
- song dimension table with all information related to the songs, duration, etc..
- artist dimension table with all the information related to the artists

The star schema as opposed to 3rd normal is more suitable and optimized for OLAP operations which will be the purpose of the database.

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
### Prerequisites:
- Make sure to Launch a redshift cluster and create an IAM role that has read access to S3.
- Set AWS access and secret keys
- 
- Security group with inbound rules appropriately set as below:
> Type: Custom TCP Rule.
> Protocol: TCP.
> Port Range: 5439,
> Source: Custom IP, with 0.0.0.0/0

### Requirements
- configparser
- boto3
- psycog2

### run
Add redshift database and IAM role info to dwh.cfg.
Test by running etl.py after running create_tables.py and running the analytic queries on your Redshift database to compare your results with the expected results.
Delete your redshift cluster when finished.
