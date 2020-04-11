# Cloud Data Warehouse using AWS RedShift
> 
This project uses Amazon Web Services S3 and Redshift to make the ETL pipeline that extracts data from S3, stages it in Redshift, and then transforms data into a set of dimensional and fact tables for analytics which provide insights songs that users are listening to. The data for this project is for a fictional startup called Sparkify. 

## Table of contents

* [Data and Code](#data-and-code)
* [Prerequisites](#prerequisites)
* [Instructions on running the application](#instructions-on-running-the-application)

## Data and Code
The dataset for the project resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in Sparkify app.

In addition to the data files, the project workspace includes four files:
* **create_tables.py** - drops and creates database tables. This file is run on the command line to reset tables prior to running the ETL script.
* **etl.py** - runs the ETL pipeline to extract data from S3 and stages it in Redshift, and then transforms it into a set of analytics tables.
* **sql_queries.py** - contains all the projects sql queries, and is imported into the etl and create_tables scripts.
* **dwh.cfg** - configuration file for specifying required variables for the RedShift cluster access as well as the S3 JSON data buckets.

## Prerequisites
* AWS RedShift cluster with a schema called dwh
* configparser
* psycopg2
python 3 is needed to run the python scripts.

## Database structure
![ERD image](/songplays_erd.png)
## Instructions on running the application
* You must have an access to an AWS RedShift cluster
* Run the create_tables.py script to create database tables.
* Run the etl.py script to execute the ETL pipeline.
