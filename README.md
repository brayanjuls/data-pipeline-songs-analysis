# Data Pipeline for songs plays analysis

## Summary
This project involves the creation of a pipeline using airflow to bring data from a data lake allocated in S3 to a data werehouse implemented in redshift, the motivations of this data pipeline was to be able to do backfills, ensure data quality, test the data set after the etl is executed and find catch any discrepancies in the datasets. 

## Pre requisites 
To run this data pipeline you should have in you enviroment:
* Python 2.7 + 
* Airflow 1.10.x +
* AWS Redshift Cluster

## How to run
You first will need to go to the main folder of your project and run the create_table.sql file in you redshift cluster, 
after that you will need to go to airflow and create two connectors, the first one "redshift" this connector should be of type
"Postgres" with you credentials and the second one "aws_credentials" this should be of type "Amazon Web Services" and it refers to 
the aws account_key and account_secret of you user, after that you should put the plugings and dags files on your Airflow intallation home folder, then you will be able to go to the airflow ui and see the Sparkify_dag, enable it and after some runs check the data on your redshift cluster. 






