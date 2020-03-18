# airflow-app
 This is a simple project so I can learn how Airflow, Docker, and AWS (S3, DynamoDB) work together.
 
 Airflow handles the ingestion of the data from a NYC Open Data dataset endpoint.
 MySQL is be the database for Airflow keeping track of all runs.
 Docker containerizes Airflow and MySQL for scalability.
 AWS S3 is a staging location for the data before it gets imported into DynamoDB
 DynamoDB is the main database for querying from the UI.

## Airflow
#### Custom Plugins
SODA plugin is a custom airflow plugin that has a custom operator "SodaToS3Operator" to pull data for a certain dataset from the Socrata Open Data API and write it as a json file on S3.
