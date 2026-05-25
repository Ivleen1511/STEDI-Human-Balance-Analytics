# STEDI-Human-Balance-Analytics

## Project Overview
This project builds a cloud-based data lakehouse solution for the STEDI Step Trainer system using AWS services. The goal is to process customer, accelerometer, and step trainer sensor data and prepare curated datasets for machine learning.

The project uses AWS Glue, AWS Athena, Amazon S3, Spark, and Python to ingest, clean, trnasform, and curate the data.

## AWS Services Used
- Amazon S3
- AWS Glue
- AWS Glue Studio
- AWS Athena
- Aparche Spark
- Python

## Project Workflow

### Landing Zone
Raw JSON data from:
- Customer records
- Accelerometer records
- Step trainer sensor records
were stored in Amazon S3 landing folders and queried using Athena.

### Trusted Zone
AWS Glue jobs were used to:
- Filter customers who agreed to share research data
- Filter accelerometer data belonging to trusted customers
- Filter step trainer data for curated customers

### Curated Zone
Curated datasets were created for machine learning by joining:
- Accelerometer data
- Step trainer sensor data
based on timestamps.

## Tables Created

### Landing Tables
- customer_landing
- accelerometer_landing
- step_trainer_landing

## Trusted Tables
- customer_trusted
- accelerometer_trusted
- step_trainer_trusted

## Curated Tables
- customer_curated
- machine_learning_curated

## Final Row Counts
| Table Name                 |  Row Count |
|----------------------------|------------|
| customer_landing           |        956 |
| accelerometer_landing      |      81273 |
| step_trainer_landing       |      28680 |
| customer_trusted           |        482 |
| accelerometer_trusted      |      40981 |
| step_trainer_trusted       |      14460 |
| customer_curated           |        482 |
| machine_learning_curated   |      43681 |

## Key Features
- Data ingestion using AWS Glue
- ETL pipelines using Spark and Python
- Privacy filtering based on research consent
- Curated machine learning dataset creation
- Querying using AWS Athena

## Challenges Faced
- Handling duplicate serial numbers in customer data
- Managing joins with non-unique values
- Fixing malformed parquet and schema mismatch issues
- Ensuring Glue and Athena consistency

## Conclusion
This project demonstrates an end-to-end cloud data engineering workflow using AWS services for building a scalable and privacy-aware data lakehouse solution.
