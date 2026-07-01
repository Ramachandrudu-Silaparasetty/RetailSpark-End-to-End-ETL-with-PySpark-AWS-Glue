# 🚀 Retail Sales Data Engineering Pipeline using PySpark & AWS Glue

## 📌 Project Overview

This project demonstrates an end-to-end Retail Data Engineering Pipeline built using PySpark and AWS Cloud services.

The pipeline follows the Medallion Architecture (Bronze → Silver → Gold) to ingest, clean, transform, and analyze large retail datasets.

The project was initially developed using Google Colab for local development and later migrated to AWS Glue to simulate a cloud-based ETL environment with event-driven workflow automation.

---

## 🛠️ Tech Stack

- Python
- PySpark
- Spark SQL
- Google Colab
- Amazon S3
- AWS Glue
- AWS Glue Workflow
- Amazon EventBridge
- Parquet
- Medallion Architecture

---

# Architecture

```

                    Amazon S3 (Bronze)
                           │
                           ▼
                    AWS Glue Workflow
                           │
                           ▼
                    AWS Glue Job (PySpark)
                           │
                           ▼
               Bronze → Silver → Gold
                           │
                           ▼
                 Business KPI Generation

```

---

## Dataset

The project uses three retail datasets.

- Customers
- Products
- Orders

Dataset size

- Orders : ~1 Million Records
- Customers : ~50,000 Records
- Products : ~10,000 Records

---

## Bronze Layer

Responsible for data ingestion.

Tasks performed

- Read CSV files
- Store raw data
- Preserve original data

Output

- Raw CSV files stored in Amazon S3 Bronze Layer

---

## Silver Layer

Responsible for data cleansing and validation.

Transformations performed

- Null value validation
- Duplicate removal
- Data type validation
- Referential Integrity validation
- Data quality checks

Output

- Cleaned datasets stored as Parquet files

---

## Gold Layer

Responsible for business transformations.

Generated KPIs

- Customer Sales
- Product Sales
- Category Sales
- State-wise Sales
- KPI Summary

Output

- Analytics-ready Parquet datasets

---

## AWS Implementation

The local PySpark project was migrated to AWS Glue.

Components used

- Amazon S3
- AWS Glue Job
- AWS Glue Workflow
- Amazon EventBridge

EventBridge automatically triggers the Glue Workflow whenever a new object is uploaded to the Bronze layer.

---

## Spark Features Demonstrated

- DataFrames
- Spark SQL
- Transformations
- Actions
- Group By
- Joins
- Aggregations
- Shuffle Operations
- Parquet Storage
- Spark DAG Execution
- Spark UI Analysis

---

## Project Highlights

✔ End-to-End ETL Pipeline

✔ Medallion Architecture

✔ AWS Cloud Integration

✔ Event-driven Workflow Automation

✔ Large-scale Data Processing using PySpark

✔ Business KPI Generation

---

## Future Enhancements

- Incremental Data Loading
- AWS Lambda Integration
- Glue Job Bookmarks
- Delta Lake Support
- Amazon Athena Reporting
- Amazon QuickSight Dashboard

---

## Repository Structure

```

Retail-Data-Engineering/

│

├── notebooks/

│ └── Project_Final.ipynb

│

├── src/

│ ├── bronze.py

│ ├── silver.py

│ ├── gold.py

│ ├── config.py

│ ├── utils.py

│ └── retail_etl.py

│

├── screenshots/

│

├── README.md

```

---

## Key Learning Outcomes

- Designed an end-to-end ETL pipeline
- Processed large retail datasets using PySpark
- Implemented Medallion Architecture
- Executed Spark jobs on AWS Glue
- Explored Spark UI (Jobs, Stages, Executors)
- Built an event-driven workflow using Amazon EventBridge

---

## Author

**Ramachandrudu Silaparasetty**

Aspiring Data Engineer | PySpark | SQL | AWS Glue | Hadoop | Hive | ETL
