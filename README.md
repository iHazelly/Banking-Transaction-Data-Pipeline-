# Banking-Transaction-Data-Pipeline-
Welcome to my end-to-end data engineering project that simulates a real‑world banking fraud detection pipeline (Banking Transaction Data Pipeline). 

🏦 Banking Transaction Data Pipeline — AWS Production Grade
Welcome to my end-to-end data engineering project that simulates a real‑world banking fraud detection pipeline.
The pipeline ingests transaction data (PaySim dataset), performs data quality checks, transforms and partitions the data using AWS Glue (PySpark), stores it as Parquet, makes it queryable via Amazon Athena, and visualizes key metrics through a live static dashboard hosted on S3 — all automated and serverless.

https://img.shields.io/badge/AWS-Glue%2520%257C%2520Athena%2520%257C%2520S3%2520%257C%2520Step%2520Functions%2520%257C%2520EventBridge%2520%257C%2520SNS-orange
https://img.shields.io/badge/PySpark-4.0-red
https://img.shields.io/badge/Python-3.9-blue
https://img.shields.io/badge/Dashboard-ApexCharts.js-brightgreen
https://img.shields.io/badge/IaC-CloudFormation-yellow
https://img.shields.io/badge/License-MIT-lightgrey

📌 Business Problem
Banks process millions of transactions daily. Traditional batch jobs run at midnight and take hours to complete. If a file is corrupted or the job fails, no one knows until morning, leading to delayed fraud detection and potential financial loss.

This project solves that by building an event‑driven, automated pipeline that:

Triggers immediately when a new CSV file arrives in S3.

Validates data quality before processing.

Transforms and partitions data into columnar Parquet format (10x faster queries).

Logs data lineage and sends success/failure alerts.

Serves real‑time insights via an interactive dashboard that auto‑updates after every run.

