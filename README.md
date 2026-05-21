# 🏦 Banking Transaction Data Pipeline — AWS Production Grade

![AWS](https://img.shields.io/badge/AWS-Glue%20%7C%20Athena%20%7C%20S3%20%7C%20Step%20Functions%20%7C%20EventBridge%20%7C%20SNS-orange)
![PySpark](https://img.shields.io/badge/PySpark-4.0-red)
![Python](https://img.shields.io/badge/Python-3.9-blue)
![Dashboard](https://img.shields.io/badge/Dashboard-ApexCharts.js-brightgreen)
![IaC](https://img.shields.io/badge/IaC-CloudFormation-yellow)
![License](https://img.shields.io/badge/License-MIT-lightgrey)

An **event‑driven, end‑to‑end data pipeline** that simulates a real‑world banking fraud detection system.  
It ingests transaction data (PaySim), performs data quality checks, transforms and partitions data using **AWS Glue (PySpark)**, stores it as **Parquet**, makes it queryable via **Amazon Athena**, and visualizes key metrics through an **interactive static dashboard** – all automated and serverless.

---

## 📌 Business Problem

Banks process millions of transactions daily. Traditional batch jobs run at midnight and take hours. If a file is corrupted or the job fails, no one knows until morning → delayed fraud detection, financial loss.

**This project solves that by building an automated pipeline that:**
- Triggers **immediately** when a new CSV file arrives in S3.
- **Validates data quality** before processing.
- **Transforms and partitions** data into columnar Parquet format (10x faster queries).
- **Logs data lineage** and sends **success/failure alerts** via email.
- **Serves live insights** via an interactive dashboard that auto‑updates after every run.

---

## 🏗️ Architecture Overview

![Architecture Diagram](docs/architecture.png)

*High‑level architecture – event‑driven pipeline using AWS services.*

| Layer | Service | Role |
|-------|---------|------|
| **Ingestion** | S3 | Raw CSV files land in `/incoming/` |
| **Orchestration** | EventBridge → Step Functions | Triggers pipeline on file upload, manages retries |
| **Schema Detection** | Glue Crawler | Automatically infers schema of raw data |
| **Transformation** | Glue ETL (PySpark) | Data quality checks, cleansing, enrichment, partitioning, Parquet conversion |
| **Storage** | S3 | Partitioned Parquet files (`year/month/day/type`) |
| **Query** | Amazon Athena | Serverless SQL analytics on Parquet |
| **Monitoring** | CloudWatch + SNS | Logging, metrics, email alerts on success/failure |
| **Visualization** | S3 Static Website (ApexCharts) | Interactive dashboard with auto‑refresh |

> **Cost**: Fully serverless – runs under **$0.50 per month** on AWS Free Tier / Learner Lab credit
