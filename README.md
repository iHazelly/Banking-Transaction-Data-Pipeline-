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

---

## 🛠️ Technologies & Services Used

| Category | Tools / Services |
|----------|------------------|
| **Cloud** | AWS (S3, Glue, Athena, Step Functions, EventBridge, SNS, CloudWatch, IAM) |
| **Data Processing** | PySpark (AWS Glue 4.0), Parquet, Snappy compression |
| **Query Engine** | Amazon Athena (Presto) |
| **Orchestration** | Step Functions, EventBridge |
| **Monitoring & Alerting** | CloudWatch, SNS (email) |
| **Visualization** | HTML, Tailwind CSS, ApexCharts.js |
| **Infrastructure as Code** | AWS CloudFormation |
| **Version Control** | Git, GitHub |

---

## 📊 Dataset

- **Source**: [PaySim Financial Dataset](https://www.kaggle.com/datasets/ealaxi/paysim1) (Kaggle)
- **Rows**: 6.3M (sample used: 10k for testing)
- **Key columns**: `step` (time), `type` (TRANSFER, CASH_OUT, etc.), `amount`, `nameOrig`, `nameDest`, `oldbalanceOrg`, `newbalanceOrig`, `isFraud`
- **Fraud distribution**: Only `TRANSFER` and `CASH_OUT` contain fraudulent transactions.

---

## 🧠 What the Pipeline Does (Step‑by‑Step)

1. **File Upload** → CSV file dropped into `s3://banking-raw-.../incoming/`
2. **EventBridge** → Detects S3 `PutObject` event and triggers Step Functions.
3. **Step Functions** → Orchestrates:  
   - Run Glue Crawler (schema update)  
   - Run Glue ETL job (transform & load)  
   - Run `MSCK REPAIR TABLE` in Athena  
   - Send success/failure email via SNS
4. **Glue ETL (PySpark)** does:
   - Data quality checks (nulls, amount>0, valid types, balance consistency)
   - Enrichment (account type, suspicious flags, time features)
   - Write data as **Parquet** partitioned by `year/month/day/type`
   - Log **data lineage** (JSON) to S3 lineage bucket
   - Export **summary JSON** to dashboard bucket (`data/summary.json`)
5. **Athena** → External table on the Parquet location, ready for SQL queries.
6. **Static Dashboard** (S3 website) → Fetches `summary.json` and renders:
   - KPI cards (total transactions, fraud cases, fraud rate, total volume)
   - Bar chart (fraud by transaction type)
   - Pie chart (transaction volume share)
   - Sparklines for trends
   - Last updated timestamp

---

## 🚀 Deployment Instructions (CloudFormation)

### Prerequisites
- AWS account with permissions (or AWS Learner Lab).
- AWS CLI installed (optional, you can use Console).

### Steps

**1. Clone the repository**

```bash
git clone https://github.com/yourusername/banking-pipeline.git
cd banking-pipeline
```

**2. Upload the Glue script to S3**

```bash
aws s3 cp glue/banking_etl_job.py s3://banking-raw-<your-suffix>/scripts/
```

**3. Deploy the CloudFormation stack**

- Go to AWS Console → CloudFormation → Create stack → Upload a template file → select `cloudformation/banking-pipeline-iac.yaml`
- Provide parameters:
  - `EnvironmentName`: `dev`
  - `BucketSuffix`: a unique suffix (e.g., `yourname-2026`)
  - `EmailAddress`: your email (to receive alerts)
  - `GlueScriptS3Key`: `scripts/banking_etl_job.py`
- Acknowledge IAM capabilities → Create stack
- Wait for `CREATE_COMPLETE` (~2-3 minutes)

**4. Upload test data**

Upload `dataset/sample_10k.csv` to your raw bucket:

```bash
aws s3 cp dataset/sample_10k.csv s3://banking-raw-<your-suffix>/incoming/
```

**5. Monitor the pipeline**

- Step Functions Console → watch execution graph
- Glue Console → Jobs → view run logs
- Check email for success/failure notification

**6. Enable Dashboard**

- Go to S3 Console → `banking-dashboard-<your-suffix>` bucket
- Properties → Static website hosting → Enable → Index document: `index.html`
- Upload `dashboard/index.html` to the bucket root
- Open the website URL — dashboard shows live data once Glue job produces `data/summary.json`

---
