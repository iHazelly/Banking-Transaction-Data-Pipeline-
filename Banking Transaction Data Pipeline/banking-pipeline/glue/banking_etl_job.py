import sys
import boto3
import json
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import *

# ============================================================
# INITIALIZE
# ============================================================
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'INPUT_PATH',
    'OUTPUT_PATH', 
    'LINEAGE_PATH',
    'SNS_TOPIC_ARN'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

sns = boto3.client('sns', region_name='us-east-1')
s3 = boto3.client('s3', region_name='us-east-1')

run_id = f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
start_time = datetime.now()

print(f"{'='*60}")
print(f"Job: {args['JOB_NAME']}")
print(f"Run ID: {run_id}")
print(f"Start: {start_time.isoformat()}")
print(f"Input: {args['INPUT_PATH']}")
print(f"Output: {args['OUTPUT_PATH']}")
print(f"{'='*60}")

# ============================================================
# STEP 1: EXTRACT
# ============================================================
print("\n[STEP 1] EXTRACT — Reading raw data from S3...")

df_raw = spark.read.csv(
    args['INPUT_PATH'],
    header=True,
    inferSchema=True
)

raw_count = df_raw.count()
print(f"Raw row count: {raw_count:,}")
print(f"Columns: {df_raw.columns}")

# ============================================================
# STEP 2: DATA QUALITY CHECK
# ============================================================
print("\n[STEP 2] DATA QUALITY — Running validation checks...")

dq_results = {}
dq_passed = True

# ---- CHECK 1: Null values in all columns ----
print("  Checking null values...")
null_counts = {}
for col in df_raw.columns:
    n = df_raw.filter(F.col(col).isNull()).count()
    null_counts[col] = n
    if n > 0:
        print(f"  WARNING: {col} has {n:,} nulls ({n/raw_count*100:.2f}%)")

critical_nulls = {k: v for k, v in null_counts.items() 
                  if k in ['step', 'type', 'amount', 'nameOrig', 'nameDest'] and v > 0}
if critical_nulls:
    dq_passed = False
    print(f"  CRITICAL nulls found in: {list(critical_nulls.keys())}")
else:
    print("  No critical nulls")

dq_results['null_checks'] = null_counts

# ---- CHECK 2: Amount must be > 0 ----
print("  Checking amount validity...")
negative_amount = df_raw.filter(F.col('amount') <= 0).count()
zero_balance_fraud = df_raw.filter(
    (F.col('isFraud') == 1) & 
    (F.col('oldbalanceOrg') == 0) & 
    (F.col('amount') > 0)
).count()
dq_results['negative_amount'] = negative_amount
dq_results['zero_balance_fraud'] = zero_balance_fraud
print(f"  Amount <= 0: {negative_amount:,} rows")
print(f"  Fraud with zero origin balance: {zero_balance_fraud:,} rows")

# ---- CHECK 3: Valid transaction types ----
print("  Checking transaction types...")
valid_types = ['TRANSFER', 'CASH_OUT', 'PAYMENT', 'DEBIT', 'CASH_IN']
invalid_types_count = df_raw.filter(~F.col('type').isin(valid_types)).count()
dq_results['invalid_type_count'] = invalid_types_count
if invalid_types_count > 0:
    dq_passed = False
    print(f"  Invalid types: {invalid_types_count:,} rows")
else:
    print("  All transaction types valid")

# ---- CHECK 4: isFraud binary ----
print("  Checking fraud flag...")
invalid_fraud = df_raw.filter(~F.col('isFraud').isin([0, 1])).count()
dq_results['invalid_fraud_flag'] = invalid_fraud
print(f"  Invalid fraud flags: {invalid_fraud}")

# ---- CHECK 5: Balance consistency ----
print("  Checking balance consistency...")
balance_inconsistent = df_raw.filter(
    (F.col('type').isin(['TRANSFER', 'CASH_OUT'])) &
    (F.col('oldbalanceOrg') > 0) &
    (F.col('newbalanceOrig') > F.col('oldbalanceOrg'))
).count()
dq_results['balance_inconsistent'] = balance_inconsistent
print(f"  Balance inconsistencies: {balance_inconsistent:,}")

# ---- DQ SUMMARY ----
fraud_count_raw = df_raw.filter(F.col('isFraud') == 1).count()
dq_summary = {
    'run_id': run_id,
    'timestamp': start_time.isoformat(),
    'input_path': args['INPUT_PATH'],
    'raw_count': raw_count,
    'fraud_count': fraud_count_raw,
    'fraud_rate': round(fraud_count_raw / raw_count * 100, 4),
    'dq_passed': dq_passed,
    'checks': dq_results
}
print(f"\n  DQ Status: {'PASSED' if dq_passed else 'WARNING'}")
print(f"  Fraud rate: {dq_summary['fraud_rate']}%")

# ============================================================
# STEP 3: TRANSFORM
# ============================================================
print("\n[STEP 3] TRANSFORM — Cleaning and enriching data...")

df_clean = df_raw \
    .filter(F.col('amount') > 0) \
    .filter(F.col('type').isin(valid_types)) \
    .filter(F.col('isFraud').isin([0, 1])) \
    .withColumn('amount', F.col('amount').cast(DoubleType())) \
    .withColumn('step', F.col('step').cast(IntegerType())) \
    .withColumn('isFraud', F.col('isFraud').cast(IntegerType())) \
    .withColumn('isFlaggedFraud', F.col('isFlaggedFraud').cast(IntegerType())) \
    .withColumn('oldbalanceOrg', F.col('oldbalanceOrg').cast(DoubleType())) \
    .withColumn('newbalanceOrig', F.col('newbalanceOrig').cast(DoubleType())) \
    .withColumn('oldbalanceDest', F.col('oldbalanceDest').cast(DoubleType())) \
    .withColumn('newbalanceDest', F.col('newbalanceDest').cast(DoubleType()))

# ---- ENRICHMENT: Add derived columns ----
df_enriched = df_clean \
    .withColumn('account_type_orig',
        F.when(F.col('nameOrig').startswith('C'), 'customer')
         .when(F.col('nameOrig').startswith('M'), 'merchant')
         .otherwise('unknown')) \
    .withColumn('account_type_dest',
        F.when(F.col('nameDest').startswith('C'), 'customer')
         .when(F.col('nameDest').startswith('M'), 'merchant')
         .otherwise('unknown')) \
    .withColumn('balance_diff_orig',
        F.round(F.col('newbalanceOrig') - F.col('oldbalanceOrg'), 2)) \
    .withColumn('balance_diff_dest',
        F.round(F.col('newbalanceDest') - F.col('oldbalanceDest'), 2)) \
    .withColumn('amount_vs_balance_ratio',
        F.when(F.col('oldbalanceOrg') > 0,
               F.round(F.col('amount') / F.col('oldbalanceOrg'), 4))
         .otherwise(F.lit(None))) \
    .withColumn('is_high_value',
        F.when(F.col('amount') >= 200000, 1).otherwise(0)) \
    .withColumn('is_zero_balance_after',
        F.when(F.col('newbalanceOrig') == 0, 1).otherwise(0)) \
    .withColumn('is_suspicious',
        F.when(
            (F.col('isFraud') == 1) |
            ((F.col('amount') >= 200000) & (F.col('type') == 'TRANSFER')) |
            ((F.col('newbalanceOrig') == 0) & (F.col('oldbalanceOrg') > 0) & (F.col('type') == 'TRANSFER')),
            1
        ).otherwise(0)) \
    .withColumn('day_of_simulation',
        (F.col('step') / 24).cast(IntegerType())) \
    .withColumn('hour_of_day', F.col('step') % 24) \
    .withColumn('processed_at', F.lit(datetime.now().isoformat())) \
    .withColumn('run_id', F.lit(run_id)) \
    .withColumn('year', F.lit(datetime.now().year)) \
    .withColumn('month', F.lit(datetime.now().month)) \
    .withColumn('day', F.lit(datetime.now().day))

clean_count = df_enriched.count()
removed_count = raw_count - clean_count
print(f"Clean row count: {clean_count:,}")
print(f"Removed (invalid): {removed_count:,}")
print(f"New columns added: account_type_orig, account_type_dest, balance_diff, "
      f"amount_vs_balance_ratio, is_high_value, is_suspicious, day_of_simulation, hour_of_day")

# ============================================================
# STEP 4: LOAD — Partitioned Parquet
# ============================================================
print("\n[STEP 4] LOAD — Writing partitioned Parquet to S3...")
print(f"Output path: {args['OUTPUT_PATH']}")
print(f"Partitions: year / month / day / type")

df_enriched.write \
    .mode('append') \
    .partitionBy('year', 'month', 'day', 'type') \
    .option('compression', 'snappy') \
    .parquet(args['OUTPUT_PATH'])

print(f"Data written successfully!")
print(f"   Format: Parquet + Snappy compression")
print(f"   Partitioning: year={datetime.now().year}/month={datetime.now().month}/day={datetime.now().day}/type=<TYPE>")

# ============================================================
# STEP 5: DATA LINEAGE LOGGING
# ============================================================
print("\n[STEP 5] LINEAGE — Logging data lineage...")

end_time = datetime.now()
duration_seconds = (end_time - start_time).seconds

lineage_record = {
    'run_id': run_id,
    'job_name': args['JOB_NAME'],
    'status': 'SUCCESS',
    'source': {
        'path': args['INPUT_PATH'],
        'format': 'CSV',
        'row_count': raw_count
    },
    'transformations': [
        'filter: amount > 0',
        'filter: valid transaction types',
        'filter: valid fraud flags',
        'enrich: account_type classification',
        'enrich: balance_diff calculation',
        'enrich: is_high_value flag',
        'enrich: is_suspicious flag',
        'enrich: time features (day_of_simulation, hour_of_day)',
        'partition: year/month/day/type'
    ],
    'target': {
        'path': args['OUTPUT_PATH'],
        'format': 'Parquet',
        'compression': 'Snappy',
        'row_count': clean_count,
        'partitioned_by': ['year', 'month', 'day', 'type']
    },
    'data_quality': dq_summary,
    'metrics': {
        'input_rows': raw_count,
        'output_rows': clean_count,
        'removed_rows': removed_count,
        'fraud_rows': fraud_count_raw,
        'duration_seconds': duration_seconds,
        'records_per_second': round(raw_count / duration_seconds) if duration_seconds > 0 else 0
    },
    'timestamps': {
        'started_at': start_time.isoformat(),
        'completed_at': end_time.isoformat(),
        'duration_seconds': duration_seconds
    }
}

# Save lineage to S3
lineage_key = f"glue/{datetime.now().strftime('%Y/%m/%d')}/{run_id}.json"
lineage_bucket = args['LINEAGE_PATH'].replace('s3://', '').split('/')[0]

s3.put_object(
    Bucket=lineage_bucket,
    Key=lineage_key,
    Body=json.dumps(lineage_record, indent=2),
    ContentType='application/json'
)
print(f"Lineage saved: s3://{lineage_bucket}/{lineage_key}")

# ============================================================
# STEP 6: NOTIFY SUCCESS
# ============================================================
message = f"""
Banking ETL Pipeline SUCCESS

Run ID: {run_id}
Duration: {duration_seconds}s
Input: {raw_count:,} rows
Output: {clean_count:,} rows (Parquet)
Fraud Cases: {fraud_count_raw:,} ({dq_summary['fraud_rate']}%)
DQ Status: {'PASSED' if dq_passed else 'WARNING'}

Output: {args['OUTPUT_PATH']}
"""

sns.publish(
    TopicArn=args['SNS_TOPIC_ARN'],
    Subject='Banking Pipeline SUCCESS',
    Message=message
)
print("Success notification sent!")

print(f"\n{'='*60}")
print(f"JOB COMPLETED: {run_id}")
print(f"Duration: {duration_seconds}s")
print(f"{'='*60}")

# ============================================================
# STEP 7: EXPORT SUMMARY JSON FOR DASHBOARD
# ============================================================
print("\n[STEP 7] Exporting summary JSON for dashboard...")

# Compute summary statistics
total_transactions = df_enriched.count()
total_fraud = df_enriched.filter(F.col('isFraud') == 1).count()
total_volume = df_enriched.agg(F.sum('amount')).collect()[0][0]
fraud_volume = df_enriched.filter(F.col('isFraud') == 1).agg(F.sum('amount')).collect()[0][0]

fraud_rate = (total_fraud / total_transactions) * 100 if total_transactions > 0 else 0

# Aggregate by transaction type
by_type = df_enriched.groupBy('type').agg(
    F.count('*').alias('cnt'),
    F.sum(F.when(F.col('isFraud') == 1, 1).otherwise(0)).alias('fraud'),
    F.round(F.sum('amount'), 2).alias('total_amount')
).collect()

by_type_list = []
for row in by_type:
    by_type_list.append({
        'type': row['type'],
        'cnt': row['cnt'],
        'fraud': row['fraud'],
        'total_amount': float(row['total_amount'])
    })

# Hourly fraud pattern (optional but included)
hourly_fraud = df_enriched.groupBy('hour_of_day').agg(
    F.count('*').alias('cnt'),
    F.sum(F.when(F.col('isFraud') == 1, 1).otherwise(0)).alias('fraud')
).orderBy('hour_of_day').collect()

hourly_list = []
for row in hourly_fraud:
    hourly_list.append({
        'hour': row['hour_of_day'],
        'cnt': row['cnt'],
        'fraud': row['fraud']
    })

# Build JSON structure
summary_json = {
    'summary': {
        'total_transactions': total_transactions,
        'total_fraud': total_fraud,
        'fraud_rate': round(fraud_rate, 2),
        'total_volume_usd': float(total_volume) if total_volume else 0,
        'fraud_volume_usd': float(fraud_volume) if fraud_volume else 0
    },
    'by_type': by_type_list,
    'hourly_fraud': hourly_list,
    'last_updated': datetime.now().isoformat()
}

# Upload JSON to dashboard bucket
dashboard_bucket = "banking-dashboard-hazell-bank-transaction-2026"
json_key = "data/summary.json"

json_body = json.dumps(summary_json, indent=2)

s3.put_object(
    Bucket=dashboard_bucket,
    Key=json_key,
    Body=json_body,
    ContentType='application/json'
)
print(f"Summary JSON uploaded to s3://{dashboard_bucket}/{json_key}")

job.commit()