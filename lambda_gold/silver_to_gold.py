import boto3
import polars as pl
import duckdb
import os
import io
from datetime import datetime

s3 = boto3.client('s3')

def handler(event, context):
    silver_bucket = os.environ['SILVER_BUCKET']
    gold_bucket = os.environ['GOLD_BUCKET']

    # List all files in silver bucket (you can filter to .csv or .parquet if needed)
    response = s3.list_objects_v2(Bucket=silver_bucket)
    files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.csv')]

    if not files:
        print("No files found in silver bucket.")
        return {'statusCode': 200, 'body': 'No data to process'}

    # Load all CSV files from Silver into Polars
    dfs = []
    for key in files:
        print(f"Reading {key}...")
        obj = s3.get_object(Bucket=silver_bucket, Key=key)
        df = df = pl.read_csv(obj['Body'])
        print("DEBUG: DataFrame type is", type(df))
        dfs.append(df)

    # Combine into one DataFrame
    full_df = pl.concat(dfs)

    print("Sample of full data:")
    print(full_df.head())

    # Let's calculate average order value per customer
    print("DEBUG: Starting groupby aggregation...")
    result = (
        full_df
        .group_by("customer_id")
        .agg([
            pl.col("order_id").count().alias("order_count"),
            pl.col("amount").sum().alias("total_spent"),
            pl.col("amount").mean().alias("avg_order_value")
        ])
    )
    




    print("DEBUG: Transformed DataFrame (result):")
    print(result.head())

    # Convert to CSV string
    csv_buffer = io.StringIO()
    result.write_csv(csv_buffer)
    print("DEBUG: CSV buffer created")

    # Define output key with timestamp
    timestamp = datetime.utcnow().strftime("%Y/%m/%d/%H/")
    key = f"{timestamp}gold_agg.csv"
    print(f"DEBUG: Target S3 key is {key}")

    # Write to Gold bucket
    s3.put_object(
    Bucket=gold_bucket,
    Key=key,
    Body=csv_buffer.getvalue()
    )
    print(f"✅ Written to s3://{gold_bucket}/{key}")

    return {
        'statusCode': 200,
        'body': 'Aggregation complete!'
    }

