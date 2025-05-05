import boto3
import polars as pl
import os
import io
from deltalake.writer import write_deltalake

s3 = boto3.client('s3')

def handler(event, context):
    bronze_bucket = os.environ['BRONZE_BUCKET']
    silver_bucket = os.environ['SILVER_BUCKET']

    for record in event.get('Records', []):
        s3_key = record['s3']['object']['key']
        print(f"Processing {s3_key}")

        try:
            response = s3.get_object(Bucket=bronze_bucket, Key=s3_key)
            data = response['Body'].read()

            # Read JSON lines into Polars
            df = pl.read_ndjson(io.BytesIO(data))

            print("Original Data:")
            print(df.head())

            # Apply data quality filters
            filtered_df = df.filter(
                (pl.col("amount") > 200) &
                (pl.col("customer_id").is_not_null()) &
                (pl.col("order_date").str.len_chars() > 0)
            )

            print("Filtered DataFrame:")
            print(filtered_df.head())


            #  Write Delta to Silver S3 path
            write_deltalake(
                f"s3://{silver_bucket}/silver_table/",
                filtered_df,
                mode="append"
            )

            print("Delta write complete for Silver layer")

        except Exception as e:
            print(f"Error processing {s3_key}: {e}")

    return {
        'statusCode': 200,
        'body': 'Transformation complete!'
    }
