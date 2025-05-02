import boto3
import polars as pl
import os
import io

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

            filtered_df = (
                           df.filter((pl.col("amount") > 200) &
                                    (pl.col("customer_id").is_not_null()) &
                                    (pl.col("order_date").str.len_chars() > 0)
                                )
                            )


            # Write to CSV in memory
            csv_buffer = io.StringIO()
            filtered_df.write_csv(csv_buffer)

            # Create silver key path
            silver_key = s3_key.replace("bronze/", "silver/").replace(".json", ".csv")
            s3.put_object(
                Bucket=silver_bucket,
                Key=silver_key,
                Body=csv_buffer.getvalue()
            )

            print(f"Written to {silver_bucket}/{silver_key}")

        except Exception as e:
            print(f"Error processing {s3_key}: {e}")

    return {
        'statusCode': 200,
        'body': 'Transformation complete!'
    }
