import json
import boto3
import pandas as pd
import io
import os

s3 = boto3.client('s3')

def handler(event, context):
    bronze_bucket = os.environ['BRONZE_BUCKET']
    silver_bucket = os.environ['SILVER_BUCKET']

    # Loop over all S3 records in the event
    for record in event['Records']:
        s3_object_key = record['s3']['object']['key']
        print(f"Processing {s3_object_key}")

        # Get the object from Bronze bucket
        obj = s3.get_object(Bucket=bronze_bucket, Key=s3_object_key)
        data = obj['Body'].read()

        # Assume each line in the file is a JSON
        json_lines = data.decode('utf-8').splitlines()
        records = [json.loads(line) for line in json_lines]

        # Load into Pandas DataFrame
        df = pd.DataFrame(records)
        print("Original DataFrame:")
        print(df.head())

        # 🛠 Your transformation logic (example)
        # Filter high-value orders > 200
        df = df[df['amount'] > 200]

        # Save back as CSV into Silver bucket
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)

        # Upload to Silver
        silver_key = s3_object_key.replace('bronze/', 'silver/').replace('.json', '.csv')

        s3.put_object(
            Bucket=silver_bucket,
            Key=silver_key,
            Body=csv_buffer.getvalue()
        )
        print(f"Written to {silver_bucket}/{silver_key}")

    return {
        'statusCode': 200,
        'body': json.dumps('Transformation complete!')
    }
