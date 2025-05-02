import json
import boto3
import pandas as pd
from io import BytesIO
import os

def handler(event, context):
    s3 = boto3.client('s3')

    bronze_bucket = os.environ['BRONZE_BUCKET']
    silver_bucket = os.environ['SILVER_BUCKET']

    response = s3.list_objects_v2(Bucket=bronze_bucket, Prefix='part')
    for obj in response.get('Contents', []):
        key = obj['Key']
        data = s3.get_object(Bucket=bronze_bucket, Key=key)['Body'].read()
        df = pd.read_parquet(BytesIO(data))

        # Example transformation: filter amount > 100
        df = df[df['amount'] > 100]

        buffer = BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)

        silver_key = key.replace('bronze', 'silver')
        s3.upload_fileobj(buffer, silver_bucket, silver_key)
