import boto3
import polars as pl
import duckdb
import os
from datetime import datetime
from deltalake import DeltaTable
from deltalake.writer import write_deltalake

s3 = boto3.client('s3')

def handler(event, context):
    silver_bucket = os.environ['SILVER_BUCKET']
    gold_bucket = os.environ['GOLD_BUCKET']

    try:
        #  Read Delta table from Silver layer
        table = DeltaTable(f"s3://{silver_bucket}/silver_table/")
        full_df = pl.from_arrow(table.to_pyarrow_table())

        print("Sample of full data:")
        print(full_df.head())

        #  Aggregation: avg order value, count, total
        result = (
            full_df
            .group_by("customer_id")
            .agg([
                pl.col("order_id").count().alias("order_count"),
                pl.col("amount").sum().alias("total_spent"),
                pl.col("amount").mean().alias("avg_order_value")
            ])
        )

        print(" Aggregation complete")
        print(result.head())

        #  Write result to Gold layer as Delta
        write_deltalake(
            f"s3://{gold_bucket}/gold_table/",
            result,
            mode="overwrite"
        )

        print(" Gold layer Delta table written")

        return {
            'statusCode': 200,
            'body': 'Aggregation complete!'
        }

    except Exception as e:
        print(f" Error during aggregation: {e}")
        return {
            'statusCode': 500,
            'body': 'Aggregation failed.'
        }

