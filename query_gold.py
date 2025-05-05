import duckdb

# Load extensions
duckdb.sql("LOAD httpfs")
duckdb.sql("LOAD json")
duckdb.sql("LOAD parquet")
duckdb.sql("LOAD delta")

# Query Delta table in S3 - Gold Layer
query = """
SELECT * FROM read_delta('s3://serverless-lakehouse-demo-gold/gold_table') LIMIT 10
"""
result = duckdb.sql(query).df()
print("Top 10 records from Gold Delta table:")
print(result)

# Additional Queries
total_customers = duckdb.sql("""
SELECT COUNT(DISTINCT customer_id) as total_customers FROM read_delta('s3://serverless-lakehouse-demo-gold/gold_table')
""").df()
print("\nTotal distinct customers:")
print(total_customers)

top_avg_spenders = duckdb.sql("""
SELECT customer_id, avg_order_value
FROM read_delta('s3://serverless-lakehouse-demo-gold/gold_table')
ORDER BY avg_order_value DESC
LIMIT 5
""").df()
print("\nTop 5 customers by average order value:")
print(top_avg_spenders)
