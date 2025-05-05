import duckdb

# Load DuckDB extensions
duckdb.sql("INSTALL httpfs; LOAD httpfs")
duckdb.sql("INSTALL json; LOAD json")
duckdb.sql("INSTALL parquet; LOAD parquet")
duckdb.sql("INSTALL delta; LOAD delta")


# Main Query: Show 10 records from the Silver Delta table
query = """
SELECT * FROM read_delta('s3://serverless-lakehouse-demo-silver/silver_table') LIMIT 10
"""
result = duckdb.sql(query).df()
print("First 10 Records:")
print(result)

# 1. Count total records
count_query = """
SELECT COUNT(*) AS total_records FROM read_delta('s3://serverless-lakehouse-demo-silver/silver_table')
"""
count_result = duckdb.sql(count_query).df()
print("\n Total Records:")
print(count_result)

# 2. Number of orders per customer
group_query = """
SELECT customer_id, COUNT(*) AS order_count
FROM read_delta('s3://serverless-lakehouse-demo-silver/silver_table')
GROUP BY customer_id
ORDER BY order_count DESC
LIMIT 5
"""
group_result = duckdb.sql(group_query).df()
print("\n Top 5 Customers by Order Count:")
print(group_result)

# 3. Average amount per customer
avg_query = """
SELECT customer_id, AVG(amount) AS avg_order_amount
FROM read_delta('s3://serverless-lakehouse-demo-silver/silver_table')
GROUP BY customer_id
ORDER BY avg_order_amount DESC
LIMIT 5
"""
avg_result = duckdb.sql(avg_query).df()
print("\n Top 5 Customers by Average Order Value:")
print(avg_result)
