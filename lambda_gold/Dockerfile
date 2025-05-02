# Use AWS Lambda Python 3.9 base image for x86_64 architecture
FROM --platform=linux/amd64 public.ecr.aws/lambda/python:3.9

# Install Polars and DuckDB
RUN pip install polars duckdb -t /var/task/

# Copy your gold transformation script
COPY silver_to_gold.py ${LAMBDA_TASK_ROOT}

# Set the Lambda handler
CMD ["silver_to_gold.handler"]

