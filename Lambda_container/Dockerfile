FROM public.ecr.aws/lambda/python:3.9

# Install dependencies into the Lambda root
RUN pip install polars boto3 deltalake -t /var/task/
 
# Copy your Lambda function
COPY process.py ${LAMBDA_TASK_ROOT}

# Command to run the Lambda function
CMD ["process.handler"]

