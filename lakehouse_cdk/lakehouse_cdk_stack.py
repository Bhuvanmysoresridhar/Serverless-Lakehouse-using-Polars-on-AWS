from aws_cdk import (
    Stack,
    aws_s3 as s3,
    aws_iam as iam,
    aws_kinesisfirehose as firehose,
    aws_lambda as lambda_,
    aws_events as events,
    aws_events_targets as targets,
    RemovalPolicy,  # ✅ Add this
    Duration,
)

from aws_cdk.aws_s3_notifications import LambdaDestination

from constructs import Construct

class LakehouseCdkStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create S3 buckets
        raw_bucket = s3.Bucket(self, "RawDataBucket",
            bucket_name="serverless-lakehouse-demo-raw",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True
        )

        bronze_bucket = s3.Bucket(self, "BronzeDataBucket",
            bucket_name="serverless-lakehouse-demo-bronze",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True
        )

        silver_bucket = s3.Bucket(self, "SilverDataBucket",
            bucket_name="serverless-lakehouse-demo-silver",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True
        )

        gold_bucket = s3.Bucket(self, "GoldDataBucket",
            bucket_name="serverless-lakehouse-demo-gold",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True
        )

        # IAM Role for Firehose to write to S3
        firehose_role = iam.Role(self, "FirehoseRole",
            assumed_by=iam.ServicePrincipal("firehose.amazonaws.com")
        )

        bronze_bucket.grant_read_write(firehose_role)

        # Firehose delivery stream
        firehose_stream = firehose.CfnDeliveryStream(self, "RawToBronzeStream",
        delivery_stream_type="DirectPut",
        extended_s3_destination_configuration=firehose.CfnDeliveryStream.ExtendedS3DestinationConfigurationProperty(
        bucket_arn=bronze_bucket.bucket_arn,
        role_arn=firehose_role.role_arn,
        buffering_hints=firehose.CfnDeliveryStream.BufferingHintsProperty(
            size_in_m_bs=1,
            interval_in_seconds=60
        ),
        compression_format="UNCOMPRESSED"
        )
    )



        # Lambda function to process Bronze to Silver
        process_lambda = lambda_.Function(self, "ProcessBronzeToSilverLambda",
            runtime=lambda_.Runtime.PYTHON_3_9,
            handler="process.handler",
            code=lambda_.Code.from_asset("lambda"),
            environment={
                "BRONZE_BUCKET": bronze_bucket.bucket_name,
                "SILVER_BUCKET": silver_bucket.bucket_name,
            }
        )

        bronze_bucket.grant_read(process_lambda)
        silver_bucket.grant_read_write(process_lambda)

        

        
        bronze_bucket.add_event_notification(
        s3.EventType.OBJECT_CREATED,
        LambdaDestination(process_lambda)
        )


        # EventBridge rule to trigger Lambda every 15 minutes (for Silver → Gold transformations)
        rule = events.Rule(self, "ScheduleGoldUpdate",
            schedule=events.Schedule.rate(Duration.minutes(15)),
        )
        rule.add_target(targets.LambdaFunction(process_lambda))
