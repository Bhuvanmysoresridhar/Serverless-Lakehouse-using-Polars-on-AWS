import aws_cdk as core
import aws_cdk.assertions as assertions

from lakehouse_cdk.lakehouse_cdk_stack import LakehouseCdkStack

# example tests. To run these tests, uncomment this file along with the example
# resource in lakehouse_cdk/lakehouse_cdk_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = LakehouseCdkStack(app, "lakehouse-cdk")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
