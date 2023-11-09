import json
from aws_cdk import (
    Stack,
    aws_s3 as s3,
    aws_s3_notifications as s3n,
    aws_sns as sns,
    aws_sqs as sqs,
    aws_lambda as _lambda,
    aws_apigateway as apigateway,
    aws_iam as iam,
    Duration,
    CfnOutput,
    aws_lambda_event_sources as lambda_event_sources,
)
from constructs import Construct

class ImageUploadAndProcessingStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create resources
        raw_image_bucket, resized_image_bucket = self.create_s3_buckets()
        image_resize_topic = self.create_sns_topic()
        image_resize_queue = self.create_sqs_queue()

        # Create roles
        lambda_role = self.create_lambda_role(raw_image_bucket, resized_image_bucket, image_resize_topic, image_resize_queue)
        api_gateway_role = self.create_api_gateway_role(raw_image_bucket, resized_image_bucket, image_resize_topic)

        # Create Lambda function with the role
        lambda_handler = self.create_lambda_function(lambda_role, resized_image_bucket, image_resize_topic)

        # Set up event notifications and event sources
        self.setup_lambda_triggers(raw_image_bucket, image_resize_queue, lambda_handler)

        # Create API Gateway and configure methods
        image_upload_api = self.create_api_gateway()
        self.configure_api_methods(image_upload_api, api_gateway_role, image_resize_topic)

        # Outputs
        CfnOutput(self, "SubscribeMethodUrl",
                  value=f"{image_upload_api.url}subscribe",
                  description="URL for the subscribe method")

        CfnOutput(self, "UploadImageMethodUrl",
                  value=f"{image_upload_api.url}upload-image/{raw_image_bucket.bucket_name}/{{filename}}",
                  description="URL for the image upload method")

    def create_s3_buckets(self):
        raw_image_bucket = s3.Bucket(self, "RawImageBucket")
        resized_image_bucket = s3.Bucket(self, "ResizedImageBucket")
        return raw_image_bucket, resized_image_bucket

    def create_sns_topic(self):
        return sns.Topic(self, "ImageResizeTopic")

    def create_sqs_queue(self):
        return sqs.Queue(self, "ImageResizeQueue", visibility_timeout=Duration.seconds(300))

    def create_lambda_role(self, raw_bucket, resized_bucket, topic, queue):
        # Create the IAM role for the Lambda function
        lambda_role = iam.Role(
            self,
            "LambdaExecutionRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole"),
            ]
        )

        # Inline policy for S3 bucket access
        lambda_role.add_to_policy(iam.PolicyStatement(
            actions=["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
            resources=[
                raw_bucket.bucket_arn + "/*",
                resized_bucket.bucket_arn + "/*",
            ],
        ))

        # Inline policy for SNS publish access
        lambda_role.add_to_policy(iam.PolicyStatement(
            actions=["sns:Publish"],
            resources=[topic.topic_arn],
        ))

        # Inline policy for SQS queue access
        lambda_role.add_to_policy(iam.PolicyStatement(
            actions=[
                "sqs:ReceiveMessage",
                "sqs:DeleteMessage",
                "sqs:GetQueueAttributes",
                "sqs:ChangeMessageVisibility",
            ],
            resources=[queue.queue_arn],
        ))

        return lambda_role

    def create_lambda_function(self, role, resized_bucket, topic):
        return _lambda.Function(
            self, "ImageResizeHandler", runtime=_lambda.Runtime.PYTHON_3_7, handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset("lambda"), environment={
                "PROCESSED_BUCKET_NAME": resized_bucket.bucket_name,
                "SNS_TOPIC_ARN": topic.topic_arn,
            }, role=role
        )

    def setup_lambda_triggers(self, raw_bucket, queue, handler):
        raw_bucket.add_event_notification(s3.EventType.OBJECT_CREATED, s3n.SqsDestination(queue))
        lambda_event_source = lambda_event_sources.SqsEventSource(queue, batch_size=1)
        handler.add_event_source(lambda_event_source)
        queue.grant_consume_messages(handler)

    def create_api_gateway_role(self, raw_bucket, resized_bucket, topic):
        # Create the IAM role for the API Gateway
        api_gateway_role = iam.Role(
            self,
            "ApiGatewayServiceRole",
            assumed_by=iam.ServicePrincipal("apigateway.amazonaws.com"),
        )

        # Policy that allows API Gateway to execute PUT operations on S3
        api_gateway_role.add_to_policy(iam.PolicyStatement(
            actions=["s3:PutObject"],
            resources=[raw_bucket.bucket_arn + "/*", resized_bucket.bucket_arn + "/*"],
        ))

        # Policy that allows API Gateway to publish to an SNS topic
        api_gateway_role.add_to_policy(iam.PolicyStatement(
            actions=["sns:Publish", "sns:Subscribe"],
            resources=[topic.topic_arn],
        ))
        return api_gateway_role

    def create_api_gateway(self):
        return apigateway.RestApi(
            self, "ImageUploadApi", rest_api_name="ImageUploadService",
            binary_media_types=["image/jpeg"], description="Service for uploading and processing images."
        )

    def configure_api_methods(self, api, role, topic):
        # Configure 'subscribe' method for API Gateway
        subscribe_resource = api.root.add_resource("subscribe")
        subscribe_resource.add_method(
            "POST",
            apigateway.AwsIntegration(
                service="sns",
                integration_http_method="POST",
                action="Subscribe",
                options=apigateway.IntegrationOptions(
                    credentials_role=role,
                    request_parameters={
                        "integration.request.querystring.Endpoint": "method.request.body.email",
                        "integration.request.querystring.Protocol": "'email'",
                        "integration.request.querystring.TopicArn": f"'{topic.topic_arn}'",
                    },
                    request_templates={
                        "application/json": json.dumps(
                            {
                                "Endpoint": "$input.path('$.email')",
                                "Protocol": "email",
                                "TopicArn": topic.topic_arn,
                            }
                        )
                    },
                    integration_responses=[
                        {
                            "statusCode": "200",
                            "response_templates": {
                                "application/json": '{"Message":"Subscription request has been sent."}'
                            },
                        }
                    ],
                ),
            ),
            method_responses=[{"statusCode": "200"}],
        )

        # Configure 'upload-image' method for API Gateway
        upload_image_resource = api.root.add_resource("upload-image")
        upload_image_resource = upload_image_resource.add_resource("{bucket}")
        upload_image_resource = upload_image_resource.add_resource("{filename}")

        s3_integration = apigateway.AwsIntegration(
            service="s3",
            integration_http_method="PUT",
            path="{bucket}/{filename}",
            options=apigateway.IntegrationOptions(
                credentials_role=role,
                request_parameters={
                    "integration.request.path.bucket": "method.request.path.bucket",
                    "integration.request.path.filename": "method.request.path.filename",
                },
                integration_responses=[
                    {
                        "statusCode": "200",
                        "response_templates": {
                            "application/json": '{"Message": "Image was uploaded successfully. Make sure to subscribe to receive the image link."}'
                        },
                    }
                ],
                passthrough_behavior=apigateway.PassthroughBehavior.WHEN_NO_MATCH,
            ),
        )

        upload_image_resource.add_method(
            "PUT",
            s3_integration,
            request_parameters={
                "method.request.path.bucket": True,
                "method.request.path.filename": True,
            },
            request_models={"image/jpeg": apigateway.Model.EMPTY_MODEL},
            method_responses=[{"statusCode": "200"}],
        )

