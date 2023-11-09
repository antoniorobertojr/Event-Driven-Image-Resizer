import json
import boto3
from PIL import Image
from io import BytesIO
import os
import traceback  # For detailed error logging

# Initialize S3 and SNS clients
s3_client = boto3.client('s3')
sns_client = boto3.client('sns')

PROCESSED_BUCKET_NAME = os.getenv('PROCESSED_BUCKET_NAME')
SNS_TOPIC_ARN = os.getenv('SNS_TOPIC_ARN')

# Function to generate a presigned URL
def generate_presigned_url(bucket_name, object_key, expiration=3600):
    try:
        response = s3_client.generate_presigned_url('get_object',
                                                    Params={'Bucket': bucket_name, 'Key': object_key},
                                                    ExpiresIn=expiration)
    except Exception as e:
        print(e)
        return None
    return response

def lambda_handler(event, context):
    print(event)
    # Extract the S3 event data
    for record in event['Records']:
        message_body = json.loads(record['body'])
        s3_info = message_body['Records'][0]['s3']
        source_bucket_name = s3_info['bucket']['name']
        file_key = s3_info['object']['key']

        # Avoid potential infinite loops with assertion to ensure source is not destination
        assert PROCESSED_BUCKET_NAME != source_bucket_name, "Source and destination buckets must be different to avoid recursion."

        try:
            # Debugging: Confirm the key is a string
            if not isinstance(file_key, str):
                raise ValueError(f"file_key must be a string, got {type(file_key)} instead.")

            # Download the image from S3
            s3_file = s3_client.get_object(Bucket=source_bucket_name, Key=file_key)
            s3_file_content = s3_file['Body'].read()

            # Debugging: Check the type of the content
            if not isinstance(s3_file_content, bytes):
                raise TypeError(f"Expected s3_file_content to be bytes, got {type(s3_file_content)} instead.")

            # Process the image
            with Image.open(BytesIO(s3_file_content)) as image:
                # Resize the image
                width, height = image.size
                image = image.resize((int(width / 2), int(height / 2)), Image.ANTIALIAS)
                buffer = BytesIO()
                image.save(buffer, 'JPEG')
                buffer.seek(0)

            # Upload the processed image to S3
            processed_file_key = 'processed-' + file_key
            s3_client.put_object(Bucket=PROCESSED_BUCKET_NAME, Key=processed_file_key, Body=buffer.getvalue())

            # Generate a presigned URL
            presigned_url = generate_presigned_url(PROCESSED_BUCKET_NAME, processed_file_key)
            if not presigned_url:
                raise ValueError("Failed to generate a presigned URL.")

            # Publish a message to the SNS topic
            sns_message = {
                'default': 'Your image has been processed successfully.',
                'email': f'Your image has been processed and is available at the following link: {presigned_url}'
            }
            sns_response = sns_client.publish(
                TopicArn=SNS_TOPIC_ARN,
                Message=json.dumps(sns_message),
                MessageStructure='json'
            )

            s3_client.delete_object(Bucket=source_bucket_name, Key=file_key)

            print(f"Processed and removed {file_key} from {source_bucket_name}.")
            print(sns_response)

        except AssertionError as error:
            # Specific catch for our recursion guard assertion
            print(error)
        except Exception as e:
            print(f"Error processing {file_key} from bucket {source_bucket_name}: {e}")
            traceback.print_exc()  # Print the traceback to help debug the issue
            continue

    return {
        'statusCode': 200,
        'body': json.dumps('Image processing completed for all valid records.')
    }

