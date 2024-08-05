import boto3
import os
import logging
import json


def log_handler(event, context):
    s3_client = boto3.client('s3')
    sqs_client = boto3.client('sqs')
    destination_bucket = os.getenv('DESTINATION_BUCKET_NAME')
    
    if not destination_bucket:
        raise ValueError("DESTINATION_BUCKET_NAME environment variable is not set.")

    
    # Loop through each message sent by SNS
    for record in event['Records']:
        # First parsing: Get the SQS message body, which contains the JSON string of the SNS message
        sns_message_json = json.loads(record['body'])
        # # Second parsing: parse SNS messages and obtain S3 event data
        # sns_message = json.loads(sns_message_json['Message'])

    try:
        total_size = 0
        paginator = s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=destination_bucket)

        for page in page_iterator:
            if 'Contents' in page:
                for obj in page['Contents']:
                    if "temp" in obj['Key']:
                        total_size += obj['Size']
                        # logger.debug(f"Found temp object {obj['Key']} with size {obj['Size']}")

        print(f"Total size of all objects in {destination_bucket}: {total_size} bytes")
        
        receipt_handle = record['receiptHandle']
        sqs_client.delete_message(
            QueueUrl = os.environ['LOG_QUEUE_URL'],
            ReceiptHandle = receipt_handle 
        )
    except Exception as e:
        print(f"Error accessing objects in {destination_bucket}: {str(e)}")
        raise

    return {"statusCode": 200, "body": "Log processing completed successfully."}
