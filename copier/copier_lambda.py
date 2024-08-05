import boto3
import json
import os
import urllib.parse

def copier_handler(event, context):
    s3_client = boto3.client('s3')
    destination_bucket = os.environ['DESTINATION_BUCKET_NAME']
    sqs_client = boto3.client('sqs')
    log_queue_url = os.getenv('LOG_QUEUE_URL')

    # Loop through each message sent by SNS
    for record in event['Records']:
        # First parsing: Get the SQS message body, which contains the JSON string of the SNS message
        sns_message_json = json.loads(record['body'])
        # Second parsing: parse SNS messages and obtain S3 event data
        sns_message = json.loads(sns_message_json['Message'])

        source_bucket = sns_message['Records'][0]['s3']['bucket']['name']
        source_key = urllib.parse.unquote_plus(sns_message['Records'][0]['s3']['object']['key'], encoding='utf-8')
        destination_key = f"{source_key}"
        
        # print(f"Attempting to copy from {source_bucket}/{source_key} to {destination_bucket}/{destination_key}")
        
        try:
            response = s3_client.head_object(Bucket=source_bucket, Key=source_key)
            size = response['ContentLength']
            s3_client.copy_object(
                Bucket=destination_bucket, 
                CopySource={'Bucket': source_bucket, 'Key': source_key}, 
                Key=destination_key
            )
            print(f"Successfully copied {source_key} from {source_bucket} to {destination_bucket}.")
            
            # Send a message to logQueue
            message = {"info": "File copied", "bucket": destination_bucket}
            sqs_client.send_message(QueueUrl=log_queue_url, MessageBody=json.dumps(message))
            
            receipt_handle = record['receiptHandle']
            sqs_client.delete_message(
                QueueUrl = os.environ['COPIER_QUEUE_URL'],
                ReceiptHandle = receipt_handle 
            )
            
        except Exception as e:
            print(f"Failed to copy object: {str(e)}")
            raise e