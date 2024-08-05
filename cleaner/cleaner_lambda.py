import boto3
import os
import json

def lambda_handler(event, context):
    s3_client = boto3.client('s3')
    bucket_name = os.getenv('DESTINATION_BUCKET_NAME')

    try: 
        for record in event['Records']:
            # First parsing: Get the SQS message body, which contains the JSON string of the SNS message
            sns_message_json = json.loads(record['body'])
            # Second parsing: parse SNS messages and obtain S3 event data
            sns_message = json.loads(sns_message_json['Message'])
            # response = s3_client.list_objects_v2(Bucket=bucket_name)
            
            
            paginator = s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=bucket_name)
            
            # collect all objects which have "temp" in the key
            temp_objects = []
            for page in pages:
                if "Contents" in page:
                    temp_objects.extend([obj for obj in page['Contents'] if 'temp' in obj['Key']])
                
            if temp_objects:
                old_object_key = min(temp_objects, key=lambda x: x['LastModified'])['Key']
                s3_client.delete_object(Bucket=bucket_name, Key=old_object_key)
                print(f"Successfully deleted the oldest object {old_object_key} from {bucket_name}")
                
    except Exception as e:
        print(f"Error for deleting temporary objects with 'temp' in the name: {e}")
            
   
    return {
        'statusCode': 200,
        'body': 'No temporary objects to delete.'
    }
