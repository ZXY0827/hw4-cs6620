import boto3
import time
import os

def upload_file_to_s3(bucket_name, file_name, content_size_kb):
    s3 = boto3.client('s3')
    content = ('x' * content_size_kb).encode() 
    s3.put_object(Bucket=bucket_name, Key=file_name, Body=content)

def main():
    bucket_name = 'mycopierlambdastack-sourceadfc1803-5mzc4a552abz'
    
    if not bucket_name:
        raise Exception("Bucket name not provided in environment variables.")
        
    files = [
        ('project.txt', 1024),
        ('temp.txt', 1024),
        ('project_new.txt', 1024),
        ('temporary_data.txt', 2560),
        ('project_new_new.txt', 1024),
        ('real_temporary_data.txt', 2048)
    ]
    
    for file_name, size in files:
        upload_file_to_s3(bucket_name, file_name, size)
        time.sleep(60)  # Sleep to make the metric graph look better

if __name__ == '__main__':
    main()
