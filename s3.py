from io import StringIO
import os
import boto3

class S3BucketConnector:
    def __init__(self,access_key,secret_key,endpoint_url,bucket):
        self.session=boto3.Session(aws_access_key_id=os.environ[access_key],
        aws_secret_access_key=os.environ[secret_key])
        self._s3=self.session.resource(service_name='s3',endpoint_url=endpoint_url)
        self._bucket=self._s3.Bucket(bucket)
