from io import StringIO
import os
import csv
import boto3
import pandas as pd

class S3BucketConnector:
    def __init__(self,access_key,secret_key,endpoint_url,bucket):
        self.session=boto3.Session(aws_access_key_id=os.environ[access_key],
        aws_secret_access_key=os.environ[secret_key])
        self._s3=self.session.resource(service_name='s3',endpoint_url=endpoint_url)
        self._bucket=self._s3.Bucket(bucket)
    
    def read_csv_to_df(self,key):
        csv_obj=self._bucket.Object(key).get().get('Body').read.decode('UTF-8')
        dataframe=pd.read_csv(StringIO(csv_obj))
        return dataframe

    def write_df_to_s3(self,dataframe,key,out_buffer=StringIO()):
        dataframe.to_csv(out_buffer,index=False)
        self._bucket.put_object(Body=out_buffer.getvalue(),Key=key)
        return True
