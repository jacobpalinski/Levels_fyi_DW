import pytest
import os
import boto3
import csv
import pandas as pd
from io import StringIO
from moto import mock_s3
from s3 import S3BucketConnector

@pytest.fixture
def s3_bucket():
    # Mocking S3 connection start
    mock_bucket=mock_s3()
    mock_bucket.start()
    # Defining Class Arguments
    s3_access_key='AWS_ACCESS_KEY_ID'
    s3_secret_key='AWS_SECRET_ACCESS_KEY'
    s3_endpoint_url='https://s3.us-east-2.amazonaws.com'
    s3_bucket_name='test-bucket'
    # Creating s3 access keys as environment variables
    os.environ[s3_access_key]='KEY1'
    os.environ[s3_secret_key]='KEY2'
    s3=boto3.resource(service_name='s3',endpoint_url=s3_endpoint_url)
    s3.create_bucket(Bucket=s3_bucket_name, CreateBucketConfiguration={'LocationConstraint':'us-east-2'})
    # Creating Test instance
    s3_bucket=S3BucketConnector(s3_access_key,s3_secret_key,s3_endpoint_url,s3_bucket_name)
    yield s3_bucket
    # Teardown
    mock_bucket.stop()

class TestS3BucketConnector:
    def test_read_csv_to_df(self,s3_bucket):
        # Expected Results
        key_exp='test.csv'
        col1_exp='col1'
        col2_exp='col2'
        val1_exp='val1'
        val2_exp='val2'
        # Test Init
        csv_content=f'{col1_exp},{col2_exp}\n{val1_exp},{val2_exp}'
        s3_bucket._bucket.put_object(Body=csv_content,Key=key_exp)
        # Method Execution
        df_result=s3_bucket.read_csv_to_df(key_exp)
        # Test after execution
        assert df_result.shape==(1,2)
        assert val1_exp==df_result[col1_exp][0]
        assert val2_exp==df_result[col2_exp][0]
    
    def test_write_df_to_s3(self,s3_bucket):
        # Expected Results
        df_exp=pd.DataFrame([['A','B'],['C','D']],columns=['col1','col2'])
        key_exp='test.csv'
        # Method Execution 
        result=s3_bucket.write_df_to_s3(df_exp,key_exp)
        # Test after execution
        df_result=s3_bucket.read_csv_to_df(key_exp)
        assert result==True
        assert df_result.equals(df_exp)==True


