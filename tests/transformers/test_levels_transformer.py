import pytest
import os
import boto3
import csv
import pandas as pd
from io import StringIO
from moto import mock_s3
import requests
from s3 import S3BucketConnector
from levels_transformer import Levels_ETL
from locations_data_test import all_locations

@pytest.fixture
def levels_etl():
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
    s3_bucket_conn=S3BucketConnector(s3_access_key,s3_secret_key,s3_endpoint_url,s3_bucket_name)
    levels_etl=Levels_ETL(s3_bucket_conn)
    yield levels_etl
    # Teardown
    mock_bucket.stop()

def test_extract_all_locations(requests_mock,levels_etl):
    requests_mock.get('https://www.levels.fyi/js/salaryData.json',json=all_locations)
    test_data=requests.get('https://www.levels.fyi/js/salaryData.json').json()
    with open('job_data.csv','w',encoding='UTF-8',newline='') as file:
        writer=csv.writer(file)
        writer.writerow(['date','company','location','title','level','specialisation','gender',
        'years_of_experience','years_at_company','base_salary','stock','bonus'])
        for job in test_data:
            if levels_etl.locations.get(job['location'].split(',')[0]):
                writer.writerow([job['timestamp'],job['company'],job['location'],job['title'],job['level'],
                job['tag'],job['gender'],job['yearsofexperience'],job['yearsatcompany'],job['basesalary'],
                job['stockgrantvalue'],job['bonus']])
    levels_etl.s3_bucket._bucket.upload_file(Filename=r'job_data.csv',Key='job_data.csv')
    bucket_file_list=levels_etl.s3_bucket._bucket.objects.prefix('job')
    csv_jobdata=levels_etl.s3_bucket._bucket.Object(key='job_data.csv').get().get('Body').read().decode('UTF-8')
    job_data_df=pd.read_csv(StringIO(csv_jobdata))
    assert bucket_file_list[0]=='job_data.csv'
    assert job_data_df.shape==
    assert job_data_df['location'].nunique()==54
    os.remove('job_data.csv')





