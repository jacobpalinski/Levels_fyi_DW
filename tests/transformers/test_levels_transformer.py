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

@pytest.fixture
def levels_etl_with_test_csv_data(tmpdir_factory,levels_etl):
    filename=str(tmpdir_factory.mktemp('data').join('test_data.csv'))
    with open(filename,'w',encoding='UTF-8',newline='') as file:
        writer=csv.writer(file)
        writer.writerow(['date','company','location','title','level','specialisation','gender',
        'years_of_experience','years_at_company','base_salary','stock','bonus'])
        writer.writerows([['1/1/2017 11:33:27','Google','Sunnyvale, CA','Software Engineer','L3','android',
        'male','1','0','120000','40000','15000'],
        ['4/20/2017 11:33:27','Apple','Austin, TX','Software Engineer','ICT2','iOS Development','female','1','0',
        '90','30','20'],
        ['4/20/2017 11:33:27','Microsoft','Bellevue, WA','Product Manager','59','UX/UI','Male','0','0','0','0','0'],
        ['7/15/2017 11:33:27','Hubspot','Cambridge, MA, United States','Software Engineer','Junior',
        'Site Reliability (SRE)','','','','135','5','0'],
        ['10/11/2017 11:33:27','Facebook','Menlo Park, CA','Software Engineer','E5','production','male',
        '11','2','215','100','40'],
        ['10/11/2017 11:33:27','Facebook','Menlo Park, CA','Software Engineer','E5','production','male',
        '11','2','215','100','40'],
        ['12/11/2017 11:33:27','spotify','New York, NY','Software Engineer','Engineer 1','fullstack developer','male',
        '4','0','180','37.5','0'],
        ['1/30/2018 11:33:27','Intel','Santa Clara, CA','Software Engineer','grade 9','augmented reality','male',
        '20','5','204','50','20'],
        ['1/30/2018 11:33:27','Intel','Santa Clara, CA','Software Engineer','grade 9','virtual reality','male',
        '20','5','204','50','20'],
        ['3/30/2018 11:33:27','Netflix','Denver, CO','Software Engineer','E5','Web Development (front-end)','male',
        '20','2','591','0','0'],
        ['4/7/2018 11:33:27','Sony Interactive Entertainment','San Francisco, CA','Software Engineer','L4',
        'backend tools','male','6','6','103','5','32'],
        ['5/9/2018 11:33:27','Lyft','New York, NY','Data Scientist','t6','algorithms','male',
        '6','3','200','200','0'],
        ['11/11/2018 11:33:27','Hudson River Trading','New York, NY','Software Engineer','L4',
        'algorithm','male','6','4','431','0','1700'],
        ['4/7/2019 11:33:27','Facebook','Chicago, IL','Product Designer','IC4',
        'user experience','female','7','0','143','40','22.7'],
        ['4/7/2019 11:33:27','Facebook','New York, NY','Product Designer','IC4',
        'ux','female','7','2','173','40','0'],
        ['4/7/2019 11:33:27','Mango Voice','Salt Lake City, UT','Product Designer','l3',
        'ui','female','5','3','74.5','0','0'],
        ['9/13/2020 11:33:27','No Salary Startup','Chicago, IL','Product Designer','',
        'user interface','female','0','0','0','100','0'],
        ['4/7/2021 11:33:27','','Chicago, IL','','IC4','user experience','female','7','0','143','40','22.7'],
        ['4/7/2021 11:33:27','twitter','Washington, DC','software engineer','swe II',
        'data','male','2','2','150','60','0']])
    levels_etl.s3_bucket._bucket.upload_file(Filename=filename,Key='test_data.csv')
    yield levels_etl

class Test_Levels_ETL:
    def test_extract_all_locations(self,tmpdir_factory,requests_mock,levels_etl):
        requests_mock.get('https://www.levels.fyi/js/salaryData.json',json=all_locations)
        test_data=requests.get('https://www.levels.fyi/js/salaryData.json').json()
        filename=str(tmpdir_factory.mktemp('data').join('job_data.csv'))
        with open(filename,'w',encoding='UTF-8',newline='') as file:
            writer=csv.writer(file)
            writer.writerow(['date','company','location','title','level','specialisation','gender',
            'years_of_experience','years_at_company','base_salary','stock','bonus'])
            for job in test_data:
                if levels_etl.locations.get(job['location'].split(',')[0]):
                    writer.writerow([job['timestamp'],job['company'],job['location'],job['title'],job['level'],
                    job['tag'],job['gender'],job['yearsofexperience'],job['yearsatcompany'],job['basesalary'],
                    job['stockgrantvalue'],job['bonus']])
        levels_etl.s3_bucket._bucket.upload_file(Filename=filename,Key='job_data.csv')
        bucket_file_list=[obj.key for obj in levels_etl.s3_bucket._bucket.objects.filter(Prefix='job')]
        jobdata_csv=levels_etl.s3_bucket._bucket.Object(key='job_data.csv').get().get('Body').read().decode('UTF-8')
        job_data_df=pd.read_csv(StringIO(jobdata_csv))
        print(bucket_file_list)
        assert bucket_file_list[0]=='job_data.csv'
        assert job_data_df.shape==(56,12)
        assert job_data_df['location'].nunique()==55

def test_transform_job_data(levels_etl_with_test_csv_data):
    key_exp='test_data.csv'
    levels_etl_with_test_csv_data.transform_job_data(key=key_exp)
    jobdata_csv=levels_etl_with_test_csv_data.s3_bucket._bucket.Object(key='job_data.csv').get().get('Body').read().decode('UTF-8')
    job_data_df=pd.read_csv(StringIO(jobdata_csv))
    assert list(job_data_df.select_dtypes(include=['float']).columns)==['years_of_experience','years_at_company',
    'base_salary','stock','bonus']
    assert job_data_df.duplicated().any()==False
    assert ((job_data_df['base_salary']==0) & (job_data_df['stock']==0)).any()==False
    assert ((job_data_df['company']=='') & (job_data_df['title']=='')).any()==False
    assert job_data_df[job_data_df['company']=='Google']['base_salary'].values[0]==120000.00
    assert job_data_df[job_data_df['company']=='Google']['stock'].values[0]==40000.00
    assert job_data_df[job_data_df['company']=='Google']['bonus'].values[0]==15000.00
    assert job_data_df[job_data_df['company']=='Apple']['base_salary'].values[0]==90000.00
    assert job_data_df[job_data_df['company']=='Apple']['stock'].values[0]==30000.00
    assert job_data_df[job_data_df['company']=='Apple']['bonus'].values[0]==20000.00

def test_transform_dates(levels_etl_with_test_csv_data):
    key_exp='test_data.csv'
    levels_etl_with_test_csv_data.transform_dates(key=key_exp)
    date_csv=levels_etl_with_test_csv_data.s3_bucket._bucket.Object(key='date.csv').get().get('Body').read().decode('UTF-8')
    date_df=pd.read_csv(StringIO(date_csv))
    assert list(date_df.columns)==['date','year','month','quarter']
    assert date_df['date'].tolist()==['2017-01-01','2017-04-20','2017-04-20','2017-07-15',
    '2017-10-11','2017-10-11','2017-12-11','2018-01-30','2018-01-30','2018-03-30','2018-04-07','2018-05-09',
    '2018-11-11','2019-04-07','2019-04-07','2019-04-07','2020-09-13','2021-04-07','2021-04-07']
    assert date_df['year'].tolist()==[2017,2017,2017,2017,2017,2017,2017,2018,2018,2018,2018,2018,2018,
    2019,2019,2019,2020,2021,2021]
    assert date_df['month'].tolist()==[1,4,4,7,10,10,12,1,1,3,4,5,11,4,4,4,9,4,4]
    assert date_df['quarter'].tolist()==[1,2,2,3,4,4,4,1,1,1,2,2,4,2,2,2,3,2,2]

def test_transform_job_details(levels_etl_with_test_csv_data):
    key_exp='test_data.csv'
    levels_etl_with_test_csv_data.transform_job_details(key=key_exp)
    job_details_csv=levels_etl_with_test_csv_data.s3_bucket._bucket.Object(key='job_details.csv').get().get('Body').read().decode('UTF-8')
    job_details_df=pd.read_csv(StringIO(job_details_csv))
    print(job_details_df[['company','level','specialisation']])
    assert job_details_df.isnull().values.any()==False
    assert job_details_df['specialisation'].str.contains('iOS').all()==False
    assert job_details_df['specialisation'].str.contains('android').all()==False
    assert job_details_df['specialisation'].str.contains('Site Reliability (SRE)').all()==False
    assert job_details_df['specialisation'].str.contains('production').all()==False
    assert job_details_df['specialisation'].str.contains('fullstack').all()==False
    assert job_details_df['specialisation'].str.contains('augmented reality').all()==False
    assert job_details_df['specialisation'].str.contains('virtual reality').all()==False
    assert job_details_df['specialisation'].str.contains('Web Development (front-end)').all()==False
    assert job_details_df['specialisation'].str.contains('backend').all()==False
    assert job_details_df['specialisation'].str.contains('algorithm').all()==False
    assert job_details_df['specialisation'].str.contains('algorithms').all()==False
    assert job_details_df['specialisation'].str.contains('user experience').all()==False
    assert job_details_df['specialisation'].str.contains('user interface').all()==False
    assert job_details_df['specialisation'].str.contains('ui').all()==False
    assert job_details_df['specialisation'].str.contains('ux').all()==False
    assert job_details_df['company'].str.istitle().all()==True
    assert job_details_df['title'].str.istitle().all()==True
    assert job_details_df['specialisation'].str.istitle().all()==True

def test_transform_offer_recipient(levels_etl_with_test_csv_data):
    key_exp='test_data.csv'
    levels_etl_with_test_csv_data.transform_offer_recipient(key=key_exp)
    offer_recipient_csv=levels_etl_with_test_csv_data.s3_bucket._bucket.Object(key='offer_recipient.csv').get().get('Body').read().decode('UTF-8')
    offer_recipient_df=pd.read_csv(StringIO(offer_recipient_csv))
    assert offer_recipient_df.isnull().values.any()==False
    assert offer_recipient_df['gender'].str.istitle().all()==True

def test_transform_locations(levels_etl_with_test_csv_data):
    key_exp='test_data.csv'
    levels_etl_with_test_csv_data.transform_locations(key=key_exp)
    locations_csv=levels_etl_with_test_csv_data.s3_bucket._bucket.Object(key='locations.csv').get().get('Body').read().decode('UTF-8')
    locations_df=pd.read_csv(StringIO(locations_csv))
    assert list(locations_df.columns)==['city','state']
    assert locations_df['city'].tolist()==['Sunnyvale','Austin','Bellevue','Cambridge',
    'Menlo Park','Menlo Park','New York','Santa Clara','Santa Clara','Denver','San Francisco','New York',
    'New York','Chicago','New York','Salt Lake City','Chicago','Chicago','Washington']
    assert locations_df['state'].tolist()==['California','Texas','Washington','Massachusetts','California',
    'California','New York','California','California','Colorado','California','New York','New York',
    'Illinois','New York','Utah','Illinois','Illinois','District of Columbia']












