import requests
import csv
import pandas as pd
from s3 import S3BucketConnector
from io import StringIO
from datetime import datetime

class Levels_ETL:
    def __init__(self,s3_bucket=S3BucketConnector):
        self.locations={'New York','Jersey City','Los Angeles','Irvine','San Francisco','Seattle','Bellevue',
        'Redmond','San Jose','Sunnyvale','Mountain View','Santa Clara','Palo Alto','Redwood City','Los Gatos',
        'Cupertino','Chicago','Dallas','Miami','Philadelphia','Pittsburgh','Atlanta','Phoenix','Boston','Cambridge',
        'Houston','Washington','Arlington','West Mclean','Detroit','Minneapolis','San Diego','Tampa','Denver',
        'Baltimore','Charlotte','Orlando','San Antonio','Portland','Pittsburgh','Las Vegas','Austin','Cincinnati',
        'Kansas City','Columbus','Indianapolis','Cleveland','Nashville','Jacksonville','Raleigh','Milwaukee',
        'Salt Lake City','Madison','Richmond','Hartford'}
        self.s3_bucket=s3_bucket
    
    def extract(self):
        ''' Extract data for specified locations from levels.fyi salaryData.json url to S3 staging area in CSV format'''
        job_data=requests.get('https://www.levels.fyi/js/salaryData.json').json()
        with open('job_data.csv','w',encoding='UTF-8',newline='') as file:
            writer=csv.writer(file)
            writer.writerow(['Date','Company','Job Title','Level', 'Tag','Gender','Years of Experience',
            'Years at Company','Base Salary', 'Stock', 'Bonus'])
            for job in job_data:
                if job['location'].split(',')[0] in self.locations:
                    writer.writerow([job['timestamp'],job['company'],job['title'],job['level'],job['tag'],
                    job['gender'],job['location'],job['yearsofexperience'],job['yearsatcompany'],job['basesalary'],
                    job['stockgrantvalue'],job['bonus']])
        self.s3_bucket._bucket.upload_file(Filename=r'job_data.csv',Key='job_data.csv')
    
    def transform_dates(self,key='job_data.csv'):
        '''Transform timestamp from job_data.csv into date_csv with date_key, year, month and quarter'''
        csv_jobdata=self.s3_bucket._bucket.Object(key).get().get('Body').read.decode('UTF-8')
        job_data_df=pd.read_csv(StringIO(csv_jobdata))
        # Extract date from timestamp
        date_df=job_data_df['timestamp']
        date_df['Date']=pd.to_datetime(date_df['timestamp'],format='%y%m%d')
        # Extract year from date
        date_df['Year']=date_df['Date'].dt.year
        # Extract month from date
        date_df['Month']=date_df['Date'].dt.month
        # Extract quarter from date
        date_df['Quarter']=date_df['Date'].dt.quarter
        # Drop timestamp since longer needed
        date_df.drop('timestamp',axis=1,inplace=True)
        out_buffer=StringIO()
        date_df.to_csv(out_buffer,index=False)
        self.s3_bucket._bucket.put_object(Body=out_buffer.getvalue(),Key='date.csv')
    
    def transform_job_details(self,key='job_data.csv'):
        '''Create csv containing company name, title, specialisation and level associated with each data point'''
        csv_jobdata=self.s3_bucket._bucket.Object(key).get().get('Body').read.decode('UTF-8')
        job_data_df=pd.read_csv(StringIO(csv_jobdata))
        job_details_df=job_data_df[['company','title','tag','level']]
        # Dealing with empty values
        job_details_df['company','title','tag','level']=job_details_df[['company','title','tag','level']].replace(
        r'^\s*$','Not Specified',regex=True)
        # Rename Columns
        job_details_df.rename(columns={'company':'Company','title':'Title','tag':'Specialisation','level':'Level'})
        # Replace 'iOS','Android' and Mobile(iOS + Android) specialisation with 'Mobile Development'
        job_details_df.loc[job_details_df['Specialisation'].str.contains('iOS',case=False),
        'Specialisation']='Mobile Development'
        job_details_df.loc[job_details_df['Specialisation'].str.contains('Android',case=False),
        'Specialisation']='Mobile Development'
        # Replace all variations containing 'site reliability' into 'Site Reliability'
        job_details_df.loc[job_details_df['Specialisation'].str.contains('site reliability',case=False),
        'Specialisation']='Site Reliability'
        # One label for all production engineers
        job_details_df.loc[job_details_df['Specialisation'].str.contains('production',case=False),
        'Specialisation']='Production'
        # Replace 'fullstack' with 'Full Stack'
        job_details_df.loc[job_details_df['Specialisation'].str.contains('fullstack',case=False),
        'Specialisation']='Full Stack'
        # Replace 'augmented reality' and 'virtual reality' with 'AR/VR'
        job_details_df.loc[job_details_df['Specialisation'].str.contains('augmented reality',case=False),
        'Specialisation']='AR/VR'
        job_details_df.loc[job_details_df['Specialisation'].str.contains('virtual reality',case=False),
        'Specialisation']='AR/VR'
        # Replace 'Web Development (front-end) with 'Front End Development'
        # Replace 'user experience' and 'user interface' with 'UX/UI'
        job_details_df.loc[job_details_df['Specialisation'].str.contains('user experience',case=False),
        'Specialisation']='UX/UI'
        job_details_df.loc[job_details_df['Specialisation'].str.contains('user interface',case=False),
        'Specialisation']='UX/UI'











