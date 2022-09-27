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
        # Dataframe
        job_data_df=self.s3_bucket.read_csv_to_df(key=key)
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
        # Write Dataframe to S3
        self.s3_bucket.write_df_to_s3(date_df,'date.csv')
    
    def transform_job_details(self,key='job_data.csv'):
        '''Create csv containing company name, title, specialisation and level associated with each data point'''
        job_data_df=self.s3_bucket.read_csv_to_df(key=key)
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
        # Replace 'front-end' with 'Front End Development'
        job_details_df.loc[job_details_df['Specialisation'].str.contains('front-end',case=False),
        'Specialisation']='Front-End Development'
        # Convert 'backend' into 'API Development (Back-End)'
        job_details_df.loc[job_details_df['Specialisation'].str.contains('backend',case=False),
        'Specialisation']='API Development (Back-End)'
        # Convert variations of algorithms and algorithm engineer into 'Algorithm'
        job_details_df.loc[job_details_df['Specialisation'].str.contains('algorithms',case=False),
        'Specialisation']='Algorithm'
        job_details_df.loc[job_details_df['Specialisation'].str.contains('algorithm',case=False),
        'Specialisation']='Algorithm'
        # Replace 'user experience' and 'user interface' with 'UX/UI'
        job_details_df.loc[job_details_df['Specialisation'].str.contains('user experience',case=False),
        'Specialisation']='UX/UI'
        job_details_df.loc[job_details_df['Specialisation'].str.contains('user interface',case=False),
        'Specialisation']='UX/UI'
        # Title all columns
        job_details_df['Company']=job_details_df['Company'].str.title()
        job_details_df['Title']=job_details_df['Title'].str.title()
        job_details_df['Specialisation']=job_details_df['Specialisation'].str.title()
        job_details_df['Level']=job_details_df['Level'].str.title()
        # Write Dataframe to S3
        self.s3_bucket.write_df_to_s3(job_details_df,'job_details.csv')












