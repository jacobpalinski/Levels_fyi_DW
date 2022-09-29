import requests
import csv
import pandas as pd
from s3 import S3BucketConnector
from io import StringIO
from datetime import datetime

class Levels_ETL:
    def __init__(self,s3_bucket=S3BucketConnector):
        self.locations={'New York':'New York','Jersey City':'New Jersey','Los Angeles':'California',
        'Irvine':'California','San Francisco':'California','Seattle':'Washington','Bellevue':'Washington',
        'Redmond':'Washington','San Jose':'California','Sunnyvale':'California','Mountain View':'California',
        'Santa Clara':'California','Palo Alto':'California','Redwood City':'California','Los Gatos':'California',
        'Cupertino':'California','Menlo Park':'California','Chicago':'Illinois','Dallas':'Texas','Miami':'Florida',
        'Philadelphia':'Pennsylvania','Pittsburgh':'Pennsylvania','Atlanta':'Georgia','Phoenix':'Arizona',
        'Boston':'Massachusetts','Cambridge':'Massachusetts','Houston':'Texas','Washington':'Distict of Columbia',
        'Arlington':'Virginia','West Mclean':'Virginia','Detroit':'Michigan','Minneapolis':'Minnesota',
        'San Diego':'California','Tampa':'Florida','Denver':'Colorado','Baltimore':'Maryland',
        'Charlotte':'North Carolina','Orlando':'Florida','San Antonio':'Texas','Portland':'Oregon',
        'Las Vegas':'Nevada','Austin':'Texas','Cincinnati':'Ohio','Kansas City':'Kansas','Columbus':'Ohio',
        'Indianapolis':'Indiana','Cleveland':'Ohio','Nashville':'Tennessee','Jacksonville':'Florida',
        'Raleigh':'North Carolina','Milwaukee':'Wisconsin','Salt Lake City':'Utah','Madison':'Wisconsin',
        'Richmond':'Virginia','Hartford':'Connecticut'}
        self.s3_bucket=s3_bucket
    
    def extract(self):
        ''' Extract data for specified locations from levels.fyi salaryData.json url to S3 staging area in CSV format'''
        job_data=requests.get('https://www.levels.fyi/js/salaryData.json').json()
        with open('job_data.csv','w',encoding='UTF-8',newline='') as file:
            writer=csv.writer(file)
            writer.writerow(['date','company','location','title','level','specialisation','gender',
            'years_of_experience','years_at_company','base_salary','stock','bonus'])
            for job in job_data:
                if self.locations.get(job['location'].split(',')[0]):
                    writer.writerow([job['timestamp'],job['company'],job['location'],job['title'],job['level'],
                    job['tag'],job['gender'],job['yearsofexperience'],job['yearsatcompany'],job['basesalary'],
                    job['stockgrantvalue'],job['bonus']])
        self.s3_bucket._bucket.upload_file(Filename=r'job_data.csv',Key='job_data.csv')
    
    def transform_job_data(self,key='job_data.csv'):
        # Create Dataframe
        job_data_df=self.s3_bucket.read_csv_to_df(key=key)
        # Convert string integers into integer datatype
        job_data_df[['years_of_experience','years_at_company',
        'base_salary','stock','bonus']]=job_data_df[['years_of_experience','years_at_company',
        'base_salary','stock','bonus']].astype(float)
        # Drop duplicates
        job_data_df.drop_duplicates(keep='first',inplace=True)
        # Drop data points with base_salary=0 and stock=0 as they are not useful for analysis
        job_data_df.drop(job_data_df[(job_data_df['base_salary']==0) & (job_data_df['stock']==0)].index,
        inplace=True)
        # Drop data points with no company and job title as they are not useful for analysis
        job_data_df.drop(job_data_df[(job_data_df['company']=='') & (job_data_df['title']=='')].index,
        inplace=True)
        # Translate base_salary,stock and bonus for all data points into figures ending in 000s
        job_data_df['base_salary']=(job_data_df['base_salary']*1000).where(job_data_df['base_salary']<10000,job_data_df['base_salary'])
        job_data_df['stock']=(job_data_df['stock']*1000).where(job_data_df['stock']<10000,job_data_df['stock'])
        job_data_df['bonus']=(job_data_df['bonus']*1000).where(job_data_df['bonus']<10000,job_data_df['bonus'])
        # Write Dataframe to S3
        self.s3_bucket.write_df_to_s3(job_data_df,'job_data.csv')
    
    def transform_dates(self,key='job_data.csv'):
        '''Transform timestamp from job_data.csv into date_csv with date_key, year, month and quarter'''
        # Create Dataframe
        job_data_df=self.s3_bucket.read_csv_to_df(key=key)
        date_df=job_data_df['date']
        # Extract date from timestamp in date column
        date_df['date']=pd.to_datetime(date_df['date'],format='%y%m%d')
        # Extract year from date
        date_df['year']=date_df['date'].dt.year
        # Extract month from date
        date_df['month']=date_df['date'].dt.month
        # Extract quarter from date
        date_df['quarter']=date_df['date'].dt.quarter
        # Write Dataframe to S3
        self.s3_bucket.write_df_to_s3(date_df,'date.csv')
    
    def transform_job_details(self,key='job_data.csv'):
        '''Create csv containing company name, title, specialisation and level associated with each data point'''
        # Create Dataframe
        job_data_df=self.s3_bucket.read_csv_to_df(key=key)
        job_details_df=job_data_df[['company','title','specialisation','level']]
        # Dealing with empty values
        job_details_df[['specialisation','level']].replace(r'^\s*$','Not Specified',
        inplace=True,regex=True)
        # Replace 'iOS','Android' and Mobile(iOS + Android) specialisation with 'Mobile Development'
        job_details_df.loc[job_details_df['specialisation'].str.contains('iOS',case=False),
        'specialisation']='Mobile Development'
        job_details_df.loc[job_details_df['specialisation'].str.contains('Android',case=False),
        'specialisation']='Mobile Development'
        # Replace all variations containing 'site reliability' into 'Site Reliability'
        job_details_df.loc[job_details_df['specialisation'].str.contains('site reliability',case=False),
        'specialisation']='Site Reliability'
        # One label for all production engineers
        job_details_df.loc[job_details_df['specialisation'].str.contains('production',case=False),
        'specialisation']='Production'
        # Replace 'fullstack' with 'Full Stack'
        job_details_df.loc[job_details_df['specialisation'].str.contains('fullstack',case=False),
        'specialisation']='Full Stack'
        # Replace 'augmented reality' and 'virtual reality' with 'AR/VR'
        job_details_df.loc[job_details_df['specialisation'].str.contains('augmented reality',case=False),
        'specialisation']='AR/VR'
        job_details_df.loc[job_details_df['specialisation'].str.contains('virtual reality',case=False),
        'specialisation']='AR/VR'
        # Replace 'front-end' with 'Front End Development'
        job_details_df.loc[job_details_df['specialisation'].str.contains('front-end',case=False),
        'specialisation']='Front-End Development'
        # Convert 'backend' into 'API Development (Back-End)'
        job_details_df.loc[job_details_df['specialisation'].str.contains('backend',case=False),
        'specialisation']='API Development (Back-End)'
        # Convert variations of algorithms and algorithm engineer into 'Algorithm'
        job_details_df.loc[job_details_df['specialisation'].str.contains('algorithms',case=False),
        'specialisation']='Algorithm'
        job_details_df.loc[job_details_df['specialisation'].str.contains('algorithm',case=False),
        'specialisation']='Algorithm'
        # Replace 'user experience','ux','ui','user interface' with 'UX/UI'
        job_details_df.loc[job_details_df['specialisation'].str.contains('user experience',case=False),
        'specialisation']='UX/UI'
        job_details_df.loc[job_details_df['specialisation'].str.contains('user interface',case=False),
        'specialisation']='UX/UI'
        job_details_df.loc[job_details_df['specialisation'].str.contains('ui',case=False),
        'specialisation']='UX/UI'
        job_details_df.loc[job_details_df['specialisation'].str.contains('ux',case=False),
        'specialisation']='UX/UI'
        # Title all column values
        job_details_df['company']=job_details_df['company'].str.title()
        job_details_df['title']=job_details_df['title'].str.title()
        job_details_df['specialisation']=job_details_df['specialisation'].str.title()
        job_details_df['level']=job_details_df['level'].str.title()
        # Write Dataframe to S3
        self.s3_bucket.write_df_to_s3(job_details_df,'job_details.csv')

    def transform_offer_recipient(self,key='job_data.csv'):
        '''Create csv containing gender, yearsofexperience and yearsatcompany associated with each data point'''
        # Create Dataframe
        job_data_df=self.s3_bucket.read_csv_to_df(key=key)
        offer_recipient_df=job_data_df[['gender','years_of_experience','years_at_company']]
        # Dealing with empty values
        offer_recipient_df[['gender','years_of_experience','years_at_company']].replace(r'^\s*$','Not Specified',
        inplace=True,regex=True)
        # Title gender values
        offer_recipient_df['gender']=offer_recipient_df['gender'].str.title()
        # Write Dataframe to S3
        self.s3_bucket.write_df_to_s3(offer_recipient_df,'offer_recipient.csv')

    def transform_locations(self,key='job_data.csv'):
        '''Create csv containing city and state information'''
        # Create Dataframe
        job_data_df=self.s3_bucket.read_csv_to_df(key=key)
        locations_df=job_data_df['location']
        # Create City Column
        locations_df['city']=locations_df['location'].split(',')[0]
        # Create State Column
        locations_df['state']=self.locations.get(locations_df['city'])
        # Drop location since longer needed
        locations_df.drop('location',axis=1,inplace=True)
        # Write Dataframe to S3
        self.s3_bucket.write_df_to_s3(locations_df,'locations.csv')












