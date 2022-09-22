import requests
import csv
from s3 import S3BucketConnector
from io import StringIO

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
        self.s3_bucket._bucket.put_object(Body='job_data.csv',Key='job_data.csv')


