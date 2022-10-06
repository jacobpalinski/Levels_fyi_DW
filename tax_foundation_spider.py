import scrapy
import time
from scrapy import Selector
from scrapy.crawler import CrawlerProcess
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

class TaxFoundationSpider(scrapy.Spider):
    name='tax_foundation'

    def start_requests(self):
        url='https://taxfoundation.org/state-income-tax-rates-2021/#Current'
        yield scrapy.Request(url=url,callback=self.parse)

    def parse(self,response):
        # Setup selenium driver
        chrome_options=webdriver.ChromeOptions()
        chrome_options.add_argument('--headless')
        driver=webdriver.Chrome(options=chrome_options)
        
        # Get url and wait for spreadsheet to load
        driver.get('https://docs.google.com/spreadsheets/d/1RNmhCqUwrJQX85UQ3OWvP0dODtD_Xh-nWwYW0EwnXmo/pubhtml?widget=true&headers=false')
        time.sleep(5)

        # Scrape data from sheets 2021,2020,2019,2018,2017
        for tab_number in range(2,7):
            driver.find_element(By.XPATH,f'//table[@class="switcherTable"]/tbody/tr/td[{tab_number}]').click()
            time.sleep(5)
            iframe=driver.find_element(By.XPATH,'//iframe')
            driver.switch_to.frame(iframe)
            response_obj=Selector(text=driver.page_source)
            rows=response_obj.xpath('/html/body/div/div/div/table/tbody/tr[position()>=3 and position()<267]')
            for row in rows:
                yield{
                    'year': 2023-tab_number,
                    'state':row.xpath('.//td[1]/text()').get(),
                    'rate':row.xpath('.//td[2]/text()').get(),
                    'bracket':row.xpath('.//td[4]/text()').get()
                }
            driver.switch_to.default_content()

process=CrawlerProcess(settings={
    'FEED_URI':'US_State_Tax_Rates_2017-2021.csv',
    'FEED_FORMAT':'csv'
})

process.crawl(TaxFoundationSpider)
process.start()