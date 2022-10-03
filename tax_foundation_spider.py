import scrapy
import time
from scrapy import Selector
from scrapy.crawler import CrawlerProcess
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By

class TaxFoundationSpider(scrapy.Spider):
    name='tax_foundation'

    def start_requests(self):
        url='https://taxfoundation.org/state-income-tax-rates-2021/#Current'
        yield scrapy.Request(url=url,callback=self.parse)

    def parse(self,response):
        driver=webdriver.Chrome()
        options=webdriver.ChromeOptions()
        options.add_argument("headless")
        
        driver.get('https://taxfoundation.org/state-income-tax-rates-2021/#Current')
        time.sleep(5)

        for tab_number in range(2,7):
            driver.find_element(By.XPATH,f'//*[@id="sheets-viewport"]/table/tbody/tr[2]/td/div/div[1]/table/tbody/tr/td[{tab_number}]').click()
            time.sleep(5)
            response_obj=Selector(text=driver.page_source)
            rows=response_obj.xpath('//*[@id="425946206"]/div/table/tbody/tr[position()>=3]')

