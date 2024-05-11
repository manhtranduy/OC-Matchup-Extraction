import requests
import pandas as pd
from datetime import datetime
import os
import re
from typing import List, Union
from support_functions import get_datetime_from_S3
import numpy as np

class S3_SciCopernicus_Search:
    def __init__(self, Input: Union[str, List[str]], 
                 start_date:str='2016-02-16',  
                 end_date:str=datetime.now().strftime('%Y-%m-%d'), 
                 credential_file:str=os.getcwd()+'/credentials_copernicus.txt', 
                 processingLevel:int=1, 
                 producttype:str='OL_1_EFR___', 
                 platformname:str='Sentinel-3', 
                 satellite:str='S3',
                 cloudcoverpercentage:int=100,
                 user:str='', 
                 password:str='',
                 number_of_results:int=1000):
        self.Input = Input
        self.start_date = start_date
        self.end_date = end_date
        self.credential_file = credential_file
        self.processingLevel = processingLevel
        self.producttype = producttype
        self.platformname = platformname
        self.satellite = satellite
        self.cloudcoverpercentage = cloudcoverpercentage
        self.user = user
        self.password = password
        self.number_of_results = number_of_results

        if not self.user or not self.password:
            self.user, self.password = self.handle_credentials(self.credential_file)

        self.product_summary = pd.DataFrame()

        self.rootURL = 'https://scihub.copernicus.eu/dhus/search?format=json&q='

        if isinstance(self.Input, list):
            if len(self.Input) > 2:
                if self.Input[0] > self.Input[1]:
                    raise ValueError('Error: Please enter in a valid input longitude')
                elif self.Input[2] > self.Input[3]:
                    raise ValueError('Error: Please enter in a valid input latitude')
                
                self.search_input = 'footprint:"Intersects(POLYGON(({} {}, {} {}, {} {}, {} {}, {} {})))"'.format(
                    self.Input[2], self.Input[0], self.Input[2], self.Input[1], self.Input[3], self.Input[1], self.Input[3], self.Input[0], self.Input[2], self.Input[0]
                )
            elif len(self.Input) == 2:
                self.search_input = 'footprint:"Intersects({}, {})"'.format(self.Input[1], self.Input[0])
        elif isinstance(self.Input, str):
            self.search_input = self.Input

        if self.processingLevel == 1:
            self.producttype = 'OL_1_EFR___'
        elif self.processingLevel == 2:
            self.producttype = 'OL_2_LFR___'

        self.search_producttype = 'producttype:{}'.format(self.producttype)
        self.search_platform = 'platformname:{}'.format(self.platformname)
        self.search_satellite = 'filename:{}*'.format(self.satellite)

        self.search()

    def handle_credentials(self, credential_file):
        with open(credential_file, 'r') as f:
            return f.read().strip().split(',')

    def search(self):
        # Convert dates to datetime objects
        start_date = datetime.strptime(self.start_date, '%Y-%m-%d')
        end_date = datetime.strptime(self.end_date, '%Y-%m-%d')

        # Create a range of dates between start_date and end_date
        time_range = pd.date_range(start_date, end_date)

        # Get unique years and months
        unique_years = time_range.year.unique()
        unique_months = time_range.month.unique()

        # For each unique year
        for year in unique_years:
            # For each unique month in a specific year
            for month in unique_months:
                # Convert back to datetime format
                s_date = time_range[(time_range.year == year) & (time_range.month == month)].min().strftime('%Y-%m-%d')
                e_date = time_range[(time_range.year == year) & (time_range.month == month)].max().strftime('%Y-%m-%d')

                self.search_time_range(s_date, e_date)


    def search_time_range(self, s_date, e_date):
        search_timerange = 'beginPosition:[{}T00:00:00.000Z TO {}T23:59:59.999Z] AND endPosition:[{}T00:00:00.000Z TO {}T23:59:59.999Z]'.format(s_date, e_date, s_date, e_date)
        URL = '{} {} AND {} AND {} AND {} AND {} &rows=100&start=0'.format(self.rootURL, 
                                                                           self.search_input, 
                                                                           self.search_platform, 
                                                                           search_timerange, self.search_producttype, self.search_satellite)
        
        try:
            response = requests.get(URL, auth=(self.user, self.password))
            response.raise_for_status()
            Data = response.json()
        except Exception as e:
            print('Failed to read URL: {}'.format(str(e)))
            return

        total_results=int(Data['feed']['opensearch:totalResults'])
        
        if total_results==0:
            print('No product is available from {} to {}'.format(s_date, e_date))
            return
        elif total_results ==1:
            product=[Data['feed']['entry']]
        else:
            product=Data['feed']['entry']
            
        print('{} products found from {} to {}'.format(total_results, s_date, e_date))
        
        for prod in product:
            self.product_summary = pd.concat([self.product_summary, self.product_info(prod)], ignore_index=True)

    def product_info(self, product):
        product_summary = {}
        product_summary['product_list'] = product['title']
        size_str = re.search('Size: (.*? [M|G])', product['summary']).group(1)
        if 'M' in size_str:
            product_summary['product_size'] = float(size_str.replace('M', ''))
        elif 'G' in size_str:
            product_summary['product_size'] = float(size_str.replace('G', '')) * 1024
        product_summary['product_url'] = product['link'][0]['href']
        product_summary['satellite_datetime'],_ = get_datetime_from_S3(product_summary['product_list'])
        
        return pd.DataFrame([product_summary])

# S3_search = S3_SciCopernicus_Search(Input=[105,10], start_date='2021-01-01', end_date='2021-01-31')
# product_list=S3_search.product_summary

           
