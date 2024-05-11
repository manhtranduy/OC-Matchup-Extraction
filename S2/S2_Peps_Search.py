import requests
import pandas as pd
from datetime import datetime, timedelta
import os
from typing import List, Union
from support_functions import get_datetime_from_S2

class S2_Peps_Search:
    def __init__(self, Input: Union[str, List[str]], 
                 start_date:str='2016-02-16', 
                 end_date:str=datetime.now().strftime('%Y-%m-%d'), 
                 credential_file:str=os.getcwd()+'/credentials_peps.txt', 
                 processingLevel:int=1, 
                 cloudcoverpercentage:int=60, 
                 satellite:str='', 
                 user:str='', 
                 password:str='',
                 maxRecords:int=500):
        self.Input = Input
        self.start_date = datetime.strptime(start_date, '%Y-%m-%d')
        self.end_date = datetime.strptime(end_date, '%Y-%m-%d')
        self.credential_file = credential_file
        self.processingLevel = processingLevel
        self.cloudcoverpercentage = cloudcoverpercentage
        self.satellite = satellite
        self.user = user
        self.password = password
        self.maxRecords = maxRecords
        self.rootURL = 'https://peps.cnes.fr/resto/api/collections/S2ST/search.json?'
        self.product_summary = pd.DataFrame(columns=['product_list', 'product_size', 'product_url',
                                                     'cloud_coverage', 'satellite_datetime'])
        if isinstance(self.Input, list):
            if len(self.Input) > 2:
                if self.Input[0] > self.Input[1] or self.Input[2] > self.Input[3]:
                    raise ValueError('Invalid longitude or latitude values')
                self.search_input = f'box={self.Input[0]},{self.Input[2]},{self.Input[1]},{self.Input[3]}'
            elif len(self.Input) == 2:
                self.search_input = f'lat={self.Input[1]}&lon={self.Input[0]}'
        elif isinstance(self.Input, str):
            self.search_input = f'tileid={self.Input}'
            
        if self.processingLevel == 1:
            self.processingLevel = '1C'
        elif self.processingLevel == 2:
            self.processingLevel = '2A'
            
        self.search_processingLevel = f'processingLevel=LEVEL{self.processingLevel}'
        
        if not self.user or not self.password:
            with open(self.credential_file, 'r') as f:
                self.user, self.password = f.read().strip().split(',')
        self.search()

    def search(self):
        time_range = pd.date_range(self.start_date, self.end_date)
        years = time_range.year.unique()
        for year in years:
            tmp_time_range = time_range[time_range.year == year]
            months = tmp_time_range.month.unique()
            for month in months:
                s_date = tmp_time_range[(tmp_time_range.year == year) & (tmp_time_range.month == month)].min()
                e_date = tmp_time_range[(tmp_time_range.year == year) & (tmp_time_range.month == month)].max()
                
                if s_date == e_date:
                    e_date = s_date + timedelta(days=1)
                s_date = s_date.strftime('%Y-%m-%d')
                e_date = e_date.strftime('%Y-%m-%d')
                self.search_time_range(s_date, e_date)

    def search_time_range(self, start_date, end_date):
        search_timerange = f'startDate={start_date}&completionDate={end_date}'
        url = "{}{}&{}&{}&maxRecords={}".format(self.rootURL, 
                                                self.search_input, 
                                                self.search_processingLevel, 
                                                search_timerange, 
                                                self.maxRecords)
        # print(url)
        response = requests.get(url, auth=(self.user, self.password))
        
        data = response.json()
        if not data:
            print('Request Failed')
        elif len(data['features']) == 0:
            print(f'No product is available from {start_date} to {end_date}')
        else:
            product_list = data['features']
            print(f'{len(product_list)} products found from {start_date} to {end_date}')
            for product in product_list:
                self.product_summary = pd.concat([self.product_summary, self.product_info(product['properties'])], ignore_index=True)
        if not self.satellite:
            self.product_summary = self.product_summary[self.product_summary['product_list'].str.contains(self.satellite)]
        self.product_summary = self.product_summary[self.product_summary['cloud_coverage'] <= self.cloudcoverpercentage]

    def product_info(self, product):
        product_summary = {}
        product_summary['product_list'] = product['title']
        size_bytes = product['services']['download']['size']
        size = size_bytes / (1024 * 1024) 
        product_summary['product_size'] = size
        product_summary['product_url'] = product['services']['download']['url']
        product_summary['cloud_coverage'] = product['cloudCover']
        product_summary['satellite_datetime'],_ = get_datetime_from_S2(product['title'])
        return pd.DataFrame([product_summary])

# S2_search = S2_Peps_Search(Input='31UCS')
# product_list=S2_search.product_summary