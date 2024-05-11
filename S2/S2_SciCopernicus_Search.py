import os
import re
import requests
from datetime import datetime
import pandas as pd
import numpy as np
from support_functions import get_datetime_from_S2

class S2_SciCopernicus_Search:
    def __init__(self, 
                 Input, 
                 start_date: str, 
                 end_date: str, 
                 credential_file: str = os.getcwd()+'/credentials_copernicus.txt', 
                 platformname: str = 'Sentinel-2', 
                 satellite: str = '', 
                 producttype: str = 'S2MSI1C', 
                 processingLevel: int = 1, 
                 cloudcoverpercentage: int = 100, 
                 user: str = '', 
                 password: str = ''):
        
        self.Input = Input
        self.start_date = start_date
        self.end_date = end_date
        self.credential_file = credential_file
        self.platformname = platformname
        self.satellite = satellite
        self.producttype = producttype
        self.processingLevel = processingLevel
        self.cloudcoverpercentage = cloudcoverpercentage
        self.user = user
        self.password = password
        self.product_summary = pd.DataFrame()

        # Authentication
        if not self.user or not self.password:
            self.user, self.password = self.handle_credentials(self.credential_file)
        
        self.search()

    def handle_credentials(self, credential_file):
        with open(credential_file, 'r') as file:
            credentials = file.read().split(',')
        return credentials[0], credentials[1]

    def search(self):
        # API URL
        rootURL = 'https://scihub.copernicus.eu/dhus/search?format=json&q='

        # Handle Location/Tile
        if isinstance(self.Input, list):
            # search according to specified location
            # Handle location
            if len(self.Input) > 2:
                if self.Input[1] > self.Input[2]:
                    raise ValueError('Error: Please enter in a valid input longitude')
                elif self.Input[3] > self.Input[4]:
                    raise ValueError('Error: Please enter in a valid input latitude')
                search_input = f'footprint:"Intersects(POLYGON(({self.Input[3]} {self.Input[1]},{self.Input[3]} {self.Input[2]},{self.Input[4]} {self.Input[2]},{self.Input[4]} {self.Input[1]},{self.Input[3]} {self.Input[1]})))"'
            elif len(self.Input) == 2:
                search_input = f'footprint:"Intersects({self.Input[2]}, {self.Input[1]})"'
        elif isinstance(self.Input, str):
            # search according to specified tile
            search_input = self.Input

        # Handle Processing Level
        if self.processingLevel == 1:
            self.producttype = 'S2MSI1C'
        elif self.processingLevel == 2:
            self.producttype = 'S2MSI2A'

        search_producttype = f'producttype:{self.producttype}' 

        # Handle platform
        search_platform = f'platformname:{self.platformname}'

        # Handle satellite
        search_satellite = f'filename:{self.satellite}*'

        # Handle cloud coverage
        search_cloud_percentage = f'cloudcoverpercentage:[0 TO {self.cloudcoverpercentage}]'

        # Handle time range
        start_date = datetime.strptime(self.start_date, '%Y-%m-%d')
        end_date = datetime.strptime(self.end_date, '%Y-%m-%d')
        time_range = pd.date_range(start_date, end_date)
        unique_years = np.unique(time_range.year)
        unique_months = np.unique(time_range.month)

        if len(time_range) == 1:
            self.search_time_range(start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'), rootURL, search_input, search_producttype, search_platform, search_cloud_percentage, search_satellite)
        elif len(time_range) > 1:
            for year in unique_years:
                for month in unique_months:
                    time_range_filtered = time_range[(time_range.year == year) & (time_range.month == month)]
                    s_date = time_range_filtered.min().strftime('%Y-%m-%d')
                    e_date = time_range_filtered.max().strftime('%Y-%m-%d')
                    self.search_time_range(s_date, e_date, rootURL, search_input, search_producttype, search_platform, search_cloud_percentage, search_satellite)

        # filter satellite
        if self.satellite:
            self.product_summary = self.product_summary[self.product_summary['product_list'].str.contains(self.satellite)]

    def search_time_range(self, s_date, e_date, rootURL, search_input, search_producttype, search_platform, search_cloud_percentage, search_satellite):
        search_timerange = f'beginPosition:[{s_date}T00:00:00.000Z TO {e_date}T23:59:59.999Z] AND endPosition:[{s_date}T00:00:00.000Z TO {e_date}T23:59:59.999Z]'
        
        URL = f'{rootURL} {search_input} AND {search_platform} AND {search_timerange} AND {search_cloud_percentage} AND {search_producttype} AND {search_satellite} &rows=100&start=0'

        headers = {
            'Authorization': f'Bearer {self.user}:{self.password}'
        }

        try:
            response = requests.get(URL, auth=(self.user, self.password))
            Data = response.json()
        except Exception as e:
            print(f'Failed to read URL: {str(e)}')
            Data = {}
            
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
        
        # Get size from summary
        size_str = re.search('Size: (.*? [M|G])', product['summary']).group(1)
        if 'M' in size_str:
            product_summary['product_size'] = float(size_str.replace('M', ''))
        elif 'G' in size_str:
            product_summary['product_size'] = float(size_str.replace('G', '')) * 1024
            
        product_summary['product_url'] = product['link'][0]['href']     
        product_summary['cloud_coverage'] = product['double']['content']
        product_summary['satellite_datetime'] = get_datetime_from_S2(product['title'])
        return pd.DataFrame([product_summary])

# S2_search = S2_SciCopernicus_Search(Input='31UCS', start_date='2021-01-01', end_date='2021-12-31')
# product_list=S2_search.product_summary