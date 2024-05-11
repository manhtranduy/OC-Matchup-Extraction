# MODIS_NASA_Search.py
'''Search for MODIS products from NASA server'''

import os
import datetime
import requests

from typing import List, Union
import pandas as pd
from bs4 import BeautifulSoup
import re
import json


class MODIS_NASA_Search:
    def __init__(self, Input:Union[str, List[str], List[float]], 
                 start_date:str='2002-07-04', 
                 end_date:str=datetime.datetime.now().strftime('%Y-%m-%d'), 
                 datacollection:List[str]=['C1570116979-OB_DAAC'], 
                 # C1379762888-LAADS
                 # C1570116979-OB_DAAC
                 # C2330511440-OB_DAAC
                 level:int=1,
                 number_of_results:int=1000):
        
        self.Input = Input
        self.start_date = start_date
        self.end_date = end_date
        self.datacollection = datacollection
        self.level = level
        self.number_of_results = number_of_results
        self.rootURL='https://cmr.earthdata.nasa.gov/search/granules'
        self.product_summary = pd.DataFrame()
        self.Data = []
        
        if self.level==1:
            self.datacollection=['C1570116979-OB_DAAC']
        elif self.level==2:
            self.datacollection=['C2330511440-OB_DAAC']
            
        # Handle input
        self.handle_spatial_input()
        self.search()
    
           
    def handle_spatial_input(self):
        if isinstance(self.Input, list):
            if len(self.Input) > 2:
                if self.Input[0] > self.Input[1]:
                    raise ValueError('Error: Please enter in a valid input longitude')
                elif self.Input[2] > self.Input[3]:
                    raise ValueError('Error: Please enter in a valid input latitude')
                self.spatial_input = f'bounding_box[]={self.Input[0]:.4f},{self.Input[2]:.4f},{self.Input[1]:.4f},{self.Input[0]:.4f}'
                
            elif len(self.Input) == 2:
                self.spatial_input = f'point={self.Input[0]:.4f},{self.Input[1]:.4f}'
    
    def handle_temporal_input(self,start_date,end_date):
        self.temporal_input="temporal={}T00:00:00Z,{}T23:59:59Z".format(start_date.strftime("%Y-%m-%d"),end_date.strftime("%Y-%m-%d"))
            

            
    def search(self):
        self.start_date = datetime.datetime.strptime(self.start_date, '%Y-%m-%d')
        self.end_date = datetime.datetime.strptime(self.end_date, '%Y-%m-%d')
        time_range = pd.date_range(self.start_date, self.end_date)
        y = [date.year for date in time_range]
        uy = list(set(y))
        m = [date.month for date in time_range]
    
        if len(time_range) == 1:
            # If the time range is only one day
            self.search_time_range(time_range[0], time_range[0])
        else:
            # If the time range is more than one day
            for year in uy:
                for month in set(m):
                    start_date = datetime.datetime(year, month, 1)
                    end_date = (datetime.datetime(year, month+1, 1) - 
                                datetime.timedelta(days=1)) if month != 12 else datetime.datetime(year, month, 31)
                    if (self.start_date > start_date) and (self.start_date.month == start_date.month):
                        start_date = self.start_date
                    if (self.end_date < end_date) and (self.end_date.month == end_date.month):
                        end_date = self.end_date
                        
                    self.search_time_range(start_date, end_date)
        

    def search_time_range(self, start_date, end_date):
        if isinstance(self.datacollection, str):
            self.datacollection = [self.datacollection]
        self.handle_temporal_input(start_date,end_date)  
        for datacollection in self.datacollection:
            url = "{}.json?collection_concept_id={}&{}&{}&page_size=2000".format(
                    self.rootURL,
                    datacollection,
                    self.spatial_input,
                    self.temporal_input)
            
            try:
                self.Data = requests.get(url).json()
            except Exception as e:
                print(f'Failed to read URL with {datacollection}: {str(e)} \n')
                continue
    
            if 'feed' in self.Data and len(self.Data['feed']['entry']) != 0:
                break
            else:
                return
    
        if not self.Data:
            print('Request Failed')
            # print(url)
            return
        elif len(self.Data['feed']['entry']) == 0:
            print(f'No product is available from {start_date.strftime("%Y-%m-%d")} to {end_date.strftime("%Y-%m-%d")}')
            return
        else:
            products = self.Data['feed']['entry']
            print(f'{len(products)} products found from {start_date.strftime("%Y-%m-%d")} to {end_date.strftime("%Y-%m-%d")}')
            for prod in products:
                self.product_summary = pd.concat([self.product_summary, self.product_info(prod)], ignore_index=True)

    def product_info(self, product):
        product_summary = {}
        if 'C1379762888-LAADS' in self.datacollection:
            collection_link=product['links'][5]['href'];
            collection_link_split=collection_link.split('/')
            collection = collection_link_split[-3];
            product_id=product['title'].split(':')[1]
            # read general info
            url_detail = 'https://ladsweb.modaps.eosdis.nasa.gov/details/file/{}/{}'.format(collection,product_id);
            response = requests.get(url_detail)
            soup = BeautifulSoup(response.content, 'html.parser')
            table = soup.find('table', class_='table')
            # Find all the tr tags in the table
            rows_list = []
            for row in table.find_all('tr'):
                # Get all td tags in this row
                cols = row.find_all('td')
                # Extract text from each td tag and store it as a tuple
                rows_list.append((cols[0].text.strip(), cols[1].text.strip()))
            
            # Convert list of tuples to pandas DataFrame
            general_info = pd.DataFrame(rows_list, columns=['Property', 'Value'])
            general_info=general_info.set_index('Property').T
            product_summary['product_list']=general_info['File Name'].iloc[0]
            product_summary['product_id']=product_id
            product_summary['collection']=general_info['Collection'].iloc[0]
            product_summary['url_detail']=url_detail
            product_summary['satellite_datetime']=general_info["Date/Time Sampled"].iloc[0]
            
            product_comp = product_summary['product_list'].split('.')
            year = product_comp[1][1:5]
            date = product_comp[1][5:8]
            base_url ='https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/{}/{}/{}/{}/'.format(
                       general_info['Collection'].values[0],
                       general_info['ESDT'].values[0],
                       year,
                       date)
            response = requests.get(base_url)
            xml_content = response.text

            # List products
            matches = re.search(r'window\.laadsArchiveFiles = (\[.*?\]);', xml_content)
            if matches:
                json_array = matches.group(1)
                # Convert the JSON array to a Python list
                product_list = json.loads(json_array)

                # Extract the names and put them in a list
                products = [product['name'] for product in product_list]
            else:
                print("No product list found")
            
            base_name = '.'.join(product_comp[:-2])
            for product in products:
                if base_name in product:
                    # print(product)
                    product_summary['product_list'] = product
                    product_summary['product_link'] = base_url + product
                
            
        elif 'C1570116979-OB_DAAC' in self.datacollection:
            product_summary['product_list']=product['producer_granule_id']
            product_summary['satellite_datetime'] = datetime.datetime.strptime(product['time_start'].replace('.000Z', ''), '%Y-%m-%dT%H:%M:%S')
            product_summary['product_link']=product['links'][0]['href']
            product_summary['polygons']=product['polygons']
            product_summary['day_night_flag']=product['day_night_flag']
        
        elif 'C2330511440-OB_DAAC' in self.datacollection:
            product_summary['product_list']=product['producer_granule_id']
            product_summary['satellite_datetime'] = datetime.datetime.strptime(product['time_start'].replace('.000Z', ''), '%Y-%m-%dT%H:%M:%S')
            product_summary['product_link']=product['links'][0]['href']
            product_summary['polygons']=product['polygons']
            product_summary['day_night_flag']=product['day_night_flag']
         
            # product_summary['product_list'] = product[]
        return pd.DataFrame([product_summary])
    
    
# MODIS_search = MODIS_NASA_Search(Input=[105,10], start_date='2021-01-01', end_date='2021-01-10')
# product_list=MODIS_search.product_summary

