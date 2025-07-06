# S3_Eumetsat_Search.py
'''Search for Sentinel-3 products from Eumetsat server'''

import os
import datetime
import requests
import traceback

from typing import List, Union
import pandas as pd
from support_functions import get_datetime_from_S3, handle_credentials
import time

from concurrent.futures import ThreadPoolExecutor, as_completed



class S3_Eumetsat_Search:
    def __init__(self, Input:Union[str, List[str], List[float]], start_date:str='2016-02-16', 
                 end_date:str=datetime.datetime.now().strftime('%Y-%m-%d'), 
                 credential_file:str=os.getcwd()+'/credentials_eumdac.txt', 
                 processingLevel:int=[], 
                 resolution:str='full', 
                 datacollection:List[str]=['EO:EUM:DAT:0577', 'EO:EUM:DAT:0409'], 
                 satellite:str='', 
                 user:str='', 
                 password:str='',
                 max_threads:int = 1,
                 number_of_results:int=1000):
        
        self.Input = Input
        self.start_date = start_date
        self.end_date = end_date
        self.credential_file = credential_file
        self.processingLevel = processingLevel
        self.resolution = resolution
        self.datacollection = datacollection
        self.satellite = satellite
        self.user = user
        self.password = password
        self.number_of_results = number_of_results
        self.max_threads = max_threads
        
        self.rootURL='https://api.eumetsat.int/data/search-products/1.0.0/os?format=json&pi='
        self.product_summary = pd.DataFrame()
        self.Data = []
        
        if self.processingLevel:
            self.handle_processing_level()
            
        # self.auth()
        self.handle_input()
        
        self.search()
    

        if len(self.user)==0 or len(self.password)==0:
            self.user, self.password = handle_credentials(self.credential_file)
           
    
    def auth(self):
        try:
            self.token = requests.post('https://api.eumetsat.int/token', 
                                       data={'grant_type': 'client_credentials'}, 
                                       auth=(self.user, self.password)).json()
        except Exception as e:
            raise ValueError(f'Credential Error: {e}')
            
    def handle_input(self):
        if isinstance(self.Input, list):
            if len(self.Input) > 2:
                if self.Input[0] > self.Input[1]:
                    raise ValueError('Error: Please enter in a valid input longitude')
                elif self.Input[2] > self.Input[3]:
                    raise ValueError('Error: Please enter in a valid input latitude')
                self.search_input = f'&bbox={self.Input[0]:.4f},{self.Input[2]:.4f},{self.Input[1]:.4f},{self.Input[3]:.4f}'
                
            elif len(self.Input) == 2:
                self.search_input = f'&bbox={self.Input[0]:.4f},{self.Input[1]:.4f},{self.Input[0]:.4f},{self.Input[1]:.4f}'
                
        elif isinstance(self.Input, str):
            self.search_input = f'&title={self.Input}'
            
    def handle_processing_level(self):
        if self.processingLevel == 1:
            self.datacollection = ['EO:EUM:DAT:0577', 'EO:EUM:DAT:0409']
        elif self.processingLevel == 2:
            self.datacollection = ['EO:EUM:DAT:0556','EO:EUM:DAT:0407']
    
    def search(self):
        self.start_date = datetime.datetime.strptime(self.start_date, '%Y-%m-%d')
        self.end_date = datetime.datetime.strptime(self.end_date, '%Y-%m-%d')
        time_range = pd.date_range(self.start_date, self.end_date)
        y = [date.year for date in time_range]
        uy = list(set(y))
        m = [date.month for date in time_range]
    
        def process_month_year(year, month):
            start_date = datetime.datetime(year, month, 1)
            end_date = (datetime.datetime(year, month+1, 1) -
                        datetime.timedelta(days=1)) if month != 12 else datetime.datetime(year, month, 31)
            if (self.start_date > start_date) and (self.start_date.month == start_date.month):
                start_date = self.start_date
            if (self.end_date < end_date) and (self.end_date.month == end_date.month):
                end_date = self.end_date
            try:   
               products_df = self.search_time_range(start_date, end_date)
            except Exception as e:
               error_message = f"Error processing {year}-{month}: {e}\n{traceback.format_exc()}"
               print(error_message)
               products_df = pd.DataFrame()
            return products_df
        
        # Modify search_time_range to return a DataFrame of products instead of directly merging
        # Assume search_time_range is adjusted accordingly
    
        if len(time_range) == 1:
            product_summary_df = self.search_time_range(time_range[0], time_range[0])
            self.product_summary = product_summary_df
        else:
            tasks = [(year, month) for year in uy for month in set(m)]
            product_summary_dfs = []
            error_messages = []
            if self.max_threads > 1:
                max_threads = min(self.max_threads, 3)
                
                product_summary_results = []
            
                with ThreadPoolExecutor(max_workers=max_threads) as executor:
                    # Submit all tasks
                    future_to_task = {executor.submit(process_month_year, year, month): (year, month) for year, month in tasks}
                    
                    # Collect results as tasks complete
                    for future in as_completed(future_to_task):
                        year, month = future_to_task[future]
                        try:
                            product_summary_df = future.result()
                            # Append a tuple of (year, month, DataFrame) to maintain order independently of task completion
                            product_summary_results.append((year, month, product_summary_df))
                        except Exception as exc:
                            print(f'Task {year}-{month} failed: {exc}')
                
                # Sort results by year and month to ensure deterministic order
                product_summary_results.sort(key=lambda x: (x[0], x[1]))
                
                # Aggregate results
                product_summary_dfs = [result[2] for result in product_summary_results if not result[2].empty]
                if product_summary_dfs:
                    self.product_summary = pd.concat(product_summary_dfs, ignore_index=True)
                else:
                    self.product_summary = pd.DataFrame()
        
            else:
                for year, month in tasks:
                    product_summary_df = process_month_year(year, month)
                    if not product_summary_df.empty:
                        product_summary_dfs.append(product_summary_df)
    
            if product_summary_dfs:
                self.product_summary = pd.concat(product_summary_dfs, ignore_index=True)
            else:
                self.product_summary = pd.DataFrame()
                
    # def search(self):
    #     self.start_date = datetime.datetime.strptime(self.start_date, '%Y-%m-%d')
    #     self.end_date = datetime.datetime.strptime(self.end_date, '%Y-%m-%d')
    #     time_range = pd.date_range(self.start_date, self.end_date)
    #     y = [date.year for date in time_range]
    #     uy = list(set(y))
    #     m = [date.month for date in time_range]
    
    #     if len(time_range) == 1:
    #         # If the time range is only one day
    #         self.search_time_range(time_range[0], time_range[0])
    #     else:
    #         # If the time range is more than one day
    #         for year in uy:
    #             for month in set(m):
    #                 start_date = datetime.datetime(year, month, 1)
    #                 end_date = (datetime.datetime(year, month+1, 1) - 
    #                             datetime.timedelta(days=1)) if month != 12 else datetime.datetime(year, month, 31)
    #                 if (self.start_date > start_date) and (self.start_date.month == start_date.month):
    #                     start_date = self.start_date
    #                 if (self.end_date < end_date) and (self.end_date.month == end_date.month):
    #                     end_date = self.end_date
    #                 self.search_time_range(start_date, end_date)

    def search_time_range(self, start_date, end_date):
        if isinstance(self.datacollection, str):
            self.datacollection = [self.datacollection]
            
        for datacollection in self.datacollection:
            url = "{}{}{}&dtstart={}T00:00:00.000Z&dtend={}T23:59:59.999Z&c={}&sat={}".format(
                    self.rootURL,
                    datacollection,
                    self.search_input,
                    start_date.strftime("%Y-%m-%d"),
                    end_date.strftime("%Y-%m-%d"),
                    self.number_of_results,
                    self.satellite)
            
            while True:
                try:
                    self.Data = requests.get(url).json()
                    break  # Exit the loop if data retrieval is successful
                except Exception as e:
                    print(f'Failed to read URL with {datacollection}: {str(e)} \n')
                    print('wait 5 seconds and retry \n')
                    time.sleep(5)
                    continue  # Continue to the next iteration if there's an error
    
            if 'totalResults' in self.Data and self.Data['totalResults'] != 0:
                break
    
        productsRequested = pd.DataFrame()
        if not self.Data:
            print('Request Failed')
            # print(url)
            return productsRequested
        if not 'totalResults' in self.Data:
            # print(url)
            return productsRequested
        elif self.Data['totalResults'] == 0:
            # print(url)
            print(f'No product is available from {start_date.strftime("%Y-%m-%d")} to {end_date.strftime("%Y-%m-%d")}')
            return productsRequested
        else:
            products = self.Data['features']
            print(f'{len(products)} products found from {start_date.strftime("%Y-%m-%d")} to {end_date.strftime("%Y-%m-%d")}')
            
            for prod in products:
                productsRequested = pd.concat([productsRequested, self.product_info(prod)], ignore_index=True)
            return productsRequested

    def product_info(self, product):
        product_summary = {}
        product_summary['product_list'] = product['id']
        product_summary['product_size'] = product['properties']['productInformation']['size'] / 1024
        for item in product['properties']['links']['data']:
            if 'href' in item:
                product_summary['product_url'] = item['href']
                break
        product_summary['satellite_datetime'],_ = get_datetime_from_S3(product_summary['product_list'])
        # return product_summary
        return pd.DataFrame([product_summary])
    
    
# S3_search = S3_Eumetsat_Search(Input=[105,10], start_date='2021-01-01', end_date='2021-03-31')
# product_list=S3_search.product_summary

