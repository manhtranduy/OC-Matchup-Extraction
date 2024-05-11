# MODIS_NASA_Download.py
'''Download MODIS products from NASA server'''

# from MODIS_NASA_Search import MODIS_NASA_Search
import os
import datetime
import requests
import pandas as pd
import zipfile
import time
from typing import Union, List
# import platform
import subprocess
import xml.etree.ElementTree as ET
from tqdm import tqdm
import concurrent.futures
import sys
import math    

class MODIS_NASA_Download:
    def __init__(self, 
                 product_list: Union[str, List[str]], 
                 email: str = 'manhtranduy1993@gmail.com',
                 token: str = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJlbWFpbF9hZGRyZXNzIjoibWFuaHRyYW5kdXkxOTkzQGdtYWlsLmNvbSIsImlzcyI6IkFQUyBPQXV0aDIgQXV0aGVudGljYXRvciIsImlhdCI6MTY5OTYwNDIwMSwibmJmIjoxNjk5NjA0MjAxLCJleHAiOjE4NTcyODQyMDEsInVpZCI6Im1hbmh0cmFuZHV5IiwidG9rZW5DcmVhdG9yIjoibWFuaHRyYW5kdXkifQ.nPka78kCQgKv6pUSKOvjVb4siNjZNuHcOIakvqXhAts',
                 # '''Go to https://ladsweb.modaps.eosdis.nasa.gov/ for token'''
                 outdir: str = os.getcwd(),
                 chunk_size: int = 100,
                 max_threads: int = os.cpu_count()):

        self.product_list = [product_list] if isinstance(product_list, str) else product_list
        self.email = email
        self.token = token
        self.outdir = outdir
        self.chunk_size = chunk_size
        self.max_threads = max_threads
        self.max_threads=1

        self.summary = pd.DataFrame()
        product_lists=[]
        for i in range(0, len(self.product_list), self.chunk_size):
            product_list = self.product_list[i:i + self.chunk_size]
            product_list=list(set(product_list))
            product_lists.append(product_list)
        
        
        if self.max_threads>1:
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_threads) as executor:
                # Submit the download function for each product in parallel
                futures = [executor.submit(self.download, product_list) for product_list in product_lists]
            
                # Wait for all tasks to complete
                concurrent.futures.wait(futures)
        else:
            for product_list in product_lists:
                self.download(product_list)
                
        


    def download(self,product_list):
        
        rootURL='https://modwebsrv.modaps.eosdis.nasa.gov/axis2/services/MODAPSservices/'
        product_list = [x for x in product_list if not (isinstance(x, float) and math.isnan(x))]
        
        if any('.' in str(product) for product in product_list):
            product_id=[]
            print('Generating product IDs ... \n')
            for product in product_list:
                URL = f'{rootURL}searchForFilesByName?collection=61&pattern={product}'
                response = requests.get(URL)  
                xml_content = response.text
                root = ET.fromstring(xml_content)
                product_id.append(root.find('.//return').text)
                
            if 'No results' in product_id:
                raise ValueError("Error when generate product IDs consider product IDs as input")
            print('Generating product IDs Done! \n')
        else:
            product_id = product_list
        


        # Order products
        print('Ordering products ... \n')
        request_order_url = '{}orderFiles?email={}&fileIds={}'.format(
            rootURL,
            self.email,
            ','.join(product_id))  
        
        # Get Order id
        response = requests.get(request_order_url)
        xml_content = response.text
        root = ET.fromstring(xml_content)
        order_id = root.find('.//return').text
        
        # Get Order status
        status_order_url = '{}getOrderStatus?orderId={}'.format(
                    rootURL,
                    order_id)
        
        while True:
            response = requests.get(status_order_url)
            xml_content = response.text
            root = ET.fromstring(xml_content)
            order_status = root.find('.//return').text
            print(f'Order {order_id}: {order_status} ... \n', )
            if order_status=='Available':
                print(f'Order {order_id}: Ready to download ... \n')
                break
            time.sleep(15) 
        
        #Download
        product_url=f'https://ladsweb.modaps.eosdis.nasa.gov/archive/orders/{order_id}/' 
        release_order_url = f'{rootURL}releaseOrder?orderId={order_id}&email={self.email}'
        cmd_line = f'wget -e robots=off -np -R .html,.tmp -A hdf -nH --cut-dirs=3 --header "Authorization: Bearer {self.token}" -nc "{product_url}" -P "{self.outdir}"'
        
        try:
            process = subprocess.Popen(
                    cmd_line,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1,
                    universal_newlines=True,
                    shell=True
                )
                
            for line in process.stdout:
                sys.stdout.write("\r" + line)
                sys.stdout.flush()
           
            process.wait()  # Wait for the wget command to finish
            response = requests.get(release_order_url)
        except Exception:
            response = requests.get(release_order_url)

 

# Usage:
# MODIS_search = MODIS_NASA_Search(Input=[105,10], start_date='2021-01-01', end_date='2021-01-10')
# products=MODIS_search.product_summary
# downloader = MODIS_NASA_Download(products['product_id'].tolist(),
#                                  max_threads=1,chunk_size=3)

# summary=downloader.summary
