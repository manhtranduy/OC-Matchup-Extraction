# S2_SciCopernicus_Download.py
'''Download for Sentinel-2 products from SciCopernicus server'''

import os
import requests
import zipfile
import time
from typing import Union, List
import pandas as pd
from S2.S2_SciCopernicus_Search import S2_SciCopernicus_Search
from support_functions import get_datetime_from_S2, check_exist, handle_credentials, get_tile_from_S2
from requests.auth import HTTPBasicAuth
import base64
from tqdm import tqdm 


class S2_SciCopernicus_Download:
    def __init__(self, product_list: Union[str, List[str]], 
                 credential_file=os.getcwd() + '/credentials_copernicus.txt', 
                 outdir=os.getcwd(), 
                 useParallel=False, 
                 user='', password='', 
                 unzip=True, 
                 removeZipfile=True):
        self.product_list = product_list
        self.credential_file = credential_file
        self.outdir = outdir
        self.useParallel = useParallel
        self.user = user
        self.password = password
        self.unzip = unzip
        self.removeZipfile = removeZipfile
        self.summary = []
        
        if len(self.user)==0 or len(self.password)==0:
            self.user, self.password = handle_credentials(self.credential_file)

        self.product_list = [product_list] if isinstance(product_list, str) else product_list  

    def download(self):
        for product_name in self.product_list:
            summary_temp = self.download_image_by_product_name(product_name)
            self.summary = pd.concat([self.summary, summary_temp], ignore_index=True)

    def download_image_by_product_name(self, product_name):
        _, date_str = get_datetime_from_S2(product_name)
        
        Tile = get_tile_from_S2(product_name)
        product_summary = S2_SciCopernicus_Search(Input=Tile,start_date=date_str,end_date= date_str)
        product_url = product_summary.product_summary['product_url'][0]
        product_size = product_summary.product_summary['product_size'][0]

        product_name, iszip, isfolder = self.download_image(product_name, product_url, product_size)
        
        summary_temp = pd.DataFrame({
            'product_list': product_name,
            'zip_status': iszip,
            'dir_status': isfolder
        }, index=[0])

        return pd.concat([summary_temp, product_summary.product_summary.drop(['product_list'], axis=1)], axis=1)

    def download_image(self, product_name, product_url, product_size):
        if product_size > 4000:
            print('The file is too big to download, skipping ...')
            return product_name, False, False
        download_zip_file = os.path.join(self.outdir, f'{product_name}.zip')
        exist_file, isfolder, iszip = check_exist(self.outdir, product_name, product_size)
        if not exist_file:
            k = 0
            while True:
                print(f'downloading {product_name}')
                try:
                    self.download_file(download_zip_file, product_url,product_size) 
                except Exception as e:
                    print(f'Error: Failed to download: {str(e)}')
                exist_file, isfolder, iszip = check_exist(self.outdir, product_name, product_size)
                if exist_file:
                    print(f'{product_name} is saved')
                    break
                print('download failed, wait 15 seconds')
                k += 1
                print(f'retry {k} times')
                time.sleep(15)
        else:
            print(f'{product_name} is already downloaded')
        if self.unzip and not isfolder and iszip:
            print(f'{product_name} unzipping ...')
            os.makedirs(self.outdir, exist_ok=True)
            with zipfile.ZipFile(download_zip_file, 'r') as zip_ref:
                zip_ref.extractall(self.outdir)
        isfolder = os.path.isdir(os.path.join(self.outdir, product_name))
        if self.removeZipfile and isfolder and iszip:
            os.remove(download_zip_file)
            print(f'{product_name} zipfile removed ...')
        iszip = os.path.isfile(download_zip_file)
        return product_name, iszip, isfolder

    def download_file(self, outfile, download_url, product_size):
        credentials = base64.b64encode(f"{self.user}:{self.password}".encode())
        headers = {'Authorization': f'Basic {credentials.decode()}'}
        
        response = requests.get(download_url, headers=headers,
                                auth=HTTPBasicAuth(self.user, self.password),
                                stream=True)
        # Total size in bytes.
        total_size = int(product_size*1024*1024)
        # print(total_size)
        block_size = 1024 #1 Kibibyte
        progress_bar = tqdm(total=total_size, unit='iB', unit_scale=True,position=0, leave=True)
        
        with open(outfile, 'wb') as file:
            for data in response.iter_content(block_size):
                progress_bar.update(len(data))
                file.write(data)
        progress_bar.close()
        if total_size != 0 and progress_bar.n != total_size:
            print("Product might be offline")

# # Usage:
# downloader = S2_SciCopernicus_Download('S2B_MSIL1C_20210130T105209_N0209_R051_T31UCS_20210130T120256')
# downloader.download()
# summary = downloader.summary