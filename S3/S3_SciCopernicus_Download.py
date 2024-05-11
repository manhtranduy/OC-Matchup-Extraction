import os
import requests
import time
from S3.S3_SciCopernicus_Search import S3_SciCopernicus_Search
from support_functions import get_datetime_from_S3, check_exist
from requests.auth import HTTPBasicAuth
import base64
from typing import Union, List
import pandas as pd
import zipfile

class S3_SciCopernicus_Download:
    def __init__(self, product_list: Union[str, List[str]], 
                 credential_file=os.getcwd() + '/credentials_copernicus.txt', 
                 outdir=os.getcwd(),  
                 user='', password='', 
                 unzip=True, 
                 removeZipfile=True):
        
        self.product_list = [product_list] if isinstance(product_list, str) else product_list
        self.credential_file = credential_file
        self.outdir = outdir
        self.user = user
        self.password = password
        self.unzip = unzip
        self.removeZipfile = removeZipfile
        self.handle_credentials()
        self.summary = pd.DataFrame()
             
    def handle_credentials(self):
        if not self.user or not self.password:
            with open(self.credential_file, 'r') as file:
                credentials = file.read().split(',')
                self.user, self.password = credentials[0], credentials[1]

    def download(self):
        for product_name in self.product_list:
            summary_temp = self.download_image_by_product_name(product_name)
            self.summary = pd.concat([self.summary, summary_temp], ignore_index=True)
            self.summary.reset_index()

    def download_image_by_product_name(self, product_name):
        _, date_str = get_datetime_from_S3(product_name)
        product_summary = S3_SciCopernicus_Search(product_name, date_str, date_str)
        product_summary=product_summary.product_summary
        product_url = product_summary['product_url'][0]
        product_size = product_summary['product_size'][0]
        # Download image
        download_name, iszip, isfolder = self.download_image(product_name, product_url, product_size)
        
        # Return summary df
        summary_temp = pd.DataFrame({
            'product_list': product_name,
            'zip_status': iszip,
            'dir_status': isfolder
        }, index=[0])

        return pd.concat([summary_temp, product_summary.product_summary.drop(['product_list'], axis=1)], axis=1)

    def download_image(self, product_name, product_url, product_size):
        if float(product_size) > 4000:
            print('The file is too big to download, skipping ...')
            return 
        
        download_zip_file = os.path.join(self.outdir, f'{product_name}.zip')
        exist_file, isfolder, iszip = check_exist(self.outdir, product_name, product_size)
        if not exist_file:
            k = 0
            while True:
                print(f'{product_name}: \n downloading ...')
                try:
                    self.download_file(download_zip_file, product_url)
                except Exception as e:
                    print(f'Error: Failed to download: {str(e)}')
                exist_file, isfolder, iszip = check_exist(self.outdir, product_name, product_size)
                
                if exist_file:
                    print(f'{product_name}: \n is downloaded successfully!')
                    break
                
                print('download failed, wait 15 seconds')
                k += 1
                print(f'retrying {k} times')
                time.sleep(15)
        else:
            print(f'{product_name} is already downloaded')
            
        exist_file, isfolder, iszip = check_exist(self.outdir, product_name, product_size)
        if self.unzip and not isfolder and iszip:
            print(f'{product_name}: unziping ...')
            os.makedirs(self.outdir, exist_ok=True)  # ensure the directory exists
            with zipfile.ZipFile(download_zip_file, 'r') as zip_ref:
                zip_ref.extractall(self.outdir)
        
        _, isfolder, iszip = check_exist(self.outdir, product_name, product_size)
        if self.removeZipfile and iszip:
            os.remove(download_zip_file)
            print(f'{product_name}: zipfile removed ...')
        
        _, isfolder, iszip = check_exist(self.outdir, product_name, product_size)
        return product_name, iszip, isfolder

    def download_file(self, outfile, download_url):
        credentials = base64.b64encode(f"{self.user}:{self.password}".encode())
        headers = {'Authorization': f'Basic {credentials.decode()}'}
        response = requests.get(download_url, headers=headers,auth=HTTPBasicAuth(self.user, self.password))
        with open(outfile, 'wb') as file:
            file.write(response.content)

# Usage:
# downloader = S3_SciCopernicus_Download('S3A_OL_1_EFR____20210130T025320_20210130T025620_20210131T073406_0179_068_032_2700_LN1_O_NT_002.SEN3')
# downloader.download()
# summary=downloader.summary