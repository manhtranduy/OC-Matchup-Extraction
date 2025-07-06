# S3_Eumetsat_Download.py
'''Download Sentinel-3 products from Eumetsat server'''

from S3.S3_Eumetsat_Search import S3_Eumetsat_Search
import os
import datetime
import requests
import pandas as pd
import zipfile
import time
import json
from typing import Union, List
from support_functions import get_datetime_from_S3, check_exist, handle_credentials
from tqdm import tqdm
    

class S3_Eumetsat_Download:
    def __init__(self, 
                 product_list: Union[str, List[str]], 
                 start_date: str = '2016-02-16', 
                 end_date: str = datetime.datetime.now().strftime('%Y-%m-%d'), 
                 credential_file: str = os.getcwd() + '/credentials_eumdac.txt', 
                 processingLevel: int = 1, 
                 resolution: str = 'full', 
                 datacollection: List[str] = ['EO:EUM:DAT:0577', 'EO:EUM:DAT:0409'], 
                 satellite: str = '', 
                 user: str = '', 
                 password: str = '',
                 outdir: str = os.getcwd(),
                 unzip: bool = True,
                 removeZipfile: bool = True,
                 token_file: str = os.path.join(os.getcwd(), 'eumetsat_token.json')):

        self.product_list = [product_list] if isinstance(product_list, str) else product_list
        self.start_date = start_date
        self.end_date = end_date
        self.credential_file = credential_file
        self.processingLevel = processingLevel
        self.resolution = resolution
        self.datacollection = datacollection
        self.satellite = satellite
        self.user = user
        self.password = password
        self.outdir = outdir
        self.unzip = unzip
        self.removeZipfile = removeZipfile
        self.token_file = token_file
        self.token = None

        if not os.path.exists(outdir):
            os.mkdir(outdir)

        self.summary = pd.DataFrame()

        if len(self.user)==0 or len(self.password)==0:
            self.user, self.password = handle_credentials(self.credential_file)
        
        # Get token from file or generate new one
        self.token = self.get_token()
            
        self.download()

    def get_token(self):
        """Get token from file if it exists and is valid, otherwise generate new one"""
        # Check if token file exists and token is valid
        if os.path.exists(self.token_file):
            try:
                with open(self.token_file, 'r') as f:
                    token_data = json.load(f)
                
                # Check if token is still valid (expiry time hasn't passed)
                expires_at = token_data.get('expires_at', 0)
                if expires_at > time.time() + 60:  # Add 60-second buffer
                    print("Using existing token from file")
                    return token_data['access_token']
            except (json.JSONDecodeError, KeyError):
                # If file is corrupted or doesn't have the expected format, ignore it
                pass
        
        # Generate new token
        return self.generate_and_save_token()

    def generate_and_save_token(self):
        """Generate new token and save it to file"""
        try:
            response = requests.post('https://api.eumetsat.int/token', 
                                    data={'grant_type': 'client_credentials'}, 
                                    auth=(self.user, self.password))
            
            if response.status_code != 200:
                raise ValueError(f"Failed to get token: {response.text}")
                
            token_data = response.json()
            
            # Add expiry time (current time + expires_in seconds)
            token_data['expires_at'] = time.time() + token_data.get('expires_in', 3600)
            
            # Save token to file
            with open(self.token_file, 'w') as f:
                json.dump(token_data, f)
            
            print("Generated and saved new token")
            return token_data['access_token']
            
        except Exception as e:
            raise ValueError(f'Credential Error: {e}')

    def auth(self):
        """Get authentication token (from cache or generate new one if needed)"""
        if self.token is None:
            self.token = self.get_token()
        return self.token

    def download(self):
        for product_name in self.product_list:
            summary_temp = self.download_image_by_product_name(product_name)
            self.summary = pd.concat([self.summary, summary_temp], ignore_index=True)

    def download_image_by_product_name(self, product_name):
        
        _,date_str = get_datetime_from_S3(product_name)
        datacollection = self.check_datacollection_from_productname(product_name)
  
        # Note that you might need to modify this line to match the API and parameters
        product_summary = S3_Eumetsat_Search(Input=product_name, start_date=date_str, end_date=date_str, datacollection=datacollection)
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
        
        if product_size > 2000:
            print('The file is too big to download, skipping ...\n ')
            return
        
        download_zip_file = os.path.join(self.outdir, f'{product_name}.zip')
            
        exist_file, isfolder, iszip = check_exist(self.outdir, product_name, product_size)  
        
        if not exist_file:
            k = 0
            while True:
                print(f'downloading: \n  {product_name}\n ')
                try:
                    # Use cached token first
                    token = self.token
                    # If token is not valid, it will be refreshed during download
                    self.download_file(download_zip_file, product_url, product_size, token)
                except requests.exceptions.HTTPError as e:
                    # If authentication error (401), refresh token and retry once
                    if e.response.status_code == 401:
                        print("Token expired. Refreshing...")
                        self.token = self.generate_and_save_token()
                        try:
                            self.download_file(download_zip_file, product_url, product_size, self.token)
                        except Exception as e2:
                            print(f'Error after token refresh: {str(e2)}')
                    else:
                        print(f'HTTP Error: {str(e)}')
                except Exception as e:
                    print(f'Error: Failed to download: {str(e)}')
                
                exist_file, isfolder, iszip = check_exist(self.outdir, product_name, product_size)
                if exist_file:
                    print(f'{product_name}: \n is successfully downloaded!\n ')
                    break
                
                print('download failed wait 15 seconds \n ')
                k += 1
                print(f'retry {k} times')
                time.sleep(15)
        else:
            print(f'{product_name}: \n already exists\n ')
        
        exist_file, isfolder, iszip = check_exist(self.outdir, product_name, product_size)
        if self.unzip and not isfolder and iszip:
            print(f'{product_name}: unziping ...')
            os.makedirs(self.outdir, exist_ok=True)  # ensure the directory exists
            with zipfile.ZipFile(download_zip_file, 'r') as zip_ref:
                zip_ref.extractall(self.outdir)
        
        exist_file, isfolder, iszip = check_exist(self.outdir, product_name, product_size)
        if self.removeZipfile and iszip:
            try:
                os.remove(download_zip_file)
                print(f'{product_name}: \n zipfile removed ...\n ')
            except:
                pass
        
        exist_file, isfolder, iszip = check_exist(self.outdir, product_name, product_size)
        return product_name, iszip, isfolder

    def download_file(self, outfile, download_url, product_size, token):
        response = requests.get(
            download_url, 
            headers={"Authorization": f"Bearer {token}"}, 
            stream=True
        )
        
        # Check for authentication errors and raise appropriate exception
        if response.status_code == 401:
            response.raise_for_status()  # This will raise an HTTPError
        
        # Total size in bytes
        total_size = int(product_size*1024*1024)
        block_size = 1024  # 1 Kibibyte

        progress_bar = tqdm(total=total_size, unit='iB', unit_scale=True, position=0, leave=True)
        
        with open(outfile, 'wb') as file:
            for data in response.iter_content(block_size):
                progress_bar.update(len(data))
                file.write(data)
        progress_bar.close()
            
    def check_datacollection_from_productname(self, product_name):
        datacollection = None
        if 'OL_2_WFR' in product_name:
            datacollection = ['EO:EUM:DAT:0556','EO:EUM:DAT:0407']
        elif 'OL_2_WRR' in product_name:
            datacollection = 'EO:EUM:DAT:0557'
        elif 'OL_1_EFR' in product_name and 'R_NT_002.SEN3' in product_name:
            datacollection = ['EO:EUM:DAT:0577', 'EO:EUM:DAT:0409']
        elif 'OL_1_EFR' in product_name and 'O_NT_002.SEN3' in product_name:
            datacollection = 'EO:EUM:DAT:0409'
        
        return datacollection


# Usage:
# downloader = S3_Eumetsat_Download('S3A_OL_1_EFR____20210130T025320_20210130T025620_20210131T082928_0179_068_032_2700_MAR_O_NT_002.SEN3')
# downloader.download()
# summary=downloader.summary