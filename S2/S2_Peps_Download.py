import os
import requests
import time
import pandas as pd
from S2.S2_Peps_Search import S2_Peps_Search
from support_functions import get_datetime_from_S2, check_exist, handle_credentials, get_tile_from_S2
from typing import Union, List
from requests.auth import HTTPBasicAuth
import base64
import zipfile
from tqdm import tqdm 
import concurrent.futures

class S2_Peps_Download:
    def __init__(self, product_list: Union[str, List[str]], 
                 credential_file:str =os.getcwd() + '/credentials_peps.txt', 
                 outdir:str =os.getcwd(),  
                 user:str ='', password:str ='', 
                 unzip:bool =True, 
                 removeZipfile:bool =True,
                 max_threads=1):
        
        self.product_list = product_list
        self.credential_file = credential_file
        self.outdir = outdir
        self.user = user
        self.password = password
        self.unzip = unzip
        self.removeZipfile = removeZipfile
        self.max_threads = max_threads
        self.summary = pd.DataFrame()

        if len(self.user)==0 or len(self.password)==0:
            self.user, self.password = handle_credentials(self.credential_file)
            
        self.product_list = [product_list] if isinstance(product_list, str) else product_list    
        
        self.download()

    def download(self):
        # print(self.product_list)
        # for product_name in self.product_list:
        #     summary_temp = self.download_image_by_product_name(product_name)
        #     self.summary = pd.concat([self.summary, summary_temp], ignore_index=True)
       
        #
        self.summary = []
        
        if self.max_threads > 1:
            # with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            #     futures = [executor.submit(self.download_image_by_product_name, product_name) for product_name in self.product_list]
                
            #     for future in futures:
            #         summary_temp.append(future.result())
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_threads) as executor:
                # Create a future for each product name
                futures = [executor.submit(self.download_image_by_product_name, product_name) for product_name in self.product_list]
            
                # Iterate over the futures and the indices of the product_list
                for future, idx in zip(futures, self.product_list.index):
                    try:
                        result = future.result()
                    except Exception as e:
                        result = pd.DataFrame()  # or however you want to represent a failed result
            
                    # Set the index of the result DataFrame to the corresponding index of the product_list
                    result.index = [idx]
                    self.summary.append(result)
                        
        else:
            for product_name, idx in zip(self.product_list,self.product_list.index):
                summary_tmp=self.download_image_by_product_name(product_name)
                summary_tmp.index = [idx]
                self.summary.append(summary_tmp)
                
        if self.summary:
            self.summary = pd.concat(self.summary).sort_index()
            
        
    def download_image_by_product_name(self, product_name):
        _, date_str = get_datetime_from_S2(product_name)
        
        Tile = get_tile_from_S2(product_name)
        product_summary = S2_Peps_Search(Input=Tile,start_date=date_str,end_date= date_str)
        product_summary=product_summary.product_summary
        
        if product_summary.shape[0] > 1:
            product_summary = product_summary.drop(product_summary['product_size'].idxmin())
            product_summary.reset_index(drop=True, inplace=True)
        
        
        product_url = product_summary['product_url'][0]
        product_size = product_summary['product_size'][0]
        download_name, iszip, isfolder = self.download_image(product_name, product_url, product_size)
        
        summary_temp = pd.DataFrame({
            'product_list': product_name,
            'zip_status': iszip,
            'dir_status': isfolder
            }, index=[0])
        
        return pd.concat([summary_temp, product_summary.drop(['product_list'], axis=1)], axis=1)

    def download_image(self, product_name, product_url, product_size):
        if product_size > 4000:
            print('The file is too big to download, skipping ...')
            return product_name, False, False
        download_zip_file = os.path.join(self.outdir, f'{product_name}.zip')
        print(download_zip_file)
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
                print('downloading failed, wait 15 seconds')
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
        
        exist_file, isfolder, iszip = check_exist(self.outdir, product_name, product_size)
        if self.removeZipfile and iszip:
            os.remove(download_zip_file)
            print(f'{product_name}: zipfile removed ...')
        
        exist_file, isfolder, iszip = check_exist(self.outdir, product_name, product_size)
        return product_name, iszip, isfolder

    def download_file(self, outfile, download_url,product_size):
        credentials = base64.b64encode(f"{self.user}:{self.password}".encode())
        headers = {'Authorization': f'Basic {credentials.decode()}'}
        
    
        response = requests.get(download_url, headers=headers,
                                auth=HTTPBasicAuth(self.user, self.password),
                                stream=True)
        # response_head = requests.head(download_url, headers=headers)
        # if response_head.status_code == 200:
        #     total_size = int(response_head.headers.get('Content-Length', 0))
        # else:
        #     # print(f"Failed to retrieve file size. Status code: {response_head.status_code}")
        #     total_size = 0
            
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
            print("ERROR, something went wrong")

# # Usage:
# downloader = S2_Peps_Download('S2B_MSIL1C_20210130T105209_N0209_R051_T31UCS_20210130T120256')
# summary = downloader.summary
