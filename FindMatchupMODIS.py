from datetime import datetime
from MODIS.MODIS_NASA_Search import MODIS_NASA_Search
from typing import List
import os
import pandas as pd
import concurrent.futures
from tqdm import tqdm
import requests
import bz2
from shapely.geometry import Point, Polygon
# import concurrent.F

class FindMatchupMODIS:
    def __init__(self, location: List[List[float]], 
                 insitu_datetime: List[str], 
                 outdir: str = os.path.join(os.getcwd(), 'L1'),
                 time_difference: int = 3,
                 filter_night: bool = True,
                 level: int = 1,
                 email: str = [],
                 token: str = [],
                 l1b_dir: str = [],
                 download_if_l1bfile_exist: bool = False,
                 max_threads: int = os.cpu_count()):
        
        self.filter_night = filter_night
        self.l1b_dir = l1b_dir
        self.outdir = outdir
        self.email = email
        self.token = token
        self.insitu_datetime = insitu_datetime
        self.time_difference = time_difference
        self.location = pd.Series(location,index = self.insitu_datetime.index,name="location")
        self.max_threads = max_threads
        self.level = level
        self.download_if_l1bfile_exist = download_if_l1bfile_exist

        if not self.download_if_l1bfile_exist:
            self.l1b_dir = None
            
        self.find_matchup_MODIS()
        # download images
        # MODIS_NASA_Download(self.product_summary_df.product_id.tolist(),
        #                     outdir=self.outdir,
        #                     max_threads=self.max_threads,
        #                     email=self.email,
        #                     token=self.token)
        
        # query_products = self.product_summary_df['product_list']
        
        
        
        # downloaded_products = glob.glob(os.path.join(self.outdir,'MYD01*.hdf'))
        # downloaded_products = [os.path.basename(file) for file in downloaded_products]
        # for ind, query_product in query_products.items():
        #     try:
        #         query_product = query_product.split('.')
        #         query_product[-2:] = []
        #         query_product = '.'.join(query_product)
        #         for downloaded_product in downloaded_products:
        #             if query_product in downloaded_product:
        #                 self.product_summary_df.loc[ind, 'product_list'] = downloaded_product
        #     except Exception:
        #         continue
        
                  
            

    def find_matchup_MODIS(self):
        self.product_summary_df = []
        
        if self.max_threads > 1:
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_threads) as executor:
                futures = [executor.submit(self.search, loc, insitu_dt) for loc, insitu_dt in zip(self.location.items(), self.insitu_datetime.items())]
                
                for future in futures:
                    self.product_summary_df.append(future.result())
                    # if product_summary is not None:
                    #     product_summary_df = pd.concat([product_summary_df, product_summary])
        else:
            for loc, insitu_dt in zip(self.location.items(), self.insitu_datetime.items()):
                self.product_summary_df.append(self.search(loc, insitu_dt))
                # if product_summary is not None:
                #     product_summary_df = pd.concat([product_summary_df, product_summary])
        if self.product_summary_df:
            self.product_summary_df = pd.concat(self.product_summary_df).sort_index()
        

    def search(self,location, insitu_datetime):
        
        try:
            date_insitu = str(datetime.strptime(insitu_datetime[1], '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d'))
            product_info = MODIS_NASA_Search(Input=location[1], start_date=date_insitu,end_date = date_insitu,level=self.level)
            product_info= product_info.product_summary
            if self.filter_night:
                product_info=product_info[~product_info['day_night_flag'].str.contains('NIGHT')].reset_index()
            
            insitu_datetime=datetime.strptime(str(insitu_datetime[1]),'%Y-%m-%d %H:%M:%S')
            time_diff = []
            for i in range(product_info.shape[0]):
                satellite_datetime=datetime.strptime(str(product_info['satellite_datetime'][i]),'%Y-%m-%d %H:%M:%S')
                time_diff.append(abs(insitu_datetime-satellite_datetime).total_seconds() / 3600.0)
            product_info['timediff_hour']=time_diff
            product_info = product_info.sort_values(by='timediff_hour')
            outfile = os.path.join(self.outdir,product_info['product_list'].values[0])
            loc = Point(location[1])
            # print(product_info['timediff_hour'].values[0])
            # Check if the time difference and spatial position meet the criteria 
            for i in range(0,len(product_info)):
               # print(i)
               polygon_coords_str = product_info['polygons'].values[i][0][0]  # Access the first element of the inner list to get the string
               polygon_coords_pairs = polygon_coords_str.split(' ')  # Now you can split the string into individual coordinates
               polygon_coords = [(float(polygon_coords_pairs[i+1]), float(polygon_coords_pairs[i])) for i in range(0, len(polygon_coords_pairs), 2)]

                # Create a polygon
               polygon = Polygon(polygon_coords)
                
               if (product_info['timediff_hour'].values[i]<=self.time_difference) and (polygon.contains(loc)):
               # if (product_info['timediff_hour'].values[i]<=self.time_difference):
                    product_info=product_info.iloc[[i]]
                    product_info.index = [location[0]]
                    product_name=product_info['product_list'].values[i].split(".")[0]
                    if not self.l1b_dir:
                        download_file(outfile,product_info['product_link'].values[i],self.token)
                    else:
                        if not os.path.exists(os.path.join(self.l1b_dir,product_name+'.L1B_LAC')):
                            download_file(outfile,product_info['product_link'].values[i],self.token)
                        else:
                            print(product_name+'.L1B_LAC already exists, skipping ... \n')
                        
                        
                    return product_info
               else:
                   product_info = pd.DataFrame(index=[location[0]])
                   return product_info
        except Exception as error:
            print('Error when searching for image:', error)
            product_info = pd.DataFrame(index=[location[0]])
            return product_info
        
        
    
def download_file(outfile, download_url, token):
    response = requests.get(download_url, headers={"Authorization": f"Bearer {token}"}, stream=True)
    
    response_head = requests.head(download_url, headers={"Authorization": f"Bearer {token}"})
    if response_head.status_code == 200:
        total_size = int(response_head.headers.get('Content-Length', 0))
    else:
        # print(f"Failed to retrieve file size. Status code: {response_head.status_code}")
        total_size = 0
    # outfile = 'D:/work/Codes/Eumetsat/matchup_python/SatData/L1/MODIS/MYD01.A2021021.1600.061.2023316170850.hdf'
   
    # if os.path.exists(outfile) and os.path.getsize(outfile) == total_size:
    if os.path.exists(outfile):
        print(f'{ os.path.basename(outfile)}: already exists \n')
    else: 
        print(f'downloading: { os.path.basename(outfile)} \n')
        if total_size:
            
            block_size = 1024  # You can adjust this as needed
            progress_bar = tqdm(total=total_size, unit='iB', unit_scale=True, position=0, leave=True)
            # progress_bar = tqdm(total=total_size, unit='iB', unit_scale=True,position=counter.increment(), leave=True)
            
            with open(outfile, 'wb') as file:
                for data in response.iter_content(block_size):
                    progress_bar.update(len(data))
                    file.write(data)
            progress_bar.close()
        else:
            username = "manhtranduy"
            password = "!Iunamy93"
            # session = requests.Session()
            session = SessionWithHeaderRedirection(username, password)
            session.auth = (username, password)
            response = session.get(download_url)
            if ('.nc' in outfile):
                with open(outfile, 'wb') as file:
                    file.write(response.content)
            else:
                with open(outfile+'.bz2', 'wb') as file:
                    file.write(response.content)
                with open(outfile+'.bz2', 'rb') as source, open(outfile, 'wb') as dest:
                    dest.write(bz2.decompress(source.read()))
                os.remove(outfile+'.bz2')
        print(f'{ os.path.basename(outfile)}: download finished \n')  
  

class SessionWithHeaderRedirection(requests.Session):

    AUTH_HOST = 'urs.earthdata.nasa.gov'

    def __init__(self, username, password):

        super().__init__()

        self.auth = (username, password)
   # Overrides from the library to keep headers when redirected to or from
   # the NASA auth host.
    def rebuild_auth(self, prepared_request, response):

        headers = prepared_request.headers
        url = prepared_request.url

        if 'Authorization' in headers:
            original_parsed = requests.utils.urlparse(response.request.url)
            redirect_parsed = requests.utils.urlparse(url)
            if (original_parsed.hostname != redirect_parsed.hostname) and \
                    redirect_parsed.hostname != self.AUTH_HOST and \
                    original_parsed.hostname != self.AUTH_HOST:
                del headers['Authorization']
        return
