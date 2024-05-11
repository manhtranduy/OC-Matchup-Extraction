from datetime import datetime
from S2.S2_Peps_Search import S2_Peps_Search
from S2.S2_Peps_Download import S2_Peps_Download 
from S2.S2_SciCopernicus_Search import S2_SciCopernicus_Search 
from S2.S2_SciCopernicus_Download import S2_SciCopernicus_Download 
from typing import List
import os
import pandas as pd
import concurrent.futures

class FindMatchupS2:
    def __init__(self, location: List[List[float]], 
                 insitu_datetime: List[str], 
                 outdir: str = os.getcwd(), 
                 multithreading: bool = False, 
                 server: str = 'peps', 
                 satellite: str = 'S2', 
                 processingLevel: int = 1, 
                 unzip: bool = True, 
                 removeZipfile: bool = True,
                 max_threads: int = os.cpu_count()):

        self.insitu_datetime = insitu_datetime
        self.location = pd.Series(location,index = self.insitu_datetime.index,name="location")
        self.outdir = outdir
        self.multithreading = multithreading
        self.server = server
        self.satellite = satellite
        self.processingLevel = processingLevel
        self.unzip = unzip
        self.removeZipfile = removeZipfile
        self.max_threads = max_threads
        
        if self.server == 'peps':
            self.search_download_func_pairs = (S2_Peps_Search, S2_Peps_Download)
        elif self.server == 'copernicus':
            self.search_download_func_pairs = (S2_SciCopernicus_Search, S2_SciCopernicus_Download)
        else:
            raise ValueError('Invalid server name')
            
        # if isinstance(self.insitu_datetime,str):
        #     self.insitu_datetime=[self.insitu_datetime]
            
        # if sum(isinstance(x, list) for x in self.location) == 0:
        #     self.location=[self.location] 
            

    def find_matchup_S2(self):
        
        search_func, download_func = self.search_download_func_pairs
        
        product_summary_df = []
        
        if self.max_threads > 1:
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_threads) as executor:
                futures = [executor.submit(self.search_and_download, search_func, download_func, loc, insitu_dt) for loc, insitu_dt in zip(self.location.items(), self.insitu_datetime.items())]
                
                for future in futures:
                    product_summary_df.append(future.result())
                    # if product_summary is not None:
                    #     product_summary_df = pd.concat([product_summary_df, product_summary])
        else:
            for loc, insitu_dt in zip(self.location.items(), self.insitu_datetime.items()):
                product_summary_df.append(self.search_and_download(search_func, download_func, loc, insitu_dt))
                # if product_summary is not None:
                #     product_summary_df = pd.concat([product_summary_df, product_summary])
        
        if product_summary_df:
            product_summary_df = pd.concat(product_summary_df).sort_index()
        
        return product_summary_df
    

    def search_and_download(self, search_func, download_func, location, insitu_datetime):
        # print(insitu_datetime)
     
        try:
            date_insitu = str(datetime.strptime(insitu_datetime[1], '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d'))
            product_info = search_func(Input=location[1], start_date=date_insitu,end_date = date_insitu)
            product_info=product_info.product_summary
        except:
            product_summary = pd.DataFrame(index=[location[0]])
            return product_summary
        
        if not product_info.empty:
            insitu_datetime=datetime.strptime(str(insitu_datetime[1]),'%Y-%m-%d %H:%M:%S')
            time_diff = []
            for i in range(product_info.shape[0]):
                satellite_datetime=datetime.strptime(str(product_info['satellite_datetime'].iloc[i]),'%Y-%m-%d %H:%M:%S')
                time_diff.append(abs(insitu_datetime-satellite_datetime).total_seconds() / 3600.0)
            product_info['timediff_hour']=time_diff
            product_info = product_info.sort_values(by='timediff_hour')
            product_info=product_info[:1]
            # print(product_info)
            try:
                product_download = download_func(product_info['product_list'],
                                                outdir=self.outdir,
                                                unzip=self.unzip,
                                                removeZipfile=self.removeZipfile)
                product_summary = product_download.summary
                product_summary['timediff_hour'] = product_info['timediff_hour']
                product_summary.index = [location[0]]
            except:
                 product_summary = pd.DataFrame(index=[location[0]])
        else:
            product_summary = pd.DataFrame(index=[location[0]])

        return product_summary


# import pandas as pd
# from support_functions import handle_dataset

# df = pd.read_excel('Gloria_LOG_Brazil_filtered.csv')
# df = handle_dataset(df)
# location = df[['lon', 'lat']].values.tolist()

# insitu_datetime = df['insitu_datetime'].tolist()


# f_MC = FindMatchupS2(location = location[56],insitu_datetime=str(insitu_datetime[56]))
# f_MC = FindMatchupS2(location = location[51:60],
#                       insitu_datetime=insitu_datetime[51:60],
#                       max_threads=1)
# summary = f_MC.find_matchup_S2()

