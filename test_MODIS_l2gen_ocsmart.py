from tqdm import tqdm
import requests
import bz2, os
from MODIS.MODIS_NASA_Search import MODIS_NASA_Search
from ACprocessors import generateMODIS_L1B


ocsmart_path = '/mnt/hgfs/F/matchup_python/Python_Linux_v2.1'
os.chdir(ocsmart_path)
from OCSMART_MC_Manh import OCSMART_MC

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
            session = SessionWithHeaderRedirection(username, password)
            session.auth = (username, password)
            response = session.get(download_url)
            with open(outfile+'.bz2', 'wb') as file:
                file.write(response.content)
            with open(outfile+'.bz2', 'rb') as source, open(outfile, 'wb') as dest:
                dest.write(bz2.decompress(source.read()))
            os.remove(outfile+'.bz2')
        print(f'{ os.path.basename(outfile)}: download finished \n')  
        
        
product_info = MODIS_NASA_Search(Input=[-43.580553,-23.103924], start_date='2019-07-13',end_date = '2019-07-13')
product_info=product_info.product_summary
product_info=product_info.drop(product_info[product_info['day_night_flag']=='NIGHT'].index).reset_index(drop=True)
product_info=product_info.drop(product_info[product_info['day_night_flag']=='BOTH'].index).reset_index(drop=True)
token = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJlbWFpbF9hZGRyZXNzIjoibWFuaHRyYW5kdXkxOTkzQGdtYWlsLmNvbSIsImlzcyI6IkFQUyBPQXV0aDIgQXV0aGVudGljYXRvciIsImlhdCI6MTcwMDE0NTkzMywibmJmIjoxNzAwMTQ1OTMzLCJleHAiOjE4NTc4MjU5MzMsInVpZCI6Im1hbmh0cmFuZHV5IiwidG9rZW5DcmVhdG9yIjoibWFuaHRyYW5kdXkifQ.8QZeE_pm95FUG7Z9kg8CTQRX-qJbmFkrRhwWUb9wFgA'
outdir='/home/manh/Desktop/work/matchup_python/SatData/BayofRio/'
if not os.path.exists(outdir):
    os.mkdir(outdir)
geo_dir=os.path.join(outdir,'GEO')
l1b_dir=os.path.join(outdir,'L1B')
l2_dir=os.path.join(outdir,'L2')
anc_dir=os.path.join(outdir,'ANCILLARY')
if not os.path.exists(l2_dir):
    os.mkdir(l2_dir)
ocssw_dir='/home/manh/SeaDAS/ocssw'

import os
import subprocess

# Set the OCSSWROOT environment variable
os.environ['OCSSWROOT'] = ocssw_dir

# Build the command to source OCSSW_bash.env within a shell session
source_command = f'source {os.path.join(os.environ["OCSSWROOT"], "OCSSW_bash.env")} && env'

# Run the command using a shell
completed_process = subprocess.run(source_command, shell=True, executable="/bin/bash", stdout=subprocess.PIPE, text=True)

# Update the environment with the sourced variables
for line in completed_process.stdout.splitlines():
    key, value = line.split('=', 1)
    os.environ[key] = value
    
    

for i in range(product_info.shape[0]):
    download_url=product_info['product_link'][i]
    product_name=product_info['product_list'][i]
    outfile=os.path.join(outdir,product_name)
    download_file(outfile, download_url, token)
    l1b_file, geo_file,anc_file = generateMODIS_L1B(outfile,geo_dir,l1b_dir,anc_dir,ocssw_dir)
    l2_file_ocsmart=os.path.join(l2_dir,product_name.replace('L1A_LAC','L2_ocsmart'))
    l2_file_l2gen=os.path.join(l2_dir,product_name.replace('L1A_LAC','L2gen'))
    
    # Run OCSMART
    if not os.path.exists(l2_file_ocsmart):
        try:
            OCSMART_MC(L1B_file = l1b_file,
                       L2_file = l2_file_ocsmart,
                       GEOpath = geo_dir,
                       )
        except:
            pass
    else:
        print(l2_file_ocsmart+' already exists')
        pass
    
    # Run L2gen
    if not os.path.exists(l2_file_l2gen):
        try:
            if not os.path.isfile(anc_file):
                subprocess.call(f'python {ocssw_dir}/ocssw_src/src/scripts/getanc.py {outfile} -o {anc_file}', shell=True)
                
            subprocess.call('{}/bin/l2gen geofile={} ifile={} ofile={} par={} \
                            oformat="netcdf4" l2prod="Rrs_nnn Rrs_748 latitude longitude"'.format(
                           ocssw_dir,
                           geo_file,
                           l1b_file,
                           l2_file_l2gen,
                           anc_file,
                            ), 
                            shell=True)
        except:
            pass
    else:
        print(l2_file_l2gen+' already exists')
        pass

