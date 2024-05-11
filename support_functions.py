# support_functions.py
import datetime
import re
from typing import Tuple
import os
import pathlib
import pandas as pd
from scipy.spatial.distance import cdist
import numpy as np
# =============================================================================
# Search and Download Handlers
# =============================================================================

def get_datetime_from_S3(product_name):
    pattern = "\d{4}\d{2}\d{2}T\d{2}\d{2}\d{2}"
    datetime_str = re.search(pattern, product_name).group(0)
    datetime_str = datetime.datetime.strptime(datetime_str, '%Y%m%dT%H%M%S')
    date_str = datetime_str.strftime("%Y-%m-%d")
    return datetime_str, date_str

def get_datetime_from_S2(product_name):
    pattern = "\d{4}\d{2}\d{2}T\d{2}\d{2}\d{2}"
    datetime_str = re.search(pattern, product_name).group(0)
    datetime_str = datetime.datetime.strptime(datetime_str, '%Y%m%dT%H%M%S')
    date_str = datetime_str.strftime("%Y-%m-%d")
    return datetime_str,date_str

def get_tile_from_S2(product_name):
    pattern = "_T\d{2}\w{3}_"
    tile = re.search(pattern, product_name).group(0)[2:-1]
    return tile


# This function checks if the file has been downloaded
def check_exist(outdir: str, download_name: str, download_size: float) -> Tuple[bool, bool, bool]:
    
    download_dir = os.path.join(outdir, download_name)
    if 'S2' in download_name: download_dir = f'{download_dir}.SAFE'
    
    download_zip_file = os.path.join(outdir, f'{download_name}.zip')
    exist_file, isfolder, iszip = False, False, False
    
    if os.path.isdir(download_dir):
        exist_size = get_directory_size(download_dir)
        if abs(exist_size - download_size) < 20:
            exist_file, isfolder = True, True
        else:
            exist_file, isfolder = False, False


    if os.path.isfile(download_zip_file):
        exist_size = get_file_size(download_zip_file)
        if abs(exist_size - download_size) < 10:
            exist_file, iszip = True, True
        else:
            exist_file, iszip = False, False
            
    return exist_file, isfolder, iszip

# This function gets the size of the downloaded file
def get_file_size(file_name: str) -> float:
    if os.path.isfile(file_name):
        return os.path.getsize(file_name) / (1024 * 1024)
    else:
        return float('NaN')

# This function gets the size of the directory
def get_directory_size(directory: str) -> float:
    total = 0
    for path, dirs, files in os.walk(directory):
        for f in files:
            fp = os.path.join(path, f)
            total += os.path.getsize(fp)
    return total / (1024 * 1024)

# This function ensures the path is correctly formatted
def handle_folder(path: str) -> str:
    path = pathlib.Path(path)
    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)
    return str(path)

def handle_credentials(credential_file):
        with open(credential_file, 'r') as file:
            credentials = file.read().split(',')
            user, password = credentials[0], credentials[1]
            return user, password

# =============================================================================
# Matchup Handlers
# =============================================================================
def handle_dataset(data):
    var_names = data.columns
    lower_var_names = data.columns.str.lower()

    # reformat lon, lat
    ind_lon = lower_var_names.isin(['lon', 'longitude'])
    ind_lat = lower_var_names.isin(['lat', 'latitude'])
    data = data.rename(columns={var_names[ind_lon][0]: 'lon', var_names[ind_lat][0]: 'lat'})

    # reformat date time     
    ind_datetime = lower_var_names.str.contains('date') & lower_var_names.str.contains('time')
    if ind_datetime.any():
        datetime_str = data[var_names[ind_datetime][0]].apply(lambda x: detect_datetime(x, '%Y-%m-%d %H:%M:%S') if isinstance(x, str) else x)


    if 'datetime_str' in locals():
        datetime_insitu = datetime_str
    # elif 'date_str' in locals() and 'time_str' in locals():
    else:  
        ind_date = lower_var_names.isin(['date', 'jour'])
        if ind_date.any():
            date_str = data[var_names[ind_date][0]].apply(lambda x: detect_datetime(x, '%Y-%m-%d'))

        ind_time = lower_var_names.isin(['time', 'heure'])
        if ind_time.any():
            time_str = data[var_names[ind_time][0]].apply(lambda x: detect_datetime(x, '%H:%M:%S'))
        datetime_insitu = date_str + ' ' + time_str
        

    data['insitu_datetime'] = datetime_insitu

    return data

def detect_datetime(datetime_string, format='%Y-%m-%d %H:%M:%S'):
    
    input_formats = [
        '%Y-%m-%dT%H:%M:%S', 
        '%Y-%m-%dT%H:%M', 
        '%Y-%m-%d', 
        '%m/%d/%Y %H:%M:%S', 
        '%m/%d/%Y %H:%M', 
        '%m/%d/%Y', 
        '%d/%m/%Y %H:%M:%S', 
        '%d/%m/%Y %H:%M', 
        '%d/%m/%Y', 
        '%H:%M:%S', 
        '%H:%M',
        '%d-%b-%y',
        '%d-%b-%Y',
    ]

    # Ensure datetime_string is a string
    
    
    if not isinstance(datetime_string, str):
        return None  # or some other default value

    for input_format in input_formats:
        try:
            dt = datetime.datetime.strptime(datetime_string, input_format)
            return dt.strftime(format)
        except ValueError:
            # print(f'Error: No valid date format found for value: {datetime_string}')
            continue

    return None

def extract_date_time_string(datetime_format):
    datetime_str = datetime_format.strftime('%Y-%m-%d %H:%M:%S').strip()
    date_str = datetime_format.strftime('%Y-%m-%d').strip()
    time_str = datetime_format.strftime('%H:%M:%S').strip()
    return datetime_str, date_str, time_str


def FindMinIndex(lon, lat, lon_s, lat_s):
    # Compute the distances
    distances = np.sqrt((lon_s - lon)**2 + (lat_s - lat)**2)

    I = np.unravel_index(np.argmin(distances, axis=None), distances.shape)
    M = distances[I]

    return M, I