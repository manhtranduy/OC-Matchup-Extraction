import os
import pandas as pd
from support_functions import handle_dataset, FindMinIndex
from FindMatchupS3 import FindMatchupS3
from FindMatchupMODIS import FindMatchupMODIS
from FindMatchupS2 import FindMatchupS2
import numpy as np
from glob import glob
# from typing import Union, List
import netCDF4
from ACprocessors import RunPOLYMER_msi_subset, RunPOLYMER_olci_subset, RunEumetsat_olci_subset, RunOCSMART_msi_subset
from ACprocessors import RunPOLYMER_modis_subset, generateMODIS_L1B, RunOCSMART_modis_subset, RunL2GEN_modis_subset,RunNASA_modis_subset, generateMODIS_L1C
from ACprocessors import RunC2RCC_msi_subset, RunC2RCC_olci_subset
from ACprocessors import RunACOLITE_msi_subset, RunACOLITE_olci_subset
from scipy.stats import mode
import datetime

class EXmatchup:
    def __init__(self, Data_input: pd.DataFrame, 
                 sensor: str = 'olci', 
                 run_AC: str = 'acolite',
                 make_out_dir: bool =False,
                 l1_server: str = 'eumetsat', 
                 l1_dir: str = os.path.join(os.getcwd(), 'L1'), 
                 l2_dir: str = os.path.join(os.getcwd(), 'L2'), 
                 l2_mini_dir: str = os.path.join(os.getcwd(), 'L2_mini'), 
                 geo_dir: str = None,
                 anc_dir: str = None,
                 l1b_dir: str = None,
                 l1c_dir: str = None,
                 ocssw_dir: str ='/home/manh/SeaDAS/ocssw',
                 block: int = None, 
                 window: int = 3, 
                 time_difference: int =3,
                 unzip: bool = True, 
                 removeZipfile: bool = True, 
                 email: str = None,
                 token: str = None, 
                 # '''Go to https://ladsweb.modaps.eosdis.nasa.gov/ for token''' for downloading modis
                 aer_opt :int = -99,
                #  0: White aerosol extrapolation.
                #  -1: Multi-scattering with 2-band model selection
                #  -2: Multi-scattering with 2-band, RH-based model selection and
                #  iterative NIR correction
                #  -3: Multi-scattering with 2-band model selection
                #  and iterative NIR correction
                #  -4: Multi-scattering with fixed model pair
                #  (requires aermodmin, aermodmax, aermodrat specification)
                #  -5: Multi-scattering with fixed model pair
                #  and iterative NIR correction
                #  (requires aermodmin, aermodmax, aermodrat specification)
                #  -6: Multi-scattering with fixed angstrom
                #  (requires aer_angstrom specification)
                #  -7: Multi-scattering with fixed angstrom
                #  and iterative NIR correction
                #  (requires aer_angstrom specification)
                #  -8: Multi-scattering with fixed aerosol optical thickness
                #  (requires taua specification)
                #  -9: Multi-scattering with 2-band model selection using Wang et al. 2009
                #  to switch between SWIR and NIR. (MODIS only, requires aer_swir_short,
                #  aer_swir_long, aer_wave_short, aer_wave_long)
                # -10: Multi-scattering with MUMM correction
                #  and MUMM NIR calculation
                # -17: Multi-scattering epsilon, RH-based model selection
                #  and iterative NIR correction
                # -18: Spectral Matching of aerosols reflectance
                #  and iterative NIR correction
                # -19: Multi-scattering epsilon (linear), RH-based model selection
                #  and iterative NIR correction
                 max_threads: int = os.cpu_count()):
        self.Data_input = Data_input
        self.sensor = sensor
        self.run_AC = run_AC
        self.l1_server = l1_server
        self.l1_dir = l1_dir
        self.l2_dir = l2_dir
        self.l2_mini_dir = l2_mini_dir
        self.block = block
        self.window = window
        self.unzip = unzip
        self.removeZipfile = removeZipfile
        self.email = email
        self.token = token
        self.time_difference = time_difference
        self.max_threads = max_threads
        self.geo_dir = geo_dir
        self.anc_dir = anc_dir
        self.l1b_dir = l1b_dir
        self.l1c_dir = l1c_dir
        self.ocssw_dir = ocssw_dir
        self.aer_opt = aer_opt
        
        if self.max_threads<=0:
            self.max_threads = os.cpu_count()
        
     
        # Check compatibility
        if run_AC == 'theia' and sensor == 'olci':
            raise ValueError('Theia products are only available for Sentinel-2/MSI')
        elif run_AC == 'eumetsat' and sensor == 'msi':
            raise ValueError('Eumetsat products are only available for Sentinel-3/OLCI')
            
        if self.sensor == 'olci' and self.l1_server == 'peps':
            raise ValueError('Sentinel-3/OLCI products are not supported on Peps server')
        elif self.sensor == 'msi' and self.l1_server == 'eumetsat':
            raise ValueError('Sentinel-2/MSI products are not supported on Eumetsat server')
            
        # Handle Directories 
        if not self.geo_dir:
            self.geo_dir = os.path.join(self.l1_dir, 'GEO')
        if not self.l1b_dir:
            self.l1b_dir = os.path.join(self.l1_dir, 'L1B')
        if not self.l1c_dir:
            self.l1c_dir = os.path.join(self.l1_dir, 'L1C')
        if not self.anc_dir:
            self.anc_dir = os.path.join(self.l1_dir, 'ANCILLARY')
        
        if make_out_dir:
            self.l2_dir = os.path.join(l2_dir,self.run_AC)
            self.l1_dir = os.path.join(l1_dir,self.sensor)
        
        if not os.path.isdir(self.l2_dir):
            os.makedirs(self.l2_dir, exist_ok=True)
        if not os.path.isdir(self.l1_dir):
            os.makedirs(self.l1_dir, exist_ok=True)
        
        
        if self.block > len(Data_input):
            self.block = len(Data_input)
        
        if not self.block:
            self.block = self.Data_input.shape[0]
        
        # Initialize variables
        if self.sensor == 'olci':
            if run_AC == 'polymer':
                self.bands = ['400', '412', '443', '490', '510', '560', '620', '665','674', '681', '709', '754', '779', '865', '885', '1020']
            elif run_AC == 'acolite':
                self.bands = ['400', '412', '443', '490', '510', '560', '620', '665', '674', '682', '709', '754', '768', '779', '865', '884', '899', '1016']
            elif run_AC in ['c2rcc', 'eumetsat']:
                self.bands = ['400', '412', '443', '490', '510', '560', '620', '665', '673', '681', '709', '753', '779', '865', '885', '1020']
                self.band_vars = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '16', '17', '18', '21']
        elif self.sensor == 'msi':
            self.bands = ['443', '490', '560', '665', '705', '740', '783', '865']
            self.band_vars = ['B1', 'B2', 'B3', 'B4', 'B5', 'B6', 'B7', 'B8A']
        elif self.sensor == 'modis':
            
            if run_AC == 'nasa':
                self.bands = ['412', '443', '469', '488', '531', '547', '551', '645', '667', '678']
            else:
                self.bands = ['412', '443', '488', '531', '551', '667','678', '748']
        params = {}
        
# =============================================================================
#         # Handle dataset
# =============================================================================
        self.Data_input = handle_dataset(self.Data_input)
        # self.Data_input['insitu_datetime']
# =============================================================================
#         # Filter Data
# =============================================================================
        # if self.filter_data:
        # Get indices before dropping NA values
        non_na_indices = self.Data_input.dropna(subset=['lon', 'lat', 'insitu_datetime']).index
        
        # Apply sensor-specific filters and get the resulting indices
        if self.sensor == 'msi':
            date_filter = (pd.to_datetime(self.Data_input['insitu_datetime'],
                                          errors='coerce', 
                                          infer_datetime_format=True) > pd.to_datetime('2015-06-23')) & \
                          (pd.to_datetime(self.Data_input['insitu_datetime'],
                                          errors='coerce', 
                                          infer_datetime_format=True) <= pd.to_datetime('today'))
        elif self.sensor == 'olci':
            date_filter = (pd.to_datetime(self.Data_input['insitu_datetime'],
                                          errors='coerce', 
                                          infer_datetime_format=True) > pd.to_datetime('2016-02-16')) & \
                          (pd.to_datetime(self.Data_input['insitu_datetime']) <= pd.to_datetime('today'))
        elif self.sensor == 'modis':
            date_filter = (pd.to_datetime(self.Data_input['insitu_datetime'], 
                                          errors='coerce', 
                                          infer_datetime_format=True) > pd.to_datetime('2002-07-04')) & \
                          (pd.to_datetime(self.Data_input['insitu_datetime'], 
                                          errors='coerce', 
                                          infer_datetime_format=True) <= pd.to_datetime('today'))
                  
        filtered_indices = non_na_indices[date_filter[non_na_indices]]
        
        filtered_data = self.Data_input.loc[filtered_indices]
        excluded_data = self.Data_input.loc[~self.Data_input.index.isin(filtered_indices)]
        self.Data_input = filtered_data
            
                
                
  
# =============================================================================
#         # Setup blocks
# =============================================================================
        multiplier = self.Data_input.shape[0] // self.block
        offset = self.Data_input.shape[0] - multiplier * self.block

        m_index = multiplier + 1 if offset != 0 else multiplier

        self.Data_output = []

        for m in range(0,m_index+1):
            Data = []
            # Block Processing
            if m == m_index and offset == 0:
                Data = self.Data_input[(m - 1) * self.block:]
            elif m == m_index and offset != 0:
                Data = self.Data_input[m * self.block - self.block:]
            else:
                Data = self.Data_input[m * self.block - self.block : m * self.block]
        
            # Find matchup images and download
            if self.sensor == 'msi':
                # config inputs
                if self.run_AC == 'theia':
                    outdir = self.l2_dir
                    processingLevel = 2
                else:
                    outdir = self.l1_dir
                    processingLevel = 1
        
                # find matchup
                FM_S2 = FindMatchupS2(location = Data[['lon', 'lat']].values.tolist(), 
                                                  insitu_datetime = Data['insitu_datetime'], 
                                                  outdir=outdir, processingLevel=processingLevel, 
                                                  unzip=self.unzip, removeZipfile=self.removeZipfile, 
                                                  server=self.l1_server, max_threads=self.max_threads)
                matchup_products = FM_S2.find_matchup_S2()
        
            elif self.sensor == 'olci':
                # config inputs
                if self.run_AC == 'eumetsat':
                    outdir = self.l2_dir
                    processingLevel = 2
                else:
                    outdir = self.l1_dir
                    processingLevel = 1
                
                # find matchup
                FM_S3 = FindMatchupS3(location = Data[['lon', 'lat']].values.tolist(), 
                                      insitu_datetime = Data['insitu_datetime'], 
                                      outdir=outdir, processingLevel=processingLevel, 
                                      unzip=self.unzip, removeZipfile=self.removeZipfile, 
                                      server=self.l1_server, max_threads=self.max_threads)
                matchup_products = FM_S3.find_matchup_S3()
                
            elif self.sensor == 'modis':
                # find matchup
                if self.run_AC=='l2gen' or self.run_AC=='ocsmart':
                    FM_MODIS = FindMatchupMODIS(location = Data[['lon', 'lat']].values.tolist(), 
                                          insitu_datetime = Data['insitu_datetime'], 
                                          filter_night=True,
                                          l1b_dir=self.l1b_dir,
                                          outdir=self.l1_dir,
                                          email=self.email,
                                          token=self.token,
                                          level=1,
                                          max_threads=self.max_threads,
                                          time_difference = self.time_difference)
                elif self.run_AC=='nasa':
                    FM_MODIS = FindMatchupMODIS(location = Data[['lon', 'lat']].values.tolist(), 
                                          insitu_datetime = Data['insitu_datetime'], 
                                          filter_night=True,
                                          l1b_dir=self.l1b_dir,
                                          outdir=self.l1_dir,
                                          email=self.email,
                                          token=self.token,
                                          level=2,
                                          max_threads=self.max_threads,
                                          time_difference = self.time_difference)
                        
                matchup_products = FM_MODIS.product_summary_df
                

                
        
            # Merge with the found product info
            if isinstance(matchup_products, pd.DataFrame) and not(matchup_products.empty):
                # Data.reset_index(drop=True, inplace=True)
                # matchup_products.reset_index(drop=True, inplace=True)
                Data = pd.concat([Data, matchup_products],axis=1)
            else:
                self.Data_output.append(Data)
                continue
            
            
# =============================================================================
#             # Run Atmospheric correction
# =============================================================================
            # Remove duplicated variables
            try:
                for k in range(len(self.bands)):
                    if f'Rrs{self.bands[k]}_avg' in Data.columns:
                        Data = Data.drop(columns=f'Rrs{self.bands[k]}_avg')
                    if f'Rrs{self.bands[k]}_med' in Data.columns:
                        Data = Data.drop(columns=f'Rrs{self.bands[k]}_med')
                    if f'Nvalid_Rrs{self.bands[k]}' in Data.columns:
                        Data = Data.drop(columns=f'Nvalid_Rrs{self.bands[k]}')
                    if f'CV_Rrs{self.bands[k]}' in Data.columns:
                        Data = Data.drop(columns=f'CV_Rrs{self.bands[k]}')
            except Exception:
                print('No duplicated variable found')
            
            flag = []
            failed_flag= []
            
            
            # print(f'datashpe= {Data.shape[0]}')
            # L2 Process block
            for i in Data.index:
                print(f'Processing: {Data.index.get_loc(i)+1}/{Data.shape[0]} of {m}/{multiplier} blocks ...')
                if not 'product_list' in Data or pd.isna(Data['product_list'].loc[i]):
                    continue
        
# =============================================================================
#               # Process Image
# =============================================================================
                # search for the path of the l1 image
                if self.sensor == 'msi':
                    # l1_image = glob(os.path.join(self.l1_dir, Data['product_list'].loc[i]+'*', '**', '*B1*'), recursive=True)[0]
                    l1_image = os.path.join(self.l1_dir, Data['product_list'].loc[i]+'.SAFE')
                    # l1_image = os.path.dirname(l1_image)
                elif self.sensor == 'olci':
                    if self.run_AC=='eumetsat':
                        l1_image = glob(os.path.join(self.l2_dir, Data['product_list'].loc[i], 'geo_coordinates.nc'), recursive=True)[0]
                        l1_image = os.path.dirname(l1_image)
                    else: 
                        l1_image = glob(os.path.join(self.l1_dir, Data['product_list'].loc[i], '**', 'geo_coordinates.nc'), recursive=True)[0]
                        l1_image = os.path.dirname(l1_image)
                elif self.sensor == 'modis':
                    l1_image = os.path.join(self.l1_dir, Data['product_list'].loc[i])
            
                # Generate Mini files
                print(f"Generating mini files for (lon={Data['lon'].loc[i]}, lat={Data['lat'].loc[i]})")
                # try:
                l2_mini, failed_flag = self.processImage(l1_image, Data['lon'].loc[i], Data['lat'].loc[i])
                # except:
                #     failed_flag = True
                #     continue
                
                ##
                # Skip if the processing is failed
                if failed_flag or not os.path.isfile(l2_mini):
                    continue
            
                # Read L2 mini data
                if run_AC == 'polymer':
                    lat_s = netCDF4.Dataset(l2_mini).variables['latitude'][:]
                    mx, my = lat_s.shape
                    lon_s = netCDF4.Dataset(l2_mini).variables['longitude'][:]
                    l2_flags = netCDF4.Dataset(l2_mini).variables['bitmask'][:]
                    Rrs = {}
                    for band in self.bands:
                        Rrs[band] = netCDF4.Dataset(l2_mini).variables[f'Rw{band}'][:]/np.pi
                elif run_AC == 'c2rcc':
                    lat_s = netCDF4.Dataset(l2_mini).variables['lat'][:]
                    mx, my = lat_s.shape
                    lon_s = netCDF4.Dataset(l2_mini).variables['lon'][:]
                    l2_flags = netCDF4.Dataset(l2_mini).variables['c2rcc_flags'][:]
                    Rrs = {}
                    for band in self.bands:
                        Rrs[band] = netCDF4.Dataset(l2_mini).variables[f'rrs_{band}'][:]
                        # Data[f'Rrs{self.bands[k]}'][Data[f'Rrs{self.bands[k]}'] <= 0] = np.nan
                elif run_AC == 'acolite':
                    lat_s = netCDF4.Dataset(l2_mini).variables['lat'][:]
                    mx, my = lat_s.shape
                    lon_s = netCDF4.Dataset(l2_mini).variables['lon'][:]
                    l2_flags = netCDF4.Dataset(l2_mini).variables['l2_flags'][:]
                    
                    # Open the NetCDF file
                    dataset = netCDF4.Dataset(l2_mini)
                    
                    # Extract the variable names from the dataset
                    rhow_vars = [var_name for var_name in dataset.variables if var_name.startswith('rhow_')]
                    
                    
                    # Rest of the code remains the same
                    bands_image = np.array([int(var.replace('rhow_', '')) for var in rhow_vars])
                    bands_table = np.array([int(band) for band in self.bands])
                    
                    
                    Rrs = {}
                    for band_im in bands_image:
                        subtracted_list = [abs(band_im - int(x)) for x in self.bands]
                        
                        if any(num < 5 for num in subtracted_list):
                            min_index = subtracted_list.index(min(subtracted_list))
                            band=self.bands[min_index]
                            Rrs[band] = dataset.variables[f'rhow_{band_im}'][:].filled(np.nan) / np.pi
                        else:
                            Rrs[band] = np.full_like(lat_s, np.nan)
        
                    
                    # Don't forget to close the NetCDF file when done
                    dataset.close()
                    
                elif run_AC == 'eumetsat':
                    lat_s = netCDF4.Dataset(l2_mini).variables['latitude'][:]
                    mx, my = lat_s.shape
                    lon_s = netCDF4.Dataset(l2_mini).variables['longitude'][:]
                    l2_flags = netCDF4.Dataset(l2_mini).variables['flag'][:]
                    Rrs = {}
                    for band in self.bands:
                        Rrs[band] = netCDF4.Dataset(l2_mini).variables[f'Rrs_{band}'][:]
                    
                    params['chl_nn'] = np.ma.filled(netCDF4.Dataset(l2_mini).variables[f'chl_nn'][:].astype(np.float64), np.nan)
                    params['tsm_nn'] = np.ma.filled(netCDF4.Dataset(l2_mini).variables[f'tsm_nn'][:].astype(np.float64), np.nan)
                    params['chl_oc4me'] = np.ma.filled(netCDF4.Dataset(l2_mini).variables[f'chl_oc4me'][:].astype(np.float64), np.nan)
                
                elif run_AC == 'l2gen':
                    lat_s = netCDF4.Dataset(l2_mini).groups['navigation_data'].variables['latitude'][:]
                    mx, my = lat_s.shape
                    lon_s = netCDF4.Dataset(l2_mini).groups['navigation_data'].variables['longitude'][:]
                    l2_flags = netCDF4.Dataset(l2_mini).groups['geophysical_data'].variables['l2_flags'][:]
                    import re
                    wl=[]
                    with netCDF4.Dataset(l2_mini, 'r') as nc_file:
                        for var_name in nc_file.groups['geophysical_data'].variables:
                            match = re.findall(r'\d+', var_name)
                            if match:
                                wl.append(int(match[0]))
                                
                    Rrs = {}
                    for band in self.bands:
                        try:
                            if np.min(np.abs(int(band) - np.array(wl)))<= 5:
                                ind=np.argmin(np.abs(int(band) - np.array(wl)))
                                # print(band,f'Rrs_{wl[ind]}nm')
                                Rrs[band] = netCDF4.Dataset(l2_mini).groups['geophysical_data'].variables[f'Rrs_{wl[ind]}'][:]
                        except:
                            pass
                        
                elif run_AC == 'nasa':
                    lat_s = netCDF4.Dataset(l2_mini).variables['latitude'][:]
                    mx, my = lat_s.shape
                    lon_s = netCDF4.Dataset(l2_mini).variables['longitude'][:]
                    l2_flags = netCDF4.Dataset(l2_mini).variables['flag'][:]
                    Rrs = {}
                    for band in self.bands:
                        Rrs[band] = netCDF4.Dataset(l2_mini).variables[f'Rrs_{band}'][:]
                    
                    params['Chl_nasa'] = np.ma.filled(netCDF4.Dataset(l2_mini).variables[f'Chl_nasa'][:].astype(np.float64), np.nan)
                elif run_AC == 'ocsmart':
                    lat_s = netCDF4.Dataset(l2_mini).variables['Latitude'][:]
                    mx, my = lat_s.shape
                    lon_s = netCDF4.Dataset(l2_mini).variables['Longitude'][:]
                    l2_flags = netCDF4.Dataset(l2_mini).variables['L2_flags'][:]
                    import re
                    wl=[]
                    # Open the NetCDF file
                    with netCDF4.Dataset(l2_mini, 'r') as nc_file:
                        for var_name in nc_file.groups['Rrs'].variables:
                            match = re.findall(r'\d+', var_name)
                            if match:
                                wl.append(int(match[0]))
        
                    Rrs = {}
                    for band in self.bands:
                        try:
                            if np.min(np.abs(int(band) - np.array(wl)))<= 5:
                                ind=np.argmin(np.abs(int(band) - np.array(wl)))
                                # print(band,f'Rrs_{wl[ind]}nm')
                                Rrs[band] = netCDF4.Dataset(l2_mini).groups['Rrs'].variables[f'Rrs_{wl[ind]}nm'][:]
                        except:
                            pass
                        
                    # if Data.loc[i,'ID']=='GID_237':
                    #     a=1
                    #     pass
                    
                    
                    
                            
                # Find Min Index
                M, I = FindMinIndex(Data['lon'].loc[i], Data['lat'].loc[i], lon_s, lat_s)
            
                # Skip if the image does not cover the in-situ location
                if M > 1:
                    print('Out of scene')
                    for band in self.bands:
                        Data.loc[i, f'Rrs{band}_avg'] = np.nan
                        Data.loc[i, f'Rrs{band}_med'] = np.nan
                        Data.loc[i, f'CV_Rrs{band}'] = np.nan
                        Data.loc[i, f'Nvalid_Rrs{band}'] = np.nan
                    Data.loc[i, f'flag_{self.run_AC}'] = np.nan
                    for param in params:
                        Data.loc[i, f'{param}_avg'] = np.nan
                        Data.loc[i, f'{param}_med'] = np.nan
                    continue
            
                # Extract the index of the surrounding pixels with defined window
                row, col = I
                # window_locs = np.zeros((self.window, self.window), dtype=int) # Initialize the matrix to store the indices
                dx = [x - (self.window-1) // 2 for x in range(self.window)]
                dy = dx
            
                window_locs = []
                for x in range(self.window):
                    for y in range(self.window):
                        try:
                            # Calculate the coordinates and append to the list window_locs
                            window_locs.append((row + dy[x], col + dx[y]))
                        except Exception as e:
                            window_locs.append(())
                            print(f"Error occurred: {e}")
                            # Skip this iteration and continue with the next iteration
                            continue
                
                # for x in range(self.window):
                #     for y in range(self.window):
                #         N[x, y] = np.ravel_multi_index([row + dy[x], col + dx[y]], (mx, my))
                #         try:
                #             # N[x, y] = np.ravel_multi_index([row + dy[x], col + dx[y]], (mx, my))
                #         except:
                #             continue
            
                Rrs_tmp = {band: [] for band in self.bands}
                params_tmp = {param: [] for param in params}
                flags = []
            
                for window_loc in window_locs:
                    # Process flags
                    try:
                        flag_value = l2_flags[window_loc] if window_loc else np.nan
                    except (IndexError, KeyError):  # Replace with the specific exceptions you're expecting
                        flag_value = np.nan
                    flags.append(flag_value)
            
                    # Process Rrs
                    for band in self.bands:
                        try:
                            value = Rrs[band][window_loc] if window_loc else np.nan
                        except (IndexError, KeyError):  # Replace with the specific exceptions you're expecting
                            value = np.nan
                        Rrs_tmp[band].append(value)
                        
                    for param in params:
                        try:
                            value = params[param][window_loc] if window_loc else np.nan
                        except (IndexError, KeyError):  # Replace with the specific exceptions you're expecting
                            value = np.nan
                        params_tmp[param].append(value)
                        
            
                flag = mode(flags, nan_policy='omit', keepdims=True).mode
            
                # Calculate mean, coefficient of variation, number of valid pixels
                try:
                    for band in self.bands:
                        Rrs_band = Rrs_tmp[band]
                        if len(Rrs_band) > 0:
                            Data.loc[i, f'Rrs{band}_avg'] = np.nanmean(Rrs_band)
                            Data.loc[i, f'Rrs{band}_med'] = np.nanmedian(Rrs_band)
                            Data.loc[i, f'CV_Rrs{band}'] = np.nanstd(Rrs_band) / np.nanmean(Rrs_band)
                            Data.loc[i, f'Nvalid_Rrs{band}'] = np.count_nonzero(~np.isnan(Rrs_band))
                        else:
                            # If the array is empty, set the corresponding values to NaN or any appropriate value
                            Data.loc[i, f'Rrs{band}_avg'] = np.nan
                            Data.loc[i, f'Rrs{band}_med'] = np.nan
                            Data.loc[i, f'CV_Rrs{band}'] = np.nan
                            Data.loc[i, f'Nvalid_Rrs{band}'] = 0
                            
                    for param in params:
                        Data.loc[i, f'{param}_avg'] = np.nanmean(params_tmp[param])
                        Data.loc[i, f'{param}_med'] = np.nanmedian(params_tmp[param])
                        Data.loc[i, f'CV_{param}'] = np.nanstd(params_tmp[param]) / np.nanmean(params_tmp[param])

                    # Assuming you have a valid value for 'flag' at index i
                    Data.loc[i, f'flag_{self.run_AC}'] = flag
                except:
                    continue
                    
            # print(f'dataShape= {Data.shape[0]}')
# =============================================================================
#                 # End processing
# =============================================================================
# =============================================================================
# Return values
# =============================================================================
            self.Data_output.append(Data) 
            date_save = sorted([x for x in Data['insitu_datetime'].tolist() if isinstance(x, str) and x is not None])
            date_save=[datetime.datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S').date().strftime('%Y_%m_%d') for dt_str in date_save]
                    
        # date_save=[datetime.datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S').date().strftime('%Y_%m_%d') for dt_str in date_save]
            if m == m_index and offset != 0:
                csv_name = f"BDB_offset{offset}_{date_save[0]}_{date_save[-1]}_{self.sensor}_{self.run_AC}.csv"
            else:
                csv_name = f"BDB{m}_block{self.block}_{date_save[0]}_{date_save[-1]}_{self.sensor}_{self.run_AC}.csv"
            csv_options = {
                            'index': True,       # Exclude the index column in the CSV
                            'mode': 'w',          # Overwrite the file if it already exists
                            'encoding': 'utf-8',  # Use UTF-8 encoding for compatibility
                            'quotechar': '"'      # Ensure fields containing special characters are quoted
                            }
            Data.to_csv(os.path.join(self.l2_dir,csv_name), **csv_options)
            
        self.Data_output=pd.concat(self.Data_output).sort_index()
        # Return to the initial table
        for column in set(self.Data_output.columns) - set(excluded_data.columns):
            excluded_data[column] = pd.NA
        self.Data_output = pd.concat([self.Data_output, excluded_data]).sort_index()
        date_save = sorted([x for x in self.Data_output['insitu_datetime'].tolist() if isinstance(x, str) and x is not None])
        date_save=[datetime.datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S').date().strftime('%Y_%m_%d') for dt_str in date_save]
        csv_name= f"MDB_{date_save[0]}_{date_save[-1]}_{self.sensor}_{self.run_AC}.csv"
        csv_options = {
                        'index': True,       # Exclude the index column in the CSV
                        'mode': 'w',          # Overwrite the file if it already exists
                        'encoding': 'utf-8',  # Use UTF-8 encoding for compatibility
                        'quotechar': '"'      # Ensure fields containing special characters are quoted
                        }
        self.Data_output.to_csv(os.path.join(self.l2_dir,csv_name), **csv_options)


    def processImage(self,l1_image, lon, lat):
        
        if self.sensor == 'modis':
            if self.run_AC == 'nasa':
                l2_mini, failed_flag = RunNASA_modis_subset(l1_image, lon, lat, 
                                                             self.l2_dir)
            else:
                l1b_file, geo_file, anc_file = generateMODIS_L1B(l1_image,self.geo_dir,self.l1b_dir,self.anc_dir,self.ocssw_dir)
                if self.run_AC == 'polymer':
                    l1c_file = generateMODIS_L1C(l1b_file,geo_file,self.l1c_dir,self.ocssw_dir)
                    l2_mini, failed_flag = RunPOLYMER_modis_subset(l1c_file, lon, lat, self.l2_dir)
                if self.run_AC == 'ocsmart':
                    l2_mini, failed_flag = RunOCSMART_modis_subset(l1b_file, lon, lat, self.l2_dir, geo_file)
                if self.run_AC == 'l2gen':
                    l2_mini, failed_flag = RunL2GEN_modis_subset(l1b_file,geo_file,anc_file, lon, lat, 
                                                                 self.l2_dir,self.ocssw_dir,self.aer_opt)
    
                # remove unnecessary files        
                files = glob('AQUA_MODIS*')
                for file_path in files:
                   if os.path.isfile(file_path):
                       try:
                           os.remove(file_path)
                           print(f"Deleted: {file_path}")
                       except Exception as e:
                           print(f"Error deleting {file_path}: {e}")
        elif self.sensor == 'olci':
            if self.run_AC == 'polymer':
                l2_mini, failed_flag = RunPOLYMER_olci_subset(l1_image, lon, lat, self.l2_dir)
            if self.run_AC == 'acolite':
                l2_mini, failed_flag = RunACOLITE_olci_subset(l1_image, lon, lat, self.l2_dir)
            if self.run_AC == 'eumetsat':   
                l2_mini, failed_flag = RunEumetsat_olci_subset(l1_image, lon, lat, self.l2_dir)
        elif self.sensor == 'msi':
            if self.run_AC == 'polymer':
                l2_mini, failed_flag = RunPOLYMER_msi_subset(l1_image, lon, lat, self.l2_dir)
            if self.run_AC == 'acolite':
                l2_mini, failed_flag = RunACOLITE_msi_subset(l1_image, lon, lat, self.l2_dir)
            if self.run_AC == 'ocsmart':   
                l2_mini, failed_flag = RunOCSMART_msi_subset(l1_image, lon, lat, self.l2_dir)
            
        
        return l2_mini, failed_flag
