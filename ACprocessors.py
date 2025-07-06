from support_functions import FindMinIndex
import os
import numpy as np
from netCDF4 import Dataset
from support_functions import get_datetime_from_S2, get_datetime_from_S3
from glob import glob
import sys
import numpy as np
import subprocess
import h5py

acolite_path = 'D:/work/Codes/OC-Matchup-Extraction/acolite-main'
if not acolite_path in sys.path:
    sys.path.append(acolite_path)

# polymer_path = 'F:/work/matchup_python/polymer-v4.16.1'
polymer_path = '/home/manh/Desktop/work/Codes/Eumetsat/matchup_python/polymer-master'
if not polymer_path in sys.path:
    sys.path.append(polymer_path)

# ocsmart_path = 'F:/work/matchup_python/OCSMART'
ocsmart_path = '/mnt/hgfs/F/matchup_python/Python_Linux_v2.1'
if not ocsmart_path in sys.path:
    sys.path.append(ocsmart_path)

# for dirpath, dirnames, filenames in os.walk(acolite_path):
#     for dirname in dirnames:
#         full_path = os.path.abspath(os.path.join(dirpath, dirname))
#         if not full_path in sys.path:
#             sys.path.append(full_path)

# =============================================================================
# # MODIS
# =============================================================================
def generateMODIS_L1B(l1_image, geo_dir, l1b_dir,anc_dir, ocssw_dir):
    # Extract the base filename without extension
    base = os.path.splitext(os.path.basename(l1_image))[0]
    geo_file = os.path.join(geo_dir, f"{base}.GEO")
    l1b_file = os.path.join(l1b_dir, f"{base}.L1B_LAC")
    anc_file = os.path.join(anc_dir, f"{base}.anc")
 
    if not os.path.isdir(geo_dir):
        os.makedirs(geo_dir, exist_ok=True)
        
    if not os.path.isdir(l1b_dir):
        os.makedirs(l1b_dir, exist_ok=True)
    
    if not os.path.isdir(anc_dir):
        os.makedirs(anc_dir, exist_ok=True)
        
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


    if not os.path.isfile(anc_file):
        subprocess.call(f'python {ocssw_dir}/ocssw_src/src/scripts/getanc.py {l1_image} -o {anc_file} --refreshDB', shell=True)
        # subprocess.call(f'{ocssw_dir}/bin/getanc {l1_image} -o {anc_file}', shell=True)
    
    
    if not os.path.isfile(l1_image):
        return l1b_file, geo_file, anc_file 
    

    # Generate GEO file
    if not os.path.exists(geo_file):
        print("GENERATING NEW GEOFILE...")
        subprocess.call(f'{ocssw_dir}/bin/modis_GEO {l1_image} -o {geo_file} --refreshDB', shell=True)

    # Generate L1B file
    if not os.path.exists(l1b_file):
        print("GENERATING NEW L1B...")
        subprocess.call(f'{ocssw_dir}/bin/modis_L1B {l1_image} {geo_file} -o {l1b_file}', shell=True)

    # Generate L1C file
    # if not os.path.exists(l1cfile):
    #     print("GENERATING L1C...")
    #     subprocess.call(f'{ocssw_dir}/bin/l2gen ifile={l1b_file} geo_file={geo_file} ofile1={l1cfile} oformat="netcdf4" l2prod="rhot_nnn polcor_nnn sena senz sola solz latitude longitude"', shell=True)

    return l1b_file, geo_file, anc_file

def generateMODIS_L1C(l1b_file,geo_file,l1c_dir,ocssw_dir):
    # Extract the base filename without extension
    base = os.path.splitext(os.path.basename(l1b_file))[0].replace('.L1B_LAC', '')
    l1cfile = os.path.join(l1c_dir, f"{base}.L1C.nc")
    
    if os.path.exists(l1cfile) or (not os.path.isfile(l1b_file)):
        return l1cfile
    
        
    if not os.path.isdir(l1c_dir):
        os.makedirs(l1c_dir, exist_ok=True)

    
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


    # Generate L1C file
    if not os.path.exists(l1cfile):
        print("GENERATING L1C...")
        subprocess.call(f'{ocssw_dir}/bin/l2gen ifile={l1b_file} geofile={geo_file} ofile1={l1cfile} oformat="netcdf4" l2prod="rhot_nnn polcor_nnn sena senz sola solz latitude longitude"', shell=True)

    return l1cfile  

def RunL2GEN_modis(l1b_file,geo_file, anc_file, l2_dir,ocssw_dir,aer_opt):
    l1_name = os.path.basename(os.path.normpath(l1b_file))
    l2_name = f'{l1_name}L2_SeaDAS.nc'
    l2_name = l2_name.replace('L1B_LAC', '')
    l2_mini = os.path.join(l2_dir,l2_name)
    if not os.path.isfile(l1b_file):
        failed_flag = True
        return l2_mini, failed_flag
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
        
    # return the l2 mini file name and the failed flag  
    # 
    failed_flag = False
    if os.path.isfile(l2_mini):
        print(f'{l2_mini} is existed/n')
        return l2_mini, failed_flag
    
    # Read coordinates
    print(f'Creating l2file {l2_name} /n')
    subprocess.call('{}/bin/l2gen geofile={} ifile={} ofile={} par={} \
                    oformat="netcdf4" l2prod="Rrs_nnn Rrs_748 latitude longitude"'.format(
                   ocssw_dir,
                   geo_file,
                   l1b_file,
                   l2_mini,
                   anc_file,
                    ), 
                    shell=True)
    return l2_mini, failed_flag
    

def RunL2GEN_modis_subset(l1b_file,geo_file, anc_file, lon, lat, l2_dir,ocssw_dir,aer_opt):
    l1_name = os.path.basename(os.path.normpath(l1b_file))
    l2_name = f'{l1_name}_{lon:.4f}_{lat:.4f}_l2gen.nc'
    l2_mini = os.path.join(l2_dir,l2_name)
    l2_mini = l2_mini.replace('L1B_LAC.hdf_', '')
    if not os.path.isfile(l1b_file):
        failed_flag = True
        return l2_mini, failed_flag
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
        
    # return the l2 mini file name and the failed flag  
    # 
    failed_flag = False
    if os.path.isfile(l2_mini):
        print(f'{l2_mini} is existed/n')
        return l2_mini, failed_flag
    
    # Read coordinates
    print(f'Creating minifile {l2_name} /n')
    # subprocess.call('{}/bin/l2gen geofile={} ifile={} ofile={} north={} south={} east={} west={} aer_opt={} glint_opt=1 maskland=0 maskhilt=0 maskcloud=0 aer_wave_short=1240 aer_wave_long=2130  oformat="netcdf4" l2prod="Rrs_nnn Rrs_748 latitude longitude"'.format(
    #                ocssw_dir,
    #                geo_file,
    #                l1b_file,
    #                l2_mini,
    #                lat+0.1,
    #                lat-0.1,
    #                lon+0.1,
    #                lon-0.1,
    #                aer_opt,
    #                 ), 
    #                 shell=True)
    
    subprocess.call('{}/bin/l2gen geofile={} ifile={} ofile={} north={} south={} east={} west={} par={} \
                    oformat="netcdf4" l2prod="Rrs_nnn Rrs_748 latitude longitude"'.format(
                   ocssw_dir,
                   geo_file,
                   l1b_file,
                   l2_mini,
                   lat+0.1,
                   lat-0.1,
                   lon+0.1,
                   lon-0.1,
                   anc_file,
                    ), 
                    shell=True)
    return l2_mini, failed_flag
    
        
    

# =============================================================================
# # POLYMER
# =============================================================================

# MSI
def RunPOLYMER_msi_subset(l1_image, lon, lat, l2_dir):
    os.chdir(polymer_path)
    from polymer.main import run_atm_corr
    from polymer.level2_nc import Level2_NETCDF
    from polymer.level1_msi import Level1_MSI
    from glob import glob
    # return the l2 mini file name and the failed flag  
    # 
    failed_flag = False
    resolution = 10
    l1_name = os.path.basename(os.path.normpath(l1_image))
    l1_name.replace('SAFE', '')
    l2_mini = os.path.join(l2_dir,f'{l1_name}_{lon}_{lat}_polymer.nc')
    
    if os.path.isfile(l2_mini):
        print(f'{l2_mini} is existed/n')
        return l2_mini, failed_flag
    
    # Read coordinates
    try:
        lon_s,lat_s, totalheight, totalwidth = read_coord_msi(l1_image, resolution)
        
        M, I = FindMinIndex(lon, lat, lon_s, lat_s)
        row, col = I
        srow = row - 30
        scol = col - 30
        erow = row + 30
        ecol = col + 30
    except:
        print('Reading coordinates failed /n')
        failed_flag = True
        return l2_mini, failed_flag


    # Polymer processing
    l1_dir = glob(l1_image+'/GRANULE/'+'L1C*')[0]
    
    try:
        # print(f'Processing {l2_mini}/n')
        print(f'Creating minifile {l1_name}_{lon:.4f}_{lat:.4f}_polymer.nc /n')
        run_atm_corr(Level1_MSI(l1_dir,resolution=resolution,sline=srow, eline=erow, scol=scol, ecol=ecol),
             Level2_NETCDF(l2_mini,          
                           overwrite=True)
             ,multiprocessing=1)
        # print(f'Processing {l2_mini} Done!/n')
        
    except:
        print('Polymer processing failed /n')
        failed_flag = True
        return l2_mini,  failed_flag
    return l2_mini,  failed_flag

def RunPOLYMER_msi(l1_image, l2_dir,res=60):
    os.chdir(polymer_path)
    from polymer.main import run_atm_corr
    from polymer.level2_nc import Level2_NETCDF
    from polymer.level1_msi import Level1_MSI
    from glob import glob
    # return the l2 mini file name and the failed flag  
    # 
    failed_flag = False
    resolution = res
    l1_name = os.path.basename(os.path.normpath(l1_image))
    l1_name.replace('SAFE', '')
    l2_file = os.path.join(l2_dir,f'{l1_name}_polymer{resolution}.nc')
    
    if os.path.isfile(l2_file):
        print(f'{l2_file} is existed/n')
        return l2_file, failed_flag
    
    # Read coordinates


    # Polymer processing
    l1_dir = glob(l1_image+'/GRANULE/'+'L1C*')[0]
    
    try:
        run_atm_corr(Level1_MSI(l1_dir,resolution=resolution),
             Level2_NETCDF(l2_file,          
                           overwrite=True)
             ,multiprocessing=1)
        # print(f'Processing {l2_mini} Done!/n')
        
    except:
        print('Polymer processing failed /n')
        failed_flag = True
        return l2_file,  failed_flag
    return l2_file,  failed_flag

# OLCI
def RunPOLYMER_olci_subset(l1_image, lon, lat, l2_dir):
    os.chdir(polymer_path)
    from polymer.main import run_atm_corr
    from polymer.level2_nc import Level2_NETCDF
    from polymer.level1_olci import Level1_OLCI
    # return the l2 mini file name and the failed flag  
    # 
    failed_flag = False
    l1_name = os.path.basename(os.path.normpath(l1_image))
    l2_mini = os.path.join(l2_dir,f'{l1_name}_{lon:.4f}_{lat:.4f}_polymer.nc')
    if os.path.isfile(l2_mini):
        print(f'{l2_mini} is existed/n')
        return l2_mini, failed_flag
    
    # Read coordinates
    try:
        coord_file = os.path.join(l1_image, 'geo_coordinates.nc')
        nc_fid = Dataset(coord_file, 'r')
        
        lon_s = nc_fid.variables['longitude'][:]
        lat_s = nc_fid.variables['latitude'][:]
        totalheight, totalwidth = lon_s.shape
        
        M, I = FindMinIndex(lon, lat, lon_s, lat_s)
        row, col = I
        srow = row - 20
        scol = col - 20
        erow = row + 20
        ecol = col + 20
    except:
        print('Reading coordinates failed /n')
        failed_flag = True
        return l2_mini, failed_flag
    

    # Polymer processing   
    try:
        # print(f'Processing {l2_mini}/n')
        print(f'Creating minifile {l1_name}_{lon:.4f}_{lat:.4f}_polymer.nc /n')
        run_atm_corr(Level1_OLCI(l1_image,sline=srow, eline=erow, scol=scol, ecol=ecol),
                     Level2_NETCDF(l2_mini,          
                                   overwrite=True)
                     ,multiprocessing=-1)
        
        # run_atm_corr(Level1_OLCI(l1_image),
        #              Level2_NETCDF(l2_mini,          
        #                            overwrite=True)
        #              ,multiprocessing=-1)

        
    except:
        print('Polymer processing failed /n')
        failed_flag = True
        return l2_mini,  failed_flag
    return l2_mini,  failed_flag

# MODIS
def RunPOLYMER_modis_subset(l1_image, lon, lat, l2_dir):
    os.chdir(polymer_path)

    from polymer.main import run_atm_corr
    from polymer.level2_nc import Level2_NETCDF
    from polymer.level1_nasa import Level1_MODIS
    # return the l2 mini file name and the failed flag  
    # 
    failed_flag = False
    l1_name = os.path.basename(os.path.normpath(l1_image))
    l2_mini = os.path.join(l2_dir,f'{l1_name}_{lon:.4f}_{lat:.4f}_polymer.nc')
    if os.path.isfile(l2_mini) or (not os.path.isfile(l1_image)):
        print(f'{l2_mini} is existed/n')
        return l2_mini, failed_flag
    
    # Read coordinates
    try:
        nc_fid = Dataset(l1_image, 'r')
        navigation_data_group = nc_fid.groups['navigation_data']
        
        lon_s = navigation_data_group.variables['longitude'][:]
        lat_s = navigation_data_group.variables['latitude'][:]
        totalheight, totalwidth = lon_s.shape
        
        M, I = FindMinIndex(lon, lat, lon_s, lat_s)
        row, col = I
        srow = row - 20
        scol = col - 20
        erow = row + 20
        ecol = col + 20
    except:
        print('Reading coordinates failed /n')
        failed_flag = True
        return l2_mini, failed_flag
    

    # Polymer processing   
# try:
    # print(f'Processing {l2_mini}/n')
    print(f'Creating minifile {l1_name}_{lon:.4f}_{lat:.4f}_polymer.nc /n')
    run_atm_corr(Level1_MODIS(l1_image,sline=srow, eline=erow, scol=scol, ecol=ecol),
                 Level2_NETCDF(l2_mini,          
                               overwrite=True)
                 ,multiprocessing=-1)
    
    # run_atm_corr(Level1_OLCI(l1_image),
    #              Level2_NETCDF(l2_mini,          
    #                            overwrite=True)
    #              ,multiprocessing=-1)

    
# except:
#     print('Polymer processing failed /n')
#     failed_flag = True
#     return l2_mini,  failed_flag
    return l2_mini,  failed_flag


    
# Read coordinates MSI
def read_coord_msi(l1_image, resolution):
    # ref: level1_msi.py (polymer)
    
    from lxml import objectify
    import pyproj
    from glob import glob
    import os
    import numpy as np
    
    print('Reading MSI coordinates, this might take time ...')
    resolution=str(resolution)
    # Parse XML file
    meta_file = list(glob(os.path.join(l1_image, 'GRANULE', '*', 'MTD_TL.xml')))[0]
    xmlroot = objectify.parse(meta_file).getroot()
    
    geocoding = xmlroot.Geometric_Info.find('Tile_Geocoding')
    code = geocoding.find('HORIZONTAL_CS_CODE').text
    # Fix: Remove the '+init=' prefix to address the FutureWarning
    proj = pyproj.Proj(code)
    
    # read image size for current resolution
    for e in geocoding.findall('Size'):
        if e.attrib['resolution'] == resolution:
            totalheight = int(e.find('NROWS').text)
            totalwidth = int(e.find('NCOLS').text)
            break
        
    # lookup position in the UTM grid
    for e in geocoding.findall('Geoposition'):
        if e.attrib['resolution'] == resolution:
            ULX = int(e.find('ULX').text)
            ULY = int(e.find('ULY').text)
            XDIM = int(e.find('XDIM').text)
            YDIM = int(e.find('YDIM').text)
    X, Y = np.meshgrid(ULX + XDIM*np.arange(totalheight), 
                    ULY + YDIM*np.arange(totalwidth))
    
    lon, lat = (proj(X, Y, inverse=True))
    return lon, lat, totalheight, totalwidth

# =============================================================================
# ACOLITE
# =============================================================================

# MSI
def RunACOLITE_msi_subset(l1_image, lon, lat, l2_dir):
    os.chdir(acolite_path)
    import acolite as ac
    
    # return the l2 mini file name and the failed flag
    # 
    failed_flag = False
    resolution = 10
    l1_name = os.path.basename(os.path.normpath(l1_image))
    l1_name.replace('SAFE', '')
    l2_mini = os.path.join(l2_dir,f'{l1_name}_{lon}_{lat}_acolite.nc')
    _,date_str=get_datetime_from_S2(l1_name)
    date_str=date_str.replace('-','_')
    if os.path.isfile(l2_mini):
        print(f'{l2_mini} is existed/n')
        return l2_mini,  failed_flag
    
    # Read coordinates

    lon_s,lat_s, totalheight, totalwidth = read_coord_msi(l1_image, resolution)
    
    M, I = FindMinIndex(lon, lat, lon_s, lat_s)
    if M>0.01:
        print('extracted point is out of scene')
        failed_flag = True
        return l2_mini,  failed_flag
    # row, col = np.unravel_index(I, (totalheight, totalwidth))
    row, col = I
    
    # Check bounds before accessing surrounding positions
    if row - 10 < 0 or row + 10 >= totalheight or col - 10 < 0 or col + 10 >= totalwidth:
        print('extracted position is too close to edge or outside image bounds')
        failed_flag = True
        return l2_mini, failed_flag

        
    N_pos = lat_s[row-10,col]
    S_pos = lat_s[row+10,col]
    W_pos = lon_s[row,col-10]
    E_pos = lon_s[row,col+10]
    limit = np.array([S_pos,W_pos,N_pos,E_pos]).flatten().tolist()
    # except:
    #     print('Reading coordinates failed /n')
    #     failed_flag = True
    #     return l2_mini, failed_flag
    
    # Define a dictionary for settings  
    acolite_settings = {"output":l2_dir, "s2_target_res": resolution,
                        "aerosol_correction":"dark_spectrum",
                        "l2w_parameters":["rhow_*"], 
                        "l2w_mask_water_parameters": False, 
                        "l2w_mask":True,
                        "limit":limit,
                        "dsf_residual_glint_correction":True,
                        "glint_write_rhog_all":False,
                        "rgb_rhot": False, "rgb_rhos": False,
                        "oli_orange_band": False,
                        "l1r_nc_delete": True, "l2r_nc_delete": True,
                        "gains": True}
    
    # Acolite processing

    try:
        if os.path.isfile(l2_mini):
            print(f'{l2_mini} is existed/n')
            return l2_mini,  failed_flag
        else:  
            print(f'Creating minifile {l1_name}_{lon:.4f}_{lat:.4f}_acolite.nc /n')
            ac.acolite.acolite_run(acolite_settings,inputfile=l1_image)
        print(f'Processing {l2_mini} Done!/n')
        
        L1R_file = glob(f'{l2_dir}/*{date_str}*L1R*')[0]
        L2R_file = glob(f'{l2_dir}/*{date_str}*L2R*')[0]
        L2W_file = glob(f'{l2_dir}/*{date_str}*L2W*')[0] 
        txt_files = glob(f'{l2_dir}/*.txt')
        
        os.remove(L1R_file)
        os.remove(L2R_file)
        os.rename(L2W_file, l2_mini)
        for txt_file in txt_files:
            os.remove(txt_file)
        
    except:
        print('Acolite processing failed /n')
        failed_flag = True
        return l2_mini,  failed_flag
    return l2_mini,  failed_flag

# OLCI
def RunACOLITE_olci_subset(l1_image, lon, lat, l2_dir):
    from acolite import acolite as ac    
    # return the l2 mini file name and the failed flag
    # 
    failed_flag = False
    l1_name = os.path.basename(os.path.normpath(l1_image))
    l2_mini = os.path.join(l2_dir,f'{l1_name}_{lon:.4f}_{lat:.4f}_acolite.nc')
    
    if os.path.isfile(l2_mini):
        print(f'{l2_mini} already exists/n')
        return l2_mini,  failed_flag
    

    _,date_str=get_datetime_from_S3(l1_name)
    date_str=date_str.replace('-','_')

    
    # Read coordinates
    try:
        coord_file = os.path.join(l1_image, 'geo_coordinates.nc')
        nc_fid = Dataset(coord_file, 'r')
        
        # lon_s = np.transpose(nc_fid.variables['longitude'][:])
        # lat_s = np.transpose(nc_fid.variables['latitude'][:])
        lon_s = nc_fid.variables['longitude'][:]
        lat_s = nc_fid.variables['latitude'][:]
        totalheight, totalwidth = lon_s.shape        
        
        M, I = FindMinIndex(lon, lat, lon_s, lat_s)
        if M > 1:
            print('Out of scene /n')
            failed_flag = True
            return l2_mini, failed_flag
        # row, col = np.unravel_index(I, (totalheight, totalwidth))
        row,col = I
        N_pos = lat_s[row-20,col]
        S_pos = lat_s[row+20,col]
        W_pos = lon_s[row,col-20]
        E_pos = lon_s[row,col+20]
        limit = np.array([S_pos,W_pos,N_pos,E_pos]).flatten().tolist()
    except:
        print('Reading coordinates failed /n')
        failed_flag = True
        return l2_mini, failed_flag
    
    # Define a dictionary for settings  
    acolite_settings = {"output":l2_dir,
                        "aerosol_correction":"dark_spectrum",
                        "l2w_parameters":["rhow_*"], 
                        "l2w_mask_water_parameters": False, 
                        "l2w_mask":True,
                        "limit":limit,
                        "dsf_residual_glint_correction":True,
                        "glint_write_rhog_all":False,
                        "rgb_rhot": False, "rgb_rhos": False,
                        "oli_orange_band": False,
                        "l1r_nc_delete": True, "l2r_nc_delete": True,
                        "gains": True}
    # Acolite processing

    try: 
        print(f'Creating minifile {l1_name}_{lon:.4f}_{lat:.4f}_acolite.nc /n')
        ac.acolite_run(acolite_settings,inputfile=l1_image)
        # print(f'Processing {l2_mini} Done!/n')
        
        L1R_file = glob(f'{l2_dir}/*{date_str}*L1R*')[0]
        L2R_file = glob(f'{l2_dir}/*{date_str}*L2R*')[0]
        L2W_file = glob(f'{l2_dir}/*{date_str}*L2W*')[0] 
        txt_files = glob(f'{l2_dir}/*.txt')
        
        os.remove(L1R_file)
        os.remove(L2R_file)
        os.rename(L2W_file, l2_mini)
        for txt_file in txt_files:
            os.remove(txt_file)
    except:
        print('Acolite processing failed /n')
        failed_flag = True
        return l2_mini,  failed_flag
    
    return l2_mini,  failed_flag


def RunEumetsat_olci_subset(l2_image, lon, lat, l2_dir): 
    '''return the l2 mini file name and the failure flag
    '''
    #
    bands = ['400', '412', '443', '490', '510', '560', '620', '665', '673', '681', '709', '753', '779', '865', '885', '1020']
    band_vars = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '16', '17', '18', '21']
    
    failed_flag = False
    l2_name = os.path.basename(os.path.normpath(l2_image))
    l2_mini = os.path.join(l2_dir,f'{l2_name}_{lon:.4f}_{lat:.4f}_eumetsat.nc')
    
    if os.path.isfile(l2_mini):
        print(f'{l2_name}_{lon:.4f}_{lat:.4f}_eumetsat.nc already exists/n')
        return l2_mini,  failed_flag
    

    _,date_str=get_datetime_from_S3(l2_image)
    date_str=date_str.replace('-','_')

    
    # Read coordinates
    try:
        coord_file = os.path.join(l2_image, 'geo_coordinates.nc')
        nc_fid = Dataset(coord_file, 'r')
        
        # lon_s = np.transpose(nc_fid.variables['longitude'][:])
        # lat_s = np.transpose(nc_fid.variables['latitude'][:])
        lon_s = nc_fid.variables['longitude'][:]
        lat_s = nc_fid.variables['latitude'][:]
        nc_fid.close()
        totalheight, totalwidth = lon_s.shape        
        
        M, I = FindMinIndex(lon, lat, lon_s, lat_s)
        if M > 1:
            print('Out of scene /n')
            failed_flag = True
            return l2_mini, failed_flag
        # row, col = np.unravel_index(I, (totalheight, totalwidth))
        row,col = I
    except:
        print('Reading coordinates failed /n')
        failed_flag = True
        return l2_mini, failed_flag
    

    try: 
        
        # create mini file
        mini_nc_file = Dataset(l2_mini, 'w', format='NETCDF4')
        
        # Define dimensions
        mini_nc_file.createDimension('x', 3)
        mini_nc_file.createDimension('y', 3)
        
        # Read Rrs
        Rrs = {}
        for band_var,band in zip(band_vars,bands):
            band_name = 'Oa' + f"{int(band_var):02}" + '_reflectance'
            ncfile = os.path.join(l2_image, band_name + '.nc')
            Rrs[band] = np.ma.filled(Dataset(ncfile, 'r').variables[band_name][row-1:row+2,col-1:col+2].astype(np.float64), np.nan)
            mini_nc_file.createVariable('Rrs_' + band, np.float64, ('x', 'y'))[:] = Rrs[band]
    
        # Read other parameters    
        chl_nn_file = os.path.join(l2_image, 'chl_nn' + '.nc')
        chl_nn = np.ma.filled(Dataset(chl_nn_file, 'r').variables['CHL_NN'][row-1:row+2,col-1:col+2].astype(np.float64), np.nan)    
        mini_nc_file.createVariable('chl_nn', np.float64, ('x', 'y'))[:] = chl_nn
        
        
        chl_oc4me_file = os.path.join(l2_image, 'chl_oc4me' + '.nc')
        chl_oc4me = np.ma.filled(Dataset(chl_oc4me_file, 'r').variables['CHL_OC4ME'][row-1:row+2,col-1:col+2].astype(np.float64), np.nan) 
        mini_nc_file.createVariable('chl_oc4me', np.float64, ('x', 'y'))[:] = chl_oc4me
        
        tsm_nn_file = os.path.join(l2_image, 'tsm_nn' + '.nc')
        tsm_nn = np.ma.filled(Dataset(tsm_nn_file, 'r').variables['TSM_NN'][row-1:row+2,col-1:col+2].astype(np.float64), np.nan) 
        mini_nc_file.createVariable('tsm_nn', np.float64, ('x', 'y'))[:] = tsm_nn
        
        flag_file = os.path.join(l2_image, 'wqsf' + '.nc')
        flag = np.ma.filled(Dataset(flag_file, 'r').variables['WQSF'][row-1:row+2,col-1:col+2].astype(np.uint64), np.nan) 
        mini_nc_file.createVariable('flag', np.uint64, ('x', 'y'))[:] = flag
    
        lon_s = np.ma.filled(lon_s[row-1:row+2,col-1:col+2].astype(np.float64), np.nan)    
        mini_nc_file.createVariable('longitude', np.float64, ('x', 'y'))[:] = lon_s
        
        lat_s = np.ma.filled(lat_s[row-1:row+2,col-1:col+2].astype(np.float64), np.nan)    
        mini_nc_file.createVariable('latitude', np.float64, ('x', 'y'))[:] = lat_s
        
        mini_nc_file.close()
        
        print(f'Created mini file {l2_name}_{lon:.4f}_{lat:.4f}_eumetsat.nc /n')
    # print(f'Processing {l2_mini} Done!/n')
    
    except:
        print('Creating {l2_mini} failed /n')
        failed_flag = True
        mini_nc_file.close()
        os.remove(l2_mini)
        return l2_mini,  failed_flag
    
    return l2_mini,  failed_flag

def RunNASA_modis_subset(l2_image, lon, lat, l2_dir): 
    '''return the l2 mini file name and the failure flag
    '''
    #
    bands = ['412', '443', '469', '488', '531', '547', '555', '645', '667', '678']
 
    
    failed_flag = False
    l2_name = os.path.basename(os.path.normpath(l2_image))
    l2_mini = os.path.join(l2_dir,f'{l2_name}_{lon:.4f}_{lat:.4f}_nasa.nc')
    
    if os.path.isfile(l2_mini):
        print(f'{l2_name}_{lon:.4f}_{lat:.4f}_nasa.nc already exists/n')
        return l2_mini,  failed_flag
    
    
    # Read coordinates
    try:
        nc_fid = Dataset(l2_image, 'r')
        
        # lon_s = np.transpose(nc_fid.variables['longitude'][:])
        # lat_s = np.transpose(nc_fid.variables['latitude'][:])
        lon_s = nc_fid.groups['navigation_data'].variables['longitude'][:]
        lat_s = nc_fid.groups['navigation_data'].variables['latitude'][:]
        nc_fid.close()
        totalheight, totalwidth = lon_s.shape        
        
        M, I = FindMinIndex(lon, lat, lon_s, lat_s)
        if M > 1:
            print('Out of scene /n')
            failed_flag = True
            return l2_mini, failed_flag
        # row, col = np.unravel_index(I, (totalheight, totalwidth))
        row,col = I
    except:
        print('Reading coordinates failed /n')
        failed_flag = True
        return l2_mini, failed_flag
    

    try: 
        # create mini file
        mini_nc_file = Dataset(l2_mini, 'w', format='NETCDF4')
        
        # Define dimensions
        mini_nc_file.createDimension('x', 3)
        mini_nc_file.createDimension('y', 3)
        
        # Read Rrs
        Rrs = {}
        for band in bands:  
            band_name = f"Rrs_{band}"
            Rrs[band] = np.ma.filled(Dataset(l2_image, 'r').groups['geophysical_data'].variables[band_name][row-1:row+2,col-1:col+2].astype(np.float64), np.nan)
            mini_nc_file.createVariable('Rrs_' + band, np.float64, ('x', 'y'))[:] = Rrs[band]
    
        # Read other parameters    
        flag = np.ma.filled(Dataset(l2_image, 'r').groups['geophysical_data'].variables['l2_flags'][row-1:row+2,col-1:col+2].astype(np.int64), np.nan)
        mini_nc_file.createVariable('flag', np.float64, ('x', 'y'))[:] = flag
        
        chl = np.ma.filled(Dataset(l2_image, 'r').groups['geophysical_data'].variables['chlor_a'][row-1:row+2,col-1:col+2].astype(np.float64), np.nan)
        mini_nc_file.createVariable('Chl_nasa', np.float64, ('x', 'y'))[:] = chl
        
        lon_s = np.ma.filled(Dataset(l2_image, 'r').groups['navigation_data'].variables['longitude'][row-1:row+2,col-1:col+2].astype(np.float64), np.nan)
        mini_nc_file.createVariable('longitude', np.float64, ('x', 'y'))[:] = lon_s
        
        lat_s = np.ma.filled(Dataset(l2_image, 'r').groups['navigation_data'].variables['latitude'][row-1:row+2,col-1:col+2].astype(np.float64), np.nan)
        mini_nc_file.createVariable('latitude', np.float64, ('x', 'y'))[:] = lat_s
    
        
        mini_nc_file.close()
        
        print(f'Created mini file {l2_name}_{lon:.4f}_{lat:.4f}_nasa.nc /n')
    # print(f'Processing {l2_mini} Done!/n')
    
    except:
        print(f'Creating {l2_mini} failed /n')
        failed_flag = True
        mini_nc_file.close()
        os.remove(l2_mini)
        return l2_mini,  failed_flag
    
    return l2_mini,  failed_flag


# OCSMART
def RunOCSMART_modis_subset(l1b_file, lon, lat, l2_dir, geo_file):
    os.chdir(ocsmart_path)

    geo_dir = os.path.dirname(geo_file)
    # return the l2 mini file name and the failed flag  
    # 
    failed_flag = False
    l1_name = os.path.basename(os.path.normpath(l1b_file))
    # l1_name = l1_name.replace('.L1B_LAC.hdf', '')
    l2_mini = os.path.join(l2_dir,f'{l1_name}_{lon:.4f}_{lat:.4f}_ocsmart.nc')
    
    if os.path.isfile(l2_mini):
        print(f'{l2_mini} is existed/n')
        return l2_mini, failed_flag
    
    import pyhdf.SD
    from OCSMART_MC_Manh import OCSMART_MC
    
    # Read coordinates
    try:
        if os.path.isfile(l1b_file):
            hdf_file = pyhdf.SD.SD(l1b_file)
            lat_s = hdf_file.select('Latitude').get()
            lon_s = hdf_file.select('Longitude').get()
            
            totalheight, totalwidth = lon_s.shape
            
            M, I = FindMinIndex(lon, lat, lon_s, lat_s)
            row, col = I
            n_pixels = 20
            while True:
                try:
                    S_pos = lat_s[row-20,col]
                    N_pos = lat_s[row+20,col]
                    E_pos = lon_s[row,col-20]
                    W_pos = lon_s[row,col+20]
                    
                    # if lat_s[row-20,col] < lat_s[row+20,col]:
                    #     S_pos = lat_s[row-20,col]
                    #     N_pos = lat_s[row+20,col]
                    # else:
                    #     S_pos = lat_s[row+20,col]
                    #     N_pos = lat_s[row-20,col]
                     
                    # if lon_s[row,col-20] < lon_s[row,col+20]:
                    #     E_pos = lon_s[row,col-20]
                    #     W_pos = lon_s[row,col+20]
                    # else:
                    #     E_pos = lon_s[row,col+20]
                    #     W_pos = lon_s[row,col-20]
                    
                    success = True
                except:
                    n_pixels -= 1
                
                if success or n_pixels == 0:
                    break
           
            if n_pixels == 0:
                print(f'Data point is not in the image /n')
                failed_flag = True
                return l2_mini, failed_flag
                
            
            
            print(f'Creating minifile {os.path.basename(l2_mini)} /n')
            
            OCSMART_MC(L1B_file = l1b_file,
                       L2_file = l2_mini,
                       GEOpath = geo_dir,
                       north = N_pos,
                       south = S_pos,
                       east = E_pos,
                       west = W_pos
                       )
    except:
            print('Reading coordinates failed /n')
            failed_flag = True
    return l2_mini, failed_flag
    
def RunOCSMART_msi_subset(l1_image, lon, lat, l2_dir):
    os.chdir(ocsmart_path)
    import pyhdf.SD
    from OCSMART_MC_Manh import OCSMART_MC
    from glob import glob
     # return the l2 mini file name and the failed flag  
     # 
    failed_flag = False
    resolution = 60
    l1_name = os.path.basename(os.path.normpath(l1_image))
    l1_name.replace('SAFE', '')
    l2_mini = os.path.join(l2_dir,f'{l1_name}_{lon}_{lat}_ocsmart.nc')
    
    if os.path.isfile(l2_mini):
        print(f'{l2_mini} is existed/n')
        return l2_mini, failed_flag
    
    # Read coordinates
    # try:
    #     lon_s,lat_s, totalheight, totalwidth = read_coord_msi(l1_image, resolution)
        
    #     M, I = FindMinIndex(lon, lat, lon_s, lat_s)
    #     row, col = I
    #     S_pos = lat_s[row+20,col]
    #     N_pos = lat_s[row-20,col]
    #     E_pos = lon_s[row,col+20]
    #     W_pos = lon_s[row,col-20]
    # except:
    #     print('Reading coordinates failed /n')
    #     failed_flag = True
    #     return l2_mini, failed_flag
 
    
    # l1_image='/home/manh/Desktop/work/MC_S2/L1/S2B_MSIL1C_20220518T105619_N0400_R094_T31UCS_20220518T114320.SAFE'
                
            
    try:        
        print(f'Creating minifile {os.path.basename(l2_mini)} /n')
        
        # OCSMART_MC(L1B_file = l1_image,
        #            GEOpath=[],
        #            L2_file = l2_mini,
        #            north = N_pos,
        #            south = S_pos,
        #            east = E_pos,
        #            west = W_pos
        #            )
        
        OCSMART_MC(L1B_file = l1_image,
                    L2_file = l2_mini,
                    lat_center=lat,
                    lon_center=lon,
                    box_width=int(20),
                    box_height=int(20),
                    )
        

    except:
            print('Reading coordinates failed /n')
            failed_flag = True
    return l2_mini, failed_flag
 