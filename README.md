from EXmatchup import EXmatchup
import pandas as pd

df = pd.read_csv('Eumetsat_Dataset_Clean.csv')
MC = EXmatchup(Data_input=df[pd.notna(df['Chla'])],
                sensor = 'modis',
                max_threads=12,
                email = 'manhtranduy1993@gmail.com',
                token = token,
                run_AC = 'l2gen',
                l1_dir = '/mnt/hgfs/work/Codes/Eumetsat/matchup_python/SatData/L1/MODIS',
                l2_dir = '/mnt/hgfs/work/Codes/Eumetsat/matchup_python/SatData/L2/MODIS',block=500)

Data = MC.Data_output
