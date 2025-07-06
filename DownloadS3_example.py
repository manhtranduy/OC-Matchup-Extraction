from S3.S3_Eumetsat_Search import S3_Eumetsat_Search
from S3.S3_Eumetsat_Download import S3_Eumetsat_Download
# from ACprocessors import RunPOLYMER_msi
import os
import glob

products=S3_Eumetsat_Search([105.5029,107.4190,8.7030,10.9810],
                            processingLevel=2,
                            start_date='2020-06-01',
                            end_date='2020-06-30')

products=products.product_summary
l2_dir='path/to/save'


    
S3_Eumetsat_Download(products['product_list'],outdir=l2_dir)




