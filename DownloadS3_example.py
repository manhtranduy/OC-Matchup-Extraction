from S3.S3_Eumetsat_Search import S3_Eumetsat_Search
from S3.S3_Eumetsat_Download import S3_Eumetsat_Download
# from ACprocessors import RunPOLYMER_msi
import os
import glob

products=S3_Eumetsat_Search([105.5029,107.4190,8.7030,10.9810],processingLevel=2)
products=products.product_summary

# l1_folder='/home/manh/Desktop/work/Brazil_Vincent/Charlotte/S3/L1C/'
l2_dir='M:/S3/L2'


    
S3_Eumetsat_Download(products['product_list'],outdir=l2_dir)




