#!/bin/bash

GEO_DIR=/home/manh/Desktop/work/MODIS/GEO/
L1C_DIR=/home/manh/Desktop/work/MODIS/L1C/
L1B_DIR=/home/manh/Desktop/work/MODIS/L1B/
L2A_DIR=/home/manh/Desktop/work/MODIS/L2A/
export OCSSWROOT=/home/manh/SeaDAS/ocssw
source $OCSSWROOT/OCSSW_bash.env
cd /home/manh/Desktop/work/MODIS/L1A/

# Assuming the files have an extension like .hdf or .txt etc.
for L1AFILE in *.hdf
do
   echo "Processing: $L1AFILE"
   BASE=$(basename $L1AFILE .L1A_LAC) # This strips the extension from the FILE variable
   GEOFILE="${GEO_DIR}${BASE}.GEO.hdf"
   L1BFILE="${L1B_DIR}${BASE}.L1B_LAC.hdf"
   L2AFILE="${L2A_DIR}${BASE}.L2.hdf"
   L1CFILE="${L1C_DIR}${BASE}.L1C.nc"

   echo "GENERATING NEW GEOFILE..."
   /home/manh/SeaDAS/ocssw/bin/modis_GEO $L1AFILE -o $GEOFILE
   echo "GENERATING NEW L1B..."
   /home/manh/SeaDAS/ocssw/bin/modis_L1B $L1AFILE $GEOFILE -o $L1BFILE
#   echo "GENERATING NEW L2..."
#   /home/manh/SeaDAS/ocssw/bin/l2gen ifile=$L1BFILE geofile=$GEOFILE ofile1=$L2AFILE aer_opt=-10
   echo "GENERATING L1C..."
   /home/manh/SeaDAS/ocssw/bin/l2gen ifile=$L1BFILE geofile=$GEOFILE ofile1=$L1CFILE oformat="netcdf4" l2prod="rhot_nnn polcor_nnn sena senz sola solz latitude longitude"
done

