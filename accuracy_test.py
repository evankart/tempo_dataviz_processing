import netCDF4 as nc
import rasterio
import numpy as np
from google.cloud import storage
import tempfile
from pathlib import Path
import os
from dotenv import load_dotenv
import re

load_dotenv()

GCS_PROJECT_ID = os.getenv('GCS_PROJECT_ID')
GCS_BUCKET_NC = os.getenv('GCS_BUCKET')
GCS_PREFIX_NC = os.getenv('GCS_BLOB_PREFIX')
GCS_BUCKET_TIF = 'external_satellite_datasets'
GCS_PREFIX_TIF = 'visualization_data/no2_daily_files/COG/mapbox/2024/march/'

client = storage.Client(project=GCS_PROJECT_ID)

# Test just one date first
test_date = '2024-03-01'

print(f"Testing {test_date}...")
bucket_nc = client.bucket(GCS_BUCKET_NC)
bucket_tif = client.bucket(GCS_BUCKET_TIF)

# Find files
nc_blob = None
tif_blob = None

for blob in bucket_nc.list_blobs(prefix=GCS_PREFIX_NC):
    if test_date in blob.name and blob.name.endswith('.nc'):
        nc_blob = blob
        break

for blob in bucket_tif.list_blobs(prefix=GCS_PREFIX_TIF):
    if test_date in blob.name and blob.name.endswith('.tif'):
        tif_blob = blob
        break

if not nc_blob or not tif_blob:
    print("ERROR: Could not find files!")
    exit()

print(f"NC: {nc_blob.name}")
print(f"TIF: {tif_blob.name}")

# Process NetCDF with detailed output
with tempfile.NamedTemporaryFile(suffix='.nc', delete=False) as tmp_nc:
    nc_blob.download_to_filename(tmp_nc.name)
    
    ds = nc.Dataset(tmp_nc.name)
    vcd = ds.variables['vertical_column_troposphere'][:]
    
    print(f"\nNetCDF shape: {vcd.shape}")
    print(f"NetCDF bands: {vcd.shape[0]}")
    
    # Get max across time for each pixel
    nc_max = np.nanmax(vcd, axis=0)
    
    print(f"After nanmax shape: {nc_max.shape}")
    
    nc_valid = nc_max[nc_max > 0]
    nc_molecules = nc_valid
    nc_ppb = nc_molecules / 2.5e16
    
    print(f"\nNetCDF raw max: {np.nanmax(nc_molecules):.2e} molecules/cm²")
    print(f"NetCDF max ppb: {np.nanmax(nc_ppb):.2f} ppb")
    print(f"NetCDF mean ppb: {np.nanmean(nc_ppb):.2f} ppb")
    
    ds.close()
    Path(tmp_nc.name).unlink()

# Process TIF with detailed output
with tempfile.NamedTemporaryFile(suffix='.tif', delete=False) as tmp_tif:
    tif_blob.download_to_filename(tmp_tif.name)
    
    with rasterio.open(tmp_tif.name) as src:
        tif_data = src.read(1)
        print(f"\nTIF shape: {tif_data.shape}")
        print(f"TIF pixel range: {np.min(tif_data)} - {np.max(tif_data)}")
    
    tif_valid = tif_data[(tif_data > 0) & (tif_data < 256)]
    print(f"TIF valid pixels: {len(tif_valid)}")
    print(f"TIF valid pixel range: {np.min(tif_valid)} - {np.max(tif_valid)}")
    
    # Convert back to molecules/cm²
    tif_molecules = ((tif_valid - 1) / 254) * 5e17
    tif_ppb = tif_molecules / 2.5e16
    
    print(f"\nTIF raw max: {np.max(tif_molecules):.2e} molecules/cm²")
    print(f"TIF max ppb: {np.max(tif_ppb):.2f} ppb")
    print(f"TIF mean ppb: {np.mean(tif_ppb):.2f} ppb")
    
    Path(tmp_tif.name).unlink()

print(f"\n{'='*60}")
print(f"DIFFERENCE: {abs(np.nanmax(nc_ppb) - np.max(tif_ppb)):.2f} ppb")
print(f"DIFFERENCE %: {abs(np.nanmax(nc_ppb) - np.max(tif_ppb)) / np.nanmax(nc_ppb) * 100:.1f}%")