import subprocess
from google.cloud import storage
from pathlib import Path
import tempfile
import time
import requests
import re
import json
from dotenv import load_dotenv
import os
import rasterio
import numpy as np

load_dotenv()

# Get environment variables
MAPBOX_TOKEN = os.getenv('MAPBOX_TOKEN')
MAPBOX_USERNAME = os.getenv('MAPBOX_USERNAME')
GCS_PROJECT_ID = os.getenv('GCS_PROJECT_ID')
GCS_BUCKET = os.getenv('GCS_BUCKET')
GCS_BLOB_PREFIX = os.getenv('GCS_BLOB_PREFIX')
GCS_BLOB_OUTPUT_PREFIX = os.getenv('GCS_BLOB_OUTPUT_PREFIX') + '2/'

LA_BOUNDARY_URL = "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/State_County/MapServer/0/query?where=NAME='Louisiana'&outFields=*&outSR=4326&f=geojson"
LA_BOUNDARY = requests.get(LA_BOUNDARY_URL).json()

# Extract date from filename
def extract_date(filename):
    match = re.search(r'(\d{4}-\d{2}-\d{2})', filename)
    if not match:
        print(f'✗ Could not extract date from {filename}')
    return match.group(1) if match else None

# Convert TEMPO NetCDF to 8-bit COG
def convert_tempo_to_8bit_cog(nc_file, output_file):
    LA_BOUNDS = [-94.043, 28.925, -88.817, 33.019]
        
    # manage temporary files
    temp_all_bands = output_file.replace('.tif', '_allbands.tif')
    temp_modified = output_file.replace('.tif', '_modified.tif')
    temp_clipped = output_file.replace('.tif', '_clipped.tif')
    temp_boundary = output_file.replace('.tif', '_boundary.geojson')
    with open(temp_boundary, 'w') as f:
        json.dump(LA_BOUNDARY, f)

    netcdf_path = f'NETCDF:"{nc_file}":vertical_column_troposphere'

    # Step 1: Extract ALL bands
    subprocess.run([
        'gdal_translate',
        '-of', 'GTiff',
        '-projwin', str(LA_BOUNDS[0]), str(LA_BOUNDS[3]), str(LA_BOUNDS[2]), str(LA_BOUNDS[1]),
        '-a_srs', 'EPSG:4269',
        netcdf_path,
        temp_all_bands
    ], check=True)

    # After Step 1, check how many bands were extracted
    print(f"Checking bands in {temp_all_bands}...")
    subprocess.run(['gdalinfo', temp_all_bands], check=True)

    # Step 2: Calculte daily MAX (or mean) NO2 across all bands
    print("Calculating daily NO2...")
    with rasterio.open(temp_all_bands) as src:
        data = src.read()
        daily_max = np.nanmax(data, axis=0)
        profile = src.profile.copy()
        profile.update({
            'count': 1,
            'dtype': 'float64',
            'nodata': 0
        })
        
        with rasterio.open(temp_modified, 'w', **profile) as dst:
            dst.write(daily_max, 1)


    # Step 3: Clip to LA borders
    subprocess.run([
        'gdalwarp',
        '-of', 'GTiff',
        '-cutline', temp_boundary,
        '-crop_to_cutline',
        '-dstnodata', '0',
        temp_modified,
        temp_clipped
    ], check=True)

    # Step 3: Convert to 8-bit COG with scaling
    subprocess.run([
        'gdal_translate',
        '-of', 'COG',
        '-ot', 'Byte',
        '-scale', '0', '5e17', '1', '255',
        '-a_nodata', '0',
        '-co', 'COMPRESS=DEFLATE',
        '-co', 'BLOCKSIZE=512',
        temp_clipped,
        output_file
    ], check=True)
    
    # Clean up temp file
    Path(temp_all_bands).unlink()
    Path(temp_modified).unlink()
    Path(temp_boundary).unlink()
    Path(temp_clipped).unlink()
        
    print(f"✓ Created 8-bit Louisiana COG: {output_file}")
    
print('='*60)
print('Processing TEMPO NetCDF')
print('='*60)
print('Initializing...')

# Initialize GCS client
client = storage.Client(project=GCS_PROJECT_ID) 
bucket = client.bucket(GCS_BUCKET)
blobs = bucket.list_blobs(prefix=GCS_BLOB_PREFIX)

pattern = r'tempo_2024-\d{2}-\d{2}.nc' # all 2024 files
# pattern = r'tempo_2024-01-\d{2}.nc'  # January 2024 files only

processed = 0
file_count = 0
for blob in blobs:
    file_count += 1
    if file_count <= 5000:        
        if bool(re.search(pattern, blob.name)):
            processed += 1

            print('file_count:', file_count)
            print(f"Processed count: {processed}")
            print(f'\nProcessing {blob.name}...')
        
            with tempfile.NamedTemporaryFile(suffix='.nc', delete=False) as tmp_nc:
                blob.download_to_filename(tmp_nc.name)
                with tempfile.NamedTemporaryFile(suffix='.tif', delete=False) as tmp_tif:
                    try:
                        # Step 1: Convert to 8-bit COG
                        convert_tempo_to_8bit_cog(tmp_nc.name, tmp_tif.name)
                        
                        # Step 2: Upload to GCS bucket
                        filename = Path(blob.name).stem.replace('tempo_', '') + '_NO2'
                        output_blob_name = f'{GCS_BLOB_OUTPUT_PREFIX}{filename}.tif'
                        output_blob = bucket.blob(output_blob_name)
                        output_blob.upload_from_filename(tmp_tif.name)
                        print(f'✓ Uploaded to gs://{GCS_BUCKET}/{output_blob_name}')
                        
                        # print(f'\n✓ {date} completed successfully!')
                        print(f"Processed count: {processed}")

                    except Exception as e:
                        print(f'✗ Error: {e}')
                    finally:
                        Path(tmp_nc.name).unlink()
                        Path(tmp_tif.name).unlink()

print(f'\nComplete! Processed {processed} files.')