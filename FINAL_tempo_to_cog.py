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

load_dotenv()

# Get environment variables
MAPBOX_TOKEN = os.getenv('MAPBOX_TOKEN')
MAPBOX_USERNAME = os.getenv('MAPBOX_USERNAME')
GCS_PROJECT_ID = os.getenv('GCS_PROJECT_ID')
GCS_BUCKET = os.getenv('GCS_BUCKET')
GCS_BLOB_PREFIX = os.getenv('GCS_BLOB_PREFIX')
GCS_BLOB_OUTPUT_PREFIX = os.getenv('GCS_BLOB_OUTPUT_PREFIX')

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
        '-b', '1',
        '-projwin', str(LA_BOUNDS[0]), str(LA_BOUNDS[3]), str(LA_BOUNDS[2]), str(LA_BOUNDS[1]),
        '-a_srs', 'EPSG:4269',
        netcdf_path,
        temp_all_bands
    ], check=True)

    # Step 2: Calculte mean (or Max) NO2 across all bands
    print("Calculating daily NO2...")
    subprocess.run([
        'gdal_calc.py',
        '--calc', 'numpy.nanmax(A, axis=0)', # or 'numpy.nanmean(A, axis=0)'
        '--allBands', 'A',
        '-A', temp_all_bands,
        '--outfile', temp_modified,
        '--NoDataValue=0'
    ], check=True)

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
        '-scale', '0', '1e16', '1', '255',
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

# Upload COG to Mapbox tileset source
def upload_to_mapbox_source(file_path, source_id):
    print(f"  Uploading to Mapbox source...")
    url = f"https://api.mapbox.com/tilesets/v1/sources/{MAPBOX_USERNAME}/{source_id}?access_token={MAPBOX_TOKEN}"
    
    with open(file_path, 'rb') as f:
        files = {'file': ('file.tif', f, 'image/tiff')}
        response = requests.post(url, files=files, timeout=300)
    
    if response.status_code == 200:
        print(f"  ✓ Uploaded to Mapbox source")
        return True
    else:
        print(f"  ✗ Upload failed: {response.text}")
        print(f"  URL: {url[:80]}...")
        return False
    
# Generate Mapbox tileset
def create_mapbox_tileset(tileset_id, source_id, date):
    print(f"  Creating tileset...")
    url = f"https://api.mapbox.com/tilesets/v1/{MAPBOX_USERNAME}.{tileset_id}"
    
    recipe = {
        "recipe": {
            "version": 1,
            "type": "rasterarray",
            "sources": [{"uri": f"mapbox://tileset-source/{MAPBOX_USERNAME}/{source_id}"}],
            "minzoom": 0,
            "maxzoom": 6,
            "layers": {
                "no2": {
                    "tilesize": 256,
                    "resampling": "nearest",
                    "buffer": 1,
                    "units": "molecules/cm^2",
                    "source_rules": {
                        "filter": ["all", ["in", ["bandindex"], ["literal", [1]]]]
                    }
                }
            }
        },
        "name": f"{date} NO2"
    }
    
    response = requests.post(
        url,
        params={"access_token": MAPBOX_TOKEN},
        headers={"Content-Type": "application/json"},
        json=recipe,
        timeout=60
    )
    
    if response.status_code in [200, 201]:
        print(f"  ✓ Tileset created")
        return True
    else:
        print(f"  ✗ Create failed: {response.text}")
        return False    


# Publish Mapbox tileset
def publish_mapbox_tileset(tileset_id):
    print(f"  Publishing tileset...")
    url = f"https://api.mapbox.com/tilesets/v1/{MAPBOX_USERNAME}.{tileset_id}/publish"
    
    response = requests.post(
        url,
        params={"access_token": MAPBOX_TOKEN},
        timeout=60
    )
    
    if response.status_code == 200:
        print(f"  ✓ Tileset published")
        return True
    else:
        print(f"  ✗ Publish failed: {response.text}")
        return False
    
print('='*60)
print('Processing TEMPO NetCDF')
print('='*60)
print('Initializing...')

# Initialize GCS client
client = storage.Client(project=GCS_PROJECT_ID) 
bucket = client.bucket(GCS_BUCKET)
blobs = bucket.list_blobs(prefix=GCS_BLOB_PREFIX)

processed = 0
file_count = 0
for blob in blobs:
    file_count += 1
    if file_count <= 15:        
        if blob.name.endswith('19.nc'):
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
                        
                        # Step 3: Create mapbox source and tileset
                        date = extract_date(blob.name)
                        mapbox_id = f"{date}-no2"
                        upload_to_mapbox_source(tmp_tif.name, mapbox_id)
                        time.sleep(5)  # Wait for upload to process
                        create_mapbox_tileset(mapbox_id, mapbox_id, date)
                        publish_mapbox_tileset(mapbox_id)
                        
                        print(f'\n✓ {date} completed successfully!')
                        print(f"Processed count: {processed}")

                    except Exception as e:
                        print(f'✗ Error: {e}')
                    finally:
                        Path(tmp_nc.name).unlink()
                        Path(tmp_tif.name).unlink()

print(f'\nComplete! Processed {processed} files.')