import requests
import os
import json
import time
import subprocess
import tempfile
import re
from pathlib import Path
from google.cloud import storage
from dotenv import load_dotenv

load_dotenv()

# Configuration
MAPBOX_TOKEN = os.getenv('MAPBOX_TOKEN')
MAPBOX_USERNAME = os.getenv('MAPBOX_USERNAME')
GCS_PROJECT_ID = os.getenv('GCS_PROJECT_ID')
GCS_BUCKET = os.getenv('GCS_BUCKET')
GCS_BLOB_PREFIX = os.getenv('GCS_BLOB_PREFIX')

# Tileset configuration
SOURCE_NAME = "tempo_no2_sep_1_5"
TILESET_ID = f"{MAPBOX_USERNAME}.tempo_no2_sep_1_5"
TILESET_NAME = "TEMPO NO2 September 1-5 2023"

# File pattern to match (adjust as needed)
FILE_PATTERN = r'tempo_2024-01-0[1-23].nc'  # January 1-5 only
MAX_FILES = 5  # Limit to 5 files

# Louisiana boundary for clipping
LA_BOUNDARY_URL = "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/State_County/MapServer/0/query?where=NAME='Louisiana'&outFields=*&outSR=4326&f=geojson"
LA_BOUNDARY = requests.get(LA_BOUNDARY_URL).json()
LA_BOUNDS = [-94.043, 28.925, -88.817, 33.019]

def convert_tempo_to_8bit_cog(nc_file, output_file):
    """Convert TEMPO NetCDF to 8-bit COG with Louisiana clipping"""
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
        # '-b', '1',
        '-projwin', str(LA_BOUNDS[0]), str(LA_BOUNDS[3]), str(LA_BOUNDS[2]), str(LA_BOUNDS[1]),
        '-a_srs', 'EPSG:4269',
        netcdf_path,
        temp_all_bands
    ], check=True)

    # Step 2: Calculate daily MAX NO2 across all bands
    subprocess.run([
        'gdal_calc.py',
        '--calc', 'numpy.nanmax(A, axis=0)',
        '--allBands', 'A',
        '-A', temp_all_bands,
        '--outfile', temp_modified,
        '--NoDataValue=0',
        '--format', 'GTiff',
        '--type', 'Float32'
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

    # Step 4: Convert to 8-bit COG with scaling
    subprocess.run([
        'gdal_translate',
        '-of', 'COG',
        '-ot', 'Byte',
        '-b', '1',  # Explicitly select only band 1
        '-scale', '0', '5e17', '1', '255',
        '-a_nodata', '0',
        '-co', 'COMPRESS=DEFLATE',
        '-co', 'BLOCKSIZE=512',
        temp_clipped,
        output_file
    ], check=True)
    
    # Clean up temp files
    for temp_file in [temp_all_bands, temp_modified, temp_boundary, temp_clipped]:
        if Path(temp_file).exists():
            Path(temp_file).unlink()

def download_and_convert_nc_files():
    """Step 1: Download NC files from GCS and convert to TIF"""
    print(f"\n{'='*60}")
    print("STEP 1: Downloading and converting NC files to TIF")
    print(f"{'='*60}")
    
    # Initialize GCS client
    client = storage.Client(project=GCS_PROJECT_ID)
    bucket = client.bucket(GCS_BUCKET)
    blobs = bucket.list_blobs(prefix=GCS_BLOB_PREFIX)
    
    tif_files = []
    processed = 0
    
    for blob in blobs:
        if not re.search(FILE_PATTERN, blob.name):
            continue
            
        if processed >= MAX_FILES:
            print(f"\nReached max files limit ({MAX_FILES})")
            break
        
        processed += 1
        print(f"\n[{processed}/{MAX_FILES}] Processing {blob.name}...")
        
        # Download NC file
        with tempfile.NamedTemporaryFile(suffix='.nc', delete=False) as tmp_nc:
            blob.download_to_filename(tmp_nc.name)
            
            # Convert to TIF
            date_match = re.search(r'(\d{4}-\d{2}-\d{2})', blob.name)
            date = date_match.group(1) if date_match else f"file_{processed}"
            
            tif_path = f"/tmp/tempo_{date}.tif"
            
            try:
                convert_tempo_to_8bit_cog(tmp_nc.name, tif_path)
                tif_files.append(tif_path)
                print(f"✓ Converted to {tif_path}")
            except Exception as e:
                print(f"❌ Conversion failed: {e}")
            finally:
                Path(tmp_nc.name).unlink()
    
    return tif_files

def upload_source_files(tif_files):
    """Step 2: Upload all TIF files to create a tileset source"""
    print(f"\n{'='*60}")
    print("STEP 2: Uploading TIF files as tileset source")
    print(f"{'='*60}")
    
    url = f"https://api.mapbox.com/tilesets/v1/sources/{MAPBOX_USERNAME}/{SOURCE_NAME}"
    
    for i, tif_file in enumerate(tif_files):
        if not os.path.exists(tif_file):
            print(f"❌ File not found: {tif_file}")
            continue
            
        print(f"\n[{i+1}/{len(tif_files)}] Uploading {Path(tif_file).name}...")
        
        with open(tif_file, 'rb') as f:
            files = {'file': (Path(tif_file).name, f, 'image/tiff')}
            params = {'access_token': MAPBOX_TOKEN}
            response = requests.post(url, files=files, params=params)
        
        if response.status_code in [200, 201]:
            print(f"✓ Uploaded successfully")
        else:
            print(f"❌ Upload failed: {response.status_code}")
            print(response.text)
            return False
    
    print(f"\n✓ All files uploaded to source: {SOURCE_NAME}")
    return True

def create_recipe():
    """Step 3: Create the recipe JSON"""
    print(f"\n{'='*60}")
    print("STEP 3: Creating recipe")
    print(f"{'='*60}")
    
    recipe = {
        "version": 1,
        "type": "rasterarray",
        "sources": [
            {
                "uri": f"mapbox://tileset-source/{MAPBOX_USERNAME}/{SOURCE_NAME}"
            }
        ],
        "minzoom": 3,
        "maxzoom": 12,
        "layers": {
            "no2": {
                "tilesize": 256,
                "resampling": "nearest",
                "buffer": 1,
                "units": "molecules/cm^2"
            }
        }
    }
    
    print(f"✓ Recipe created")
    print(json.dumps(recipe, indent=2))
    return recipe

def create_tileset(recipe):
    """Step 4: Create the tileset with recipe"""
    print(f"\n{'='*60}")
    print("STEP 4: Creating tileset")
    print(f"{'='*60}")
    
    url = f"https://api.mapbox.com/tilesets/v1/{TILESET_ID}"
    data = {
        "recipe": recipe,
        "name": TILESET_NAME
    }
    
    response = requests.post(url, json=data, params={'access_token': MAPBOX_TOKEN})
    
    if response.status_code in [200, 201]:
        print(f"✓ Tileset created: {TILESET_ID}")
        return True
    else:
        print(f"❌ Failed to create tileset: {response.status_code}")
        print(response.text)
        return False

def publish_tileset():
    """Step 5: Publish (process) the tileset"""
    print(f"\n{'='*60}")
    print("STEP 5: Publishing tileset")
    print(f"{'='*60}")
    
    url = f"https://api.mapbox.com/tilesets/v1/{TILESET_ID}/publish"
    response = requests.post(url, params={'access_token': MAPBOX_TOKEN})
    
    if response.status_code in [200, 201]:
        job = response.json()
        job_id = job.get('jobId')
        print(f"✓ Publishing started")
        print(f"  Job ID: {job_id}")
        return job_id
    else:
        print(f"❌ Failed to publish: {response.status_code}")
        print(response.text)
        return None

def check_job_status(job_id):
    """Check the status of the publishing job"""
    print(f"\n{'='*60}")
    print("STEP 6: Checking job status")
    print(f"{'='*60}")
    
    url = f"https://api.mapbox.com/tilesets/v1/{TILESET_ID}/jobs/{job_id}"
    
    while True:
        response = requests.get(url, params={'access_token': MAPBOX_TOKEN})
        
        if response.status_code == 200:
            job = response.json()
            stage = job.get('stage', 'unknown')
            
            print(f"Status: {stage}")
            
            if stage == 'success':
                print(f"\n✓ Tileset published successfully!")
                print(f"  View at: https://studio.mapbox.com/tilesets/{TILESET_ID}/")
                break
            elif stage == 'failed':
                print(f"\n❌ Publishing failed")
                print(json.dumps(job, indent=2))
                break
            else:
                print(f"  Still processing... (waiting 10 seconds)")
                time.sleep(10)
        else:
            print(f"❌ Failed to check status: {response.status_code}")
            break

def main():
    print(f"\n{'#'*60}")
    print("MAPBOX TILESET CREATION SCRIPT")
    print(f"{'#'*60}")
    print(f"Username: {MAPBOX_USERNAME}")
    print(f"Source: {SOURCE_NAME}")
    print(f"Tileset ID: {TILESET_ID}")
    print(f"File pattern: {FILE_PATTERN}")
    print(f"Max files: {MAX_FILES}")
    
    # Validate environment
    if not MAPBOX_TOKEN:
        print("❌ MAPBOX_TOKEN not set")
        return
    if not MAPBOX_USERNAME:
        print("❌ MAPBOX_USERNAME not set")
        return
    if not GCS_PROJECT_ID:
        print("❌ GCS_PROJECT_ID not set")
        return
    
    # Execute workflow
    tif_files = download_and_convert_nc_files()
    
    if not tif_files:
        print("\n❌ No TIF files created")
        return
    
    print(f"\n✓ Created {len(tif_files)} TIF files")
    
    if not upload_source_files(tif_files):
        print("\n❌ Workflow stopped due to upload errors")
        # Cleanup temp files
        for f in tif_files:
            if Path(f).exists():
                Path(f).unlink()
        return
    
    recipe = create_recipe()
    
    if not create_tileset(recipe):
        print("\n❌ Workflow stopped due to tileset creation error")
        # Cleanup temp files
        for f in tif_files:
            if Path(f).exists():
                Path(f).unlink()
        return
    
    job_id = publish_tileset()
    
    if job_id:
        check_job_status(job_id)
    
    # Cleanup temp files
    print(f"\nCleaning up temporary files...")
    for f in tif_files:
        if Path(f).exists():
            Path(f).unlink()
            print(f"  Deleted {f}")
    
    print(f"\n{'#'*60}")
    print("WORKFLOW COMPLETE")
    print(f"{'#'*60}")
    print(f"\nYour tileset is available at:")
    print(f"mapbox://{TILESET_ID}")
    print(f"\nView in Mapbox Studio:")
    print(f"https://studio.mapbox.com/tilesets/{TILESET_ID}/")

if __name__ == "__main__":
    main()