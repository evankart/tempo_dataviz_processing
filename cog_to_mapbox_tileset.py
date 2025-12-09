from google.cloud import storage
import time
import requests
import re
from dotenv import load_dotenv
import os

load_dotenv()

# Get environment variables
# MAPBOX_TOKEN = os.getenv('MAPBOX_TOKEN')
# MAPBOX_USERNAME = os.getenv('MAPBOX_USERNAME')

MAPBOX_TOKEN = os.getenv('MAPBOX_TOKEN_UCB')
MAPBOX_USERNAME = os.getenv('MAPBOX_USERNAME_UCB')

GCS_PROJECT_ID = os.getenv('GCS_PROJECT_ID')
GCS_BUCKET = os.getenv('GCS_BUCKET')
GCS_BLOB_PREFIX = os.getenv('GCS_BLOB_PREFIX')
GCS_BLOB_OUTPUT_PREFIX = os.getenv('GCS_BLOB_OUTPUT_PREFIX')

LA_BOUNDARY_URL = "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/State_County/MapServer/0/query?where=NAME='Louisiana'&outFields=*&outSR=4326&f=geojson"
LA_BOUNDARY = requests.get(LA_BOUNDARY_URL).json()

def post_with_backoff(url, **kwargs):
    for attempt in range(5):
        r = requests.post(url, **kwargs)
        if r.status_code != 429:
            return r
        # wait and retry after hitting rate limit
        sleep_seconds = 2 ** attempt # 2, 4, 8, 16, 32 seconds
        print(f"  Rate limited (429). Retrying in {sleep_seconds} seconds...")
        time.sleep(sleep_seconds)  
    return r  # last response

# Check if tileset already exists
def tileset_exists(tileset_id):
    url = f"https://api.mapbox.com/tilesets/v1/{MAPBOX_USERNAME}.{tileset_id}"
    response = requests.get(url, params={"access_token": MAPBOX_TOKEN}, timeout=30)
    
    if response.status_code == 200:
        return True
    elif response.status_code == 404:
        return False
    else:
        print(f"  Warning: Could not check tileset status: {response.status_code}")
        return False

# Extract date from filename
def extract_date(filename):
    match = re.search(r'(\d{4}-\d{2}-\d{2})', filename)
    if not match:
        print(f'✗ Could not extract date from {filename}')
    return match.group(1) if match else None
\
# Upload COG to Mapbox tileset source
def upload_to_mapbox_source_from_gcs(blob, source_id):
    """Upload directly from GCS blob to Mapbox"""
    print(f"  Uploading to Mapbox source...")
    url = f"https://api.mapbox.com/tilesets/v1/sources/{MAPBOX_USERNAME}/{source_id}?access_token={MAPBOX_TOKEN}"
    
    # Stream blob content directly
    file_content = blob.download_as_bytes()
    
    files = {'file': ('file.tif', file_content, 'image/tiff')}
    response = post_with_backoff(url, files=files, timeout=300)
    
    if response.status_code == 200:
        print(f"  ✓ Uploaded to Mapbox source")
        return True
    else:
        print(f"  ✗ Upload failed: {response.status_code} {response.text}")
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
            "minzoom": 3,
            "maxzoom": 5,
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
    
    response = post_with_backoff(
        url,
        params={"access_token": MAPBOX_TOKEN},
        headers={"Content-Type": "application/json"},
        json=recipe,
        timeout=60
    )
    
    if response.status_code in [200, 201]:
        print(f"  ✓ Tileset created")
        return True
    elif response.status_code == 400 and "already exists" in response.text:
        print(f"  ⊙ Tileset already exists")
        return True
    else:
        print(f"  ✗ Create failed: {response.status_code} {response.text}")
        return False


# Publish Mapbox tileset
def publish_mapbox_tileset(tileset_id):
    print(f"  Publishing tileset...")
    url = f"https://api.mapbox.com/tilesets/v1/{MAPBOX_USERNAME}.{tileset_id}/publish"
    
    response = post_with_backoff(
        url,
        params={"access_token": MAPBOX_TOKEN},
        timeout=60
    )
    
    if response.status_code == 200:
        print(f"  ✓ Tileset published")
        return True
    else:
        print(f"  ✗ Publish failed: {response.status_code} {response.text}")
        return False
    
print('='*60)
print('Processing TEMPO NetCDF')
print('='*60)
print('Initializing...')

# Initialize GCS client
client = storage.Client(project=GCS_PROJECT_ID) 
bucket = client.bucket(GCS_BUCKET)
blobs = bucket.list_blobs(prefix=GCS_BLOB_OUTPUT_PREFIX)

file_count = 0
processed = 0
skipped = 0

for blob in blobs:
    if not blob.name.endswith('.tif'):
        continue
    
    file_count += 1
    
    if file_count > 31:
        print(f"\n✓ Reached file limit")
        break
    
    print(f'\nFile #{file_count}: {blob.name}')

    date = extract_date(blob.name)
    mapbox_id = f"{date}-no2"

    # Check if tileset already exists
    if tileset_exists(mapbox_id):
        print(f"Tileset {mapbox_id} already exists, skipping...")
        skipped += 1
        continue
    
    print(f"Processing {file_count}...")

    processed += 1

    try:
        upload_to_mapbox_source_from_gcs(blob, mapbox_id)
        # time.sleep(10)
        create_mapbox_tileset(mapbox_id, mapbox_id, date)
        # time.sleep(10)
        publish_mapbox_tileset(mapbox_id)
        # time.sleep(30)

        print(f"✓ {date} completed successfully!")
        
    except Exception as e:
        print(f'✗ Error: {e}')

print(f'\nComplete! Total files: {file_count - 1}, Processed {processed} new tilesets, Skipped {skipped} existing tilesets.')