# import rasterio
# import numpy as np
# from google.cloud import storage
# import tempfile
# from pathlib import Path
# import os
# from dotenv import load_dotenv
# from collections import defaultdict

# load_dotenv()

# GCS_PROJECT_ID = os.getenv('GCS_PROJECT_ID')
# GCS_BUCKET = 'external_satellite_datasets'
# GCS_BLOB_PREFIX = 'visualization_data/no2_daily_files/COG/mapbox/2024/'

# # Initialize GCS
# client = storage.Client(project=GCS_PROJECT_ID)
# bucket = client.bucket(GCS_BUCKET)
# blobs = list(bucket.list_blobs(prefix=GCS_BLOB_PREFIX))

# monthly_counts = defaultdict(int)
# daily_results = []

# print(f"Found {len(blobs)} files to process...")

# for blob in blobs:
#     if blob.name.endswith('.tif'):
#         date = Path(blob.name).stem.replace('_NO2', '')
#         print(f"Processing {date}...")
        
#         with tempfile.NamedTemporaryFile(suffix='.tif', delete=False) as tmp:
#             blob.download_to_filename(tmp.name)
            
#             try:
#                 with rasterio.open(tmp.name) as src:
#                     data = src.read(1)
                
#                 # Filter out nodata (0) and convert pixel values to ppb
#                 valid_data = data[(data > 0) & (data < 256)]
                
#                 if len(valid_data) > 0:
#                     ppb_data = ((valid_data - 1) / 254) * 200
                    
#                     max_ppb = np.max(ppb_data)
                    
#                     # Extract month (YYYY-MM from YYYY-MM-DD)
#                     month = date[:7]
                    
#                     # Count if any pixel exceeds 100 ppb
#                     if max_ppb > 100:
#                         monthly_counts[month] += 1
                    
#                     daily_results.append({
#                         'date': date,
#                         'month': month,
#                         'max_ppb': max_ppb,
#                         'over_100': max_ppb > 100
#                     })
                    
#             except Exception as e:
#                 print(f"Error: {e}")
#             finally:
#                 Path(tmp.name).unlink()

# # Sort months by count
# sorted_months = sorted(monthly_counts.items(), key=lambda x: x[1], reverse=True)

# print("\n" + "="*80)
# print("MONTHS WITH MOST DAYS OVER 100 PPB (EPA 1-HOUR LIMIT)")
# print("="*80)
# print(f"{'Month':<15} {'Days > 100 ppb':<15}")
# print("-"*80)

# for month, count in sorted_months:
#     print(f"{month:<15} {count:<15}")

# print("\n" + "="*80)
# print("DAYS OVER 100 PPB:")
# print("="*80)

# over_100_days = [r for r in daily_results if r['over_100']]
# over_100_days.sort(key=lambda x: x['max_ppb'], reverse=True)

# print(f"{'Date':<15} {'Max (ppb)':<15}")
# print("-"*80)
# for r in over_100_days:  # Show all
#     print(f"{r['date']:<15} {r['max_ppb']:<15.2f}")

# print(f"\nTotal days analyzed: {len(daily_results)}")
# print(f"Total days over 100 ppb: {len(over_100_days)}")

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
GCS_PREFIX_TIF = 'visualization_data/no2_daily_files/COG/mapbox/2024/'

client = storage.Client(project=GCS_PROJECT_ID)

# Get all January 2024 files
print("Finding January 2024 files...")
bucket_nc = client.bucket(GCS_BUCKET_NC)
bucket_tif = client.bucket(GCS_BUCKET_TIF)

nc_files = {}
tif_files = {}

# Find NetCDF files
for blob in bucket_nc.list_blobs(prefix=GCS_PREFIX_NC):
    if blob.name.endswith('.nc'):
        match = re.search(r'(2024-01-\d{2})', blob.name)
        if match:
            date = match.group(1)
            nc_files[date] = blob

# Find TIF files
for blob in bucket_tif.list_blobs(prefix=GCS_PREFIX_TIF):
    if blob.name.endswith('.tif'):
        match = re.search(r'(2024-01-\d{2})', blob.name)
        if match:
            date = match.group(1)
            tif_files[date] = blob

print(f"Found {len(nc_files)} NetCDF files")
print(f"Found {len(tif_files)} TIF files")

# Find common dates
common_dates = sorted(set(nc_files.keys()) & set(tif_files.keys()))
print(f"Processing {len(common_dates)} matching dates\n")

results = []

for date in common_dates:
    print(f"Processing {date}...")
    
    nc_blob = nc_files[date]
    tif_blob = tif_files[date]
    
    # Process NetCDF
    with tempfile.NamedTemporaryFile(suffix='.nc', delete=False) as tmp_nc:
        nc_blob.download_to_filename(tmp_nc.name)
        
        ds = nc.Dataset(tmp_nc.name)
        vcd = ds.variables['vertical_column_troposphere'][:]
        nc_max = np.nanmax(vcd, axis=0)
        nc_valid = nc_max[nc_max > 0]
        nc_ppb = nc_valid / 2.5e16
        
        nc_max_ppb = np.nanmax(nc_ppb)
        nc_mean_ppb = np.nanmean(nc_ppb)
        
        ds.close()
        Path(tmp_nc.name).unlink()
    
    # Process TIF
    with tempfile.NamedTemporaryFile(suffix='.tif', delete=False) as tmp_tif:
        tif_blob.download_to_filename(tmp_tif.name)
        
        with rasterio.open(tmp_tif.name) as src:
            tif_data = src.read(1)
        
        tif_valid = tif_data[(tif_data > 0) & (tif_data < 256)]
        tif_molecules = ((tif_valid - 1) / 254) * 5e17
        tif_ppb = tif_molecules / 2.5e16
        
        tif_max_ppb = np.max(tif_ppb)
        tif_mean_ppb = np.mean(tif_ppb)
        
        Path(tmp_tif.name).unlink()
    
    # Calculate differences
    max_diff = abs(nc_max_ppb - tif_max_ppb)
    max_diff_pct = (max_diff / nc_max_ppb * 100) if nc_max_ppb > 0 else 0
    mean_diff = abs(nc_mean_ppb - tif_mean_ppb)
    mean_diff_pct = (mean_diff / nc_mean_ppb * 100) if nc_mean_ppb > 0 else 0
    
    results.append({
        'date': date,
        'nc_max': nc_max_ppb,
        'tif_max': tif_max_ppb,
        'max_diff': max_diff,
        'max_diff_pct': max_diff_pct,
        'nc_mean': nc_mean_ppb,
        'tif_mean': tif_mean_ppb,
        'mean_diff': mean_diff,
        'mean_diff_pct': mean_diff_pct
    })

# Print summary
print("\n" + "="*100)
print("JANUARY 2024 COMPARISON SUMMARY")
print("="*100)
print(f"{'Date':<12} {'NC Max':<10} {'TIF Max':<10} {'Diff':<10} {'Diff %':<10} {'Status':<10}")
print("-"*100)

for r in results:
    status = "✓ OK" if r['max_diff'] < 5 else "✗ WARN"
    print(f"{r['date']:<12} {r['nc_max']:<10.2f} {r['tif_max']:<10.2f} {r['max_diff']:<10.2f} {r['max_diff_pct']:<10.1f} {status:<10}")

print("\n" + "="*100)
print("STATISTICS:")
print("="*100)
avg_max_diff = np.mean([r['max_diff'] for r in results])
avg_mean_diff = np.mean([r['mean_diff'] for r in results])
max_max_diff = np.max([r['max_diff'] for r in results])

print(f"Average max difference: {avg_max_diff:.2f} ppb")
print(f"Average mean difference: {avg_mean_diff:.2f} ppb")
print(f"Largest max difference: {max_max_diff:.2f} ppb")

good_count = sum(1 for r in results if r['max_diff'] < 5)
print(f"\nDays within 5 ppb tolerance: {good_count}/{len(results)} ({good_count/len(results)*100:.1f}%)")