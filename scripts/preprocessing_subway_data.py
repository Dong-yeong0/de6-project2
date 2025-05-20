import glob
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
from common.common import clean_data

RAW_DATA_PATH = '../raw_data/subway/*.csv'
OUTPUT_DIR = '../processed_data/subway'
CHUNKS = 1

COLUMNS = [
    'usage_date',
    'line_number',
    'station_name',
    'boarding_count',
    'alighting_count',
    'register_date',
]

DATE_COLUMNS = ['usage_date', 'register_date']
NUMERIC_COLUMNS = ['boarding_count', 'alighting_count']

csv_files = glob.glob(RAW_DATA_PATH)
data_frames = []

for file in csv_files:
    df = pd.read_csv(file, index_col=False, header=0)
    df.columns = COLUMNS

    # Convert date columns to datetime.date
    for col in DATE_COLUMNS:
        df[col] = pd.to_datetime(df[col], format='%Y%m%d', errors='coerce').dt.date

    # Convert numeric columns to nullable integer
    for col in NUMERIC_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

    data_frames.append(df)

# Combine all data
combined_data = pd.concat(data_frames)
cleaned_data = clean_data(combined_data)

# Save to Parquet in chunks
os.makedirs(OUTPUT_DIR, exist_ok=True)
total_rows = len(cleaned_data)
chunk_size = total_rows // CHUNKS
for i in range(CHUNKS):
    start_idx = i * chunk_size
    end_idx = start_idx + chunk_size if i < CHUNKS - 1 else total_rows
    chunk = cleaned_data.iloc[start_idx:end_idx]
    chunk.to_parquet(f'{OUTPUT_DIR}/data_part{i + 1}.parquet', index=False)
    print(f"Part {i + 1} saved: {len(chunk):,} rows")