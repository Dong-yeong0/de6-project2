import glob
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
from common.common import clean_data

RAW_DATA_PATH = '../raw_data/bicycle/*.csv'
OUTPUT_DIR = '../processed_data/bicycle'
CHUNKS = 5

COLUMNS = [
    "dt",
    "aggregation_unit",
    "time_slot",
    "start_spot_id",
    "start_spot_name",
    "end_spot_id",
    "end_spot_name",
    "total_rentals",
    "total_usage_minutes",
    "total_usage_distance"
]

NUMERIC_COLUMNS = [
    "total_rentals",
    "total_usage_minutes",
    "total_usage_distance"
]

csv_files = glob.glob(RAW_DATA_PATH)
data_frames = []
for file in csv_files:
    df = pd.read_csv(file, index_col=False, header=0)
    df.columns = COLUMNS

    # Convert date and time_slot to datetime
    df['dt'] = pd.to_datetime(df['dt'], format='%Y%m%d', errors='coerce')
    df['hour_min'] = df['time_slot'].astype(str).str.zfill(4)
    df['hour'] = df['hour_min'].str[:2]
    df['min'] = df['hour_min'].str[2:]
    df['usage_date'] = df['dt'] + pd.to_timedelta(df['hour'] + ':' + df['min'] + ':00')
    df['usage_date'] = pd.to_datetime(df['usage_date']).dt.date

    df.drop(columns=['dt', 'time_slot', 'hour_min', 'hour', 'min'], inplace=True)

    # Convert numeric columns
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
