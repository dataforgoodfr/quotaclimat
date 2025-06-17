import csv
import os
import time
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from collections import defaultdict
from quotaclimat.data_processing.mediatree.s3.s3_utils import upload_folder_to_s3, get_s3_client
from zoneinfo import ZoneInfo

# execute me with docker compose up testconsole -d / exec run bash
# docker compose exec testconsole bash
# /app/ cd i8n/
# /app/i8n# poetry run python3 srt-to-mediatree-format.py

timezone = "Europe/Brussels"

def split_words_on_apostrophes(text):
    split_words = []
    for word in text.split():
        if "'" in word and not word.startswith("'"):
            left, right = word.split("'", 1)
            split_words.append(f"{left}'")
            split_words.append(right)
        else:
            split_words.append(word)
    return split_words

def parse_csv_without_headers(file_path, encoding='utf-8'):
    """
    Parse CSV file without headers with the format:
    channel_name, channel_program_name, start, plaintext
    """
    data = []
    row_count = 0
    
    try:
        with open(file_path, 'r', encoding=encoding, errors='replace') as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=';')
            for row in csv_reader:
                row_count += 1
                if len(row) >= 4:
                    channel_name = row[0]
                    program_name = row[1]
                    start_time = row[2]
                    plaintext = row[3]

                    if "Ã©" in plaintext:
                        raise ValueError(f"Invalid character in plaintext at row {row_count}: {plaintext}")

                    data.append({
                        'channel_name': channel_name,
                        'program_name': program_name,
                        'start_time': start_time,
                        'plaintext': plaintext
                    })
                else:
                    print(f"  Warning: Row {row_count} has fewer than 4 fields, skipping: {row}")
        
        print(f"  Successfully parsed {len(data)} rows from {file_path}")
    except Exception as e:
        print(f"  ERROR processing file {file_path}: {e}")
        raise e
    
    return data

def convert_datetime_to_timestamp(date_str):
    """Convert date string to UNIX timestamp"""
    try:
        
        dt: datetime = datetime.strptime(date_str, "%d/%m/%Y %H:%M:%S")
        aware_dt = dt.replace(tzinfo=ZoneInfo(timezone))
        # print(f"  Converting date: '{date_str}' to datetime : {aware_dt} - aware_dt.timestamp(): {aware_dt.timestamp()} - dt.timestamp(): {dt.timestamp()}")
        return int(aware_dt.timestamp()), aware_dt
    except ValueError:
        print(f"  ERROR converting date: '{date_str}' - Invalid format")
        return 0, None

def group_by_window_and_partition(data, window_minutes=2):
    """
    Group data into time windows of specified minutes
    and organize by year/month/day/channel partitions
    """
    # First level: year/month/day/channel
    partitions = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(list))))
    # Then for each partition: windows
    windows_by_partition = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(list)))))
    
    invalid_count = 0
    
    print(f"Grouping {len(data)} entries into {window_minutes}-minute windows with partitioning")
    
    for item in data:
        timestamp, dt = convert_datetime_to_timestamp(item['start_time'])
        if timestamp > 0 and dt:  # Skip invalid timestamps
            # Create partition keys
            year = dt.year
            month = dt.month
            day = dt.day
            channel = item['channel_name']
            item['start'] = dt
            
            # Add to partitioned data
            partitions[year][month][day][channel].append(item)
            
            # Group by window within partition
            window_start = timestamp - (timestamp % (window_minutes * 60))
            windows_by_partition[year][month][day][channel][window_start].append(item)
        else:
            invalid_count += 1
    
    # Count total partitions and windows
    partition_count = sum(1 for year in partitions for month in partitions[year] 
                         for day in partitions[year][month] 
                         for channel in partitions[year][month][day])
    
    window_count = sum(1 for year in windows_by_partition 
                      for month in windows_by_partition[year] 
                      for day in windows_by_partition[year][month] 
                      for channel in windows_by_partition[year][month][day]
                      for window in windows_by_partition[year][month][day][channel])
    
    print(f"Created {partition_count} partitions with {window_count} total time windows")
    print(f"Skipped {invalid_count} entries with invalid timestamps")
    
    return windows_by_partition

def create_mediatree_data_for_partition(windows_data):
    """
    Create mediatree data for a specific partition in a format suitable for Parquet
    Returns a list of dictionaries, one for each window
    """
    result = []
    
    for window_start, items in windows_data.items():
        # Combine plaintext from all items in the window
        combined_plaintext = " ".join([item['plaintext'] for item in items])
        
        # Get channel info from the first item
        channel_name = items[0]['channel_name']
        start = items[0]['start']
        
        # Process words for SRT
        words = split_words_on_apostrophes(combined_plaintext)
        srt_entries = []
        
        # Convert window_start to milliseconds for cts_in_ms
        base_timestamp_ms = window_start * 1000
        num_words = len(words)
        window_duration_ms = 2 * 60 * 1000  # 2 minutes in milliseconds
        word_duration_ms = window_duration_ms // num_words

        for i, word in enumerate(words):
            srt_entry = {
                "duration_ms": 31,
                "cts_in_ms": base_timestamp_ms + (i * word_duration_ms),
                "text": word
            }
            srt_entries.append(srt_entry)
        
        # Create window entry
        window_entry = {
            "srt": srt_entries,
            "channel_name": channel_name,
            "channel_title": channel_name,
            "start": start,
            "plaintext": combined_plaintext
        }
        
        result.append(window_entry)
    
    return result

def process_csv_folder_to_partitioned_parquet(folder_path, output_dir="mediatree_output", encoding='utf-8'):
    """
    Process all CSV files in a folder to mediatree format
    with partitioning by year/month/day/channel and output as Parquet
    """
    
    print(f"\n{'='*50}")
    print(f"Starting processing of CSV files in: {folder_path}")
    print(f"{'='*50}\n")
    
    start_time = time.time()
    all_data = []
    
    # Check if folder exists
    if not os.path.exists(folder_path):
        print(f"ERROR: Folder {folder_path} does not exist")
        return None
    
    # Get list of CSV files
    csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
    print(f"Found {len(csv_files)} parquet files in folder")
    
    # Process each CSV file in the folder
    for i, filename in enumerate(csv_files):
        file_path = os.path.join(folder_path, filename)
        print(f"\nProcessing file {i+1}/{len(csv_files)}: {filename}")
        file_data = parse_csv_without_headers(file_path, encoding=encoding)
        all_data.extend(file_data)
        print(f"  Total data rows so far: {len(all_data)}")
    
    if not all_data:
        print("ERROR: No valid data found in CSV files")
        return None
    
    print(f"\nTotal rows collected from all files: {len(all_data)}")
    
    # Group data by 2-minute windows with partitioning
    windows_by_partition = group_by_window_and_partition(all_data)
    
    # Create output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created output directory: {output_dir}")
    
    # Process each partition and save to separate Parquet file
    partition_count = 0
    for year in windows_by_partition:
        for month in windows_by_partition[year]:
            for day in windows_by_partition[year][month]:
                for channel in windows_by_partition[year][month][day]:
                    partition_count += 1
                    
                    # Create directory structure
                    partition_dir = os.path.join(output_dir, f"year={year}", f"month={month}", 
                                                f"day={day}", f"channel={channel}")
                    if not os.path.exists(partition_dir):
                        os.makedirs(partition_dir)
                    
                    # Create mediatree data for this partition
                    windows_data = windows_by_partition[year][month][day][channel]
                    mediatree_data = create_mediatree_data_for_partition(windows_data)
                    
                    # Convert to pandas DataFrame for Parquet conversion
                    df = pd.DataFrame(mediatree_data)
                    
                    # Create a PyArrow Table from the DataFrame
                    table = pa.Table.from_pandas(df)
                    
                    # Save to Parquet file
                    output_file = os.path.join(partition_dir, "data.parquet")
                    pq.write_table(table, output_file)
                    
                    print(f"Created partition {partition_count}: year={year}/month={month}/day={day}/channel={channel}")
                    print(f"  - {len(windows_data)} windows")
                    print(f"  - Saved to {output_file}")
    
    elapsed_time = time.time() - start_time
    print(f"\n{'='*50}")
    print(f"Processing complete!")
    print(f"Created {partition_count} partitioned Parquet files in: {output_dir}")
    print(f"Total processing time: {elapsed_time:.2f} seconds")
    print(f"{'='*50}\n")

if __name__ == "__main__":
    folder_path_2024 = f"csa-belge/2024"
    encoding_2024 = f"cp1252"
    folder_path_2025 = f"csa-belge/2025"
    encoding_2025 = f"utf-8"

    bucket = "mediatree"
    output_dir = "mediatree_output"
    s3_root_folder = "country=belgium"

    print(f"Using timezone: {timezone}")
    print(f"Using bucket: {bucket}")
    print(f"Using s3_root_folder: {s3_root_folder}")
    # process_csv_folder_to_partitioned_parquet(folder_path_2024, output_dir)
    process_csv_folder_to_partitioned_parquet(folder_path_2025, output_dir,encoding=encoding_2025)
    s3_client = get_s3_client()
    upload_folder_to_s3(output_dir,bucket, s3_root_folder, s3_client=s3_client)