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
from quotaclimat.utils.logger import getLogger
import logging
# execute me with docker compose up testconsole -d / exec run bash
# docker compose exec testconsole bash
# /app/ cd i8n/
# /app/i8n# poetry run python3 srt-to-mediatree-format.py

timezone = "Europe/Berlin"


def convert_real_datetime_to_timestamp(dt: datetime) -> tuple[int, datetime]:
    if dt.tzinfo is None:
        print(f"Datetime is naive (no timezone): {dt}")
        return 0, None

    return int(dt.timestamp()), dt

def convert_datetime_to_timestamp(date_str):
    """Convert date string to UNIX timestamp"""
    try:
        
        dt: datetime = datetime.strptime(date_str, "%d/%m/%Y %H:%M:%S")
        aware_dt = dt.replace(tzinfo=ZoneInfo(timezone))
        # logging.info(f"  Converting date: '{date_str}' to datetime : {aware_dt} - aware_dt.timestamp(): {aware_dt.timestamp()} - dt.timestamp(): {dt.timestamp()}")
        return int(aware_dt.timestamp()), aware_dt
    except ValueError:
        logging.info(f"  ERROR converting date: '{date_str}' - Invalid format")
        return 0, None

def group_by_window_and_partition(data, window_minutes=2, start_column_name='datetime'):
    """
    Group data into time windows of specified minutes
    and organize by year/month/day/channel partitions
    """
    # First level: year/month/day/channel
    partitions = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(list))))
    # Then for each partition: windows
    windows_by_partition = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(list)))))
    
    invalid_count = 0
    
    logging.info(f"Grouping {len(data)} entries into {window_minutes}-minute windows with partitioning")
    
    for item in data:
        timestamp, dt = convert_real_datetime_to_timestamp(item[start_column_name])
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
    
    logging.info(f"Created {partition_count} partitions with {window_count} total time windows")
    logging.info(f"Skipped {invalid_count} entries with invalid timestamps")
    
    return windows_by_partition

def create_mediatree_data_for_partition(windows_data, language='german'):
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

def process_csv_folder_to_partitioned_parquet(folder_path, output_dir="mediatree_output"):
    """
    Process all CSV files in a folder to mediatree format
    with partitioning by year/month/day/channel and output as Parquet
    """
    
    logging.info(f"\n{'='*50}")
    logging.info(f"Starting processing of parquet files in: {folder_path}")
    logging.info(f"{'='*50}\n")
    
    start_time = time.time()
    all_data = []
    
    # Check if folder exists
    if not os.path.exists(folder_path):
        logging.error(f"ERROR: Folder {folder_path} does not exist")
        return None
    
    # Get list of parquet files

    # Group data by 2-minute windows with partitioning
    windows_by_partition = group_by_window_and_partition(all_data)
    
    # Create loal output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        logging.info(f"Created output directory: {output_dir}")
    
    # Process each partition and save to separate Parquet file
    partition_count = 0
    for year in windows_by_partition:
        for month in windows_by_partition[year]:
            for day in windows_by_partition[year][month]:
                for channel in windows_by_partition[year][month][day]:
                    partition_count += 1


                    partition_dir = os.path.join(output_dir, f"year={year}", f"month={month}",
                                                f"day={day}", f"channel={channel}")
                    logging.info(f"Processing partition {partition_count}: {partition_dir}")
                    os.makedirs(partition_dir, exist_ok=True)

                    windows_data = windows_by_partition[year][month][day][channel]
                    mediatree_data = create_mediatree_data_for_partition(windows_data, language='german')
                    df_out = pd.DataFrame(mediatree_data)
                    output_file = os.path.join(partition_dir, "data.parquet")
                    df_out.to_parquet(output_file)
                    logging.info(f"Partition {partition_count}: {partition_dir}")
    
    elapsed_time = time.time() - start_time
    logging.info(f"\n{'='*50}")
    logging.info(f"Processing complete!")
    logging.info(f"Created {partition_count} partitioned Parquet files in: {output_dir}")
    logging.info(f"Total processing time: {elapsed_time:.2f} seconds")
    logging.info(f"{'='*50}\n")

def map_channel_name(channel_name: str) -> str:
    match channel_name:
        case "Das Erste":
            return "daserste"
        case "ZDF":
            return "zdf"
        case "ZDFneo":
            return "zdf-neo"
        case "RTL":
            return "rtl-television"
        case "Sat.1":
            return "sat1"
        case "ProSieben":
            return "prosieben"
        case "Kabel Eins":
            return "kabel-eins"
        case _:
            return ""

def read_and_parse_parquet(path):
    df = pd.read_parquet(path)
    df['datetime'] = df['datetime'].apply(
    lambda dt: dt if dt.tzinfo is not None else pd.Series([dt]).dt.tz_localize("Europe/Berlin", ambiguous='NaT')[0]
    )
    df['plaintext'] = df['text']
    df['channel_name'] = df['channel'].apply(map_channel_name)
    df['channel_title'] = df['channel']
    df.drop(columns=['channel', 'text'], inplace=True)
    df['channel'] = df['channel_name']
    df['year'] = df['datetime'].dt.year
    df['month'] = df['datetime'].dt.month
    df['day'] = df['datetime'].dt.day

    return df
if __name__ == "__main__":
    getLogger()
    logging.info("Starting srt parquet to mediatree parquet format")
    path = os.getenv("PATH_PARQUET", "germany_big.parquet" )
    timezone = os.getenv("TIMEZONE", "Europe/Berlin" )
   
    bucket = os.getenv("BUCKET", "mediatree")
    output_dir: str = os.getenv("OUTPUT_DIR", "mediatree_output")
    s3_root_folder = os.getenv("S3_ROOT_FOLDER", "country=belgium")
    date = os.getenv("DATE", None)
    number_of_previous_days = os.getenv("NUMBER_OF_PREVIOUS_DAYS", 7)

    date_column = "datetime"
    logging.info(f"Using timezone: {timezone}")
    logging.info(f"Using bucket: {bucket}")
    logging.info(f"Using s3_root_folder: {s3_root_folder}")
    logging.info(f"date: {date} - number_of_previous_days: {number_of_previous_days}")


    df = read_and_parse_parquet(path)
    # partition by year month day 
    tmp_output_path = "tmp_output"
    df.to_parquet(tmp_output_path, partition_cols=['year', 'month', 'day'])
    df_tmp = pd.read_parquet(tmp_output_path)

    # keep 7 days from date
    date_datetime = datetime.strptime(date, "%Y-%m-%d")
    # date_datetime - 7 days
    date_datetime_minus_7_days = date_datetime - timedelta(days=number_of_previous_days)
    df_filtered_by_date = df_tmp[df_tmp['date_column'] >= date_datetime_minus_7_days]

    process_csv_folder_to_partitioned_parquet(df_filtered_by_date, output_dir)
    #process_csv_folder_to_partitioned_parquet(folder_path_2025, output_dir)

    df.to_parquet("final_output", partition_cols=['year', 'month', 'day', 'channel'])
    # s3_client = get_s3_client()
    # upload_folder_to_s3(output_dir,bucket, s3_root_folder, s3_client=s3_client)