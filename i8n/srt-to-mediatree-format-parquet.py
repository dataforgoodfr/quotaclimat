import sys
import os
import time
import pandas as pd
from sentry_sdk.crons import monitor
from datetime import datetime, timedelta
from collections import defaultdict
from quotaclimat.data_processing.mediatree.s3.s3_utils import upload_folder_to_s3, get_s3_client, read_file_from_s3
from quotaclimat.data_processing.mediatree.i8n.country import GERMANY, get_channel_title_for_name
from zoneinfo import ZoneInfo
import time
from quotaclimat.utils.logger import getLogger
from quotaclimat.utils.sentry import sentry_init
import logging

def convert_real_datetime_to_timestamp(dt: datetime, timezone: str) -> tuple[int, datetime]:
    if dt.tzinfo is None:
        logging.warning(f"Datetime is naive (no timezone): {dt} - it should be in {timezone}, replacing it...")
        dt = dt.replace(tzinfo=ZoneInfo(timezone))

    return int(dt.timestamp()), dt

def convert_datetime_to_timestamp(date_str, timezone):
    """Convert date string to UNIX timestamp"""
    try:
        
        dt: datetime = datetime.strptime(date_str, "%d/%m/%Y %H:%M:%S")
        aware_dt = dt.replace(tzinfo=ZoneInfo(timezone))
        # logging.info(f"  Converting date: '{date_str}' to datetime : {aware_dt} - aware_dt.timestamp(): {aware_dt.timestamp()} - dt.timestamp(): {dt.timestamp()}")
        return int(aware_dt.timestamp()), aware_dt
    except ValueError:
        logging.info(f"  ERROR converting date: '{date_str}' - Invalid format")
        return 0, None

def group_by_window_and_partition(data, window_minutes=2, start_column_name='datetime', timezone='Europe/Berlin'):
    logging.info("""
    Group data into time windows of specified minutes
    and organize by year/month/day/channel partitions
    """)
    # First level: year/month/day/channel
    partitions = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(list))))
    # Then for each partition: windows
    windows_by_partition = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(list)))))
    
    invalid_count = 0
    
    logging.info(f"Grouping {len(data)} entries into {window_minutes}-minute windows with partitioning")
    
    for item in data:
        timestamp, dt = convert_real_datetime_to_timestamp(item[start_column_name], timezone)
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
        channel_title = items[0]['channel_title']
        start = items[0]['start']
        
        # Process words for SRT
        if language == 'french':
            words = split_words_on_apostrophes(combined_plaintext)
        else:
            words = combined_plaintext.split()
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
            "channel_title": channel_title,
            "start": start,
            "plaintext": combined_plaintext
        }
        
        result.append(window_entry)
    
    return result

def process_csv_folder_to_partitioned_parquet(df, output_dir="mediatree_output", timezone='Europe/Berlin'):
    logging.info(f"Creating 2 minute windows..")
    start_time = time.time()
    logging.info(f"converting {len(df)} rows DF to dict...")
    all_data = df.to_dict(orient="records")
    logging.info(f"converted")
    
    # Group data by 2-minute windows with partitioning
    windows_by_partition = group_by_window_and_partition(all_data, timezone=timezone)
    
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
                    df_out.to_parquet(output_file) # enable us to do it small part by small part
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
        
def map_channel_title(channel_name: str,  country=GERMANY) -> str:
    logging.debug(f"Mapping channel title for {channel_name}")
    return map_channel_name(channel_name)

def read_and_parse_parquet(path, timezone, date, bucket_name):
    logging.info(f"Reading parquet file: {path}...")
    start = time.time()
    if bucket_name.startswith("local"):
        logging.info(f"Reading local parquet file: {path}")
        df = pd.read_parquet(path)
    else:
        df = read_file_from_s3(path, bucket_name=bucket_name)
    logging.info(f"Reading parquet done, applying changes...")
    
    #filter out rows where datetime > date
    logging.info(f"filter by date {date} - rows {len(df)}...")
    df = df[df['datetime'] >= date]

    logging.info(f"filter by date done. {len(df)} rows")
    logging.info("adding datetime")
    df['datetime'] = df['datetime'].apply(
    lambda dt: dt if dt.tzinfo is not None else pd.Series([dt]).dt.tz_localize(timezone, ambiguous='NaT')[0]
    )
    df['year'] = df['datetime'].dt.year
    df['month'] = df['datetime'].dt.month
    df['day'] = df['datetime'].dt.day
    logging.info("day,month,year done")

    logging.info(f"text to plaintext...")
    df['plaintext'] = df['text']
    logging.info(f"text is now plaintext")
    logging.info(f"map_channel_name...")
    df['channel_title'] = df['channel']
    logging.info(f"map_channel_name done")
    df['channel_name'] = df['channel_title'].apply(map_channel_title)
    logging.info(f"dropping text...")
    df.drop(columns=['text'], inplace=True)
    logging.info(f"drop text done")
    df['channel'] = df['channel_name']

    end = time.time()
    logging.info(f"Loaded parquet in {end - start:.2f} seconds")
    return df

if __name__ == "__main__":
    with monitor(monitor_slug='srt-germany-to-mediatree'): #https://docs.sentry.io/platforms/python/crons/
        try:
            getLogger()
            sentry_init()
            logging.info("Starting srt parquet to mediatree parquet format")
            path = os.getenv("PATH_PARQUET", "raw_data.parquet")
            timezone = os.getenv("TIMEZONE", "Europe/Berlin")
        
            bucket = os.getenv("BUCKET_NAME", "germany")
            bucket_output = os.getenv("BUCKET_OUTPUT", "test-bucket-mediatree")
            output_dir: str = os.getenv("OUTPUT_DIR", "mediatree_output_test")
            s3_root_folder = os.getenv("S3_ROOT_FOLDER", "country=germany")
            date = os.getenv("DATE", None)
            if date is None:
                logging.info("No date provided, using today's date")
                date = datetime.now().strftime("%Y-%m-%d")
            else:
                logging.info(f"Using date from env : {date}")
            date_datetime = datetime.strptime(date, "%Y-%m-%d")
            number_of_previous_days = int(os.getenv("NUMBER_OF_PREVIOUS_DAYS", 7))
                # date_datetime - 7 days
            date_datetime_minus_7_days = date_datetime - timedelta(days=number_of_previous_days)

            date_column = "datetime" # parquet column name
            logging.info(f"Using timezone: {timezone}")
            logging.info(f"Using bucket input : {bucket} and bucket output: {bucket_output} with s3_root_folder {s3_root_folder}")
            logging.info(f"Using s3_root_folder: {s3_root_folder}")
            logging.info(f"date: {date} - reading number_of_previous_days: {number_of_previous_days}, so : {date_datetime_minus_7_days}")

            df = read_and_parse_parquet(path, timezone=timezone, date=date_datetime_minus_7_days, bucket_name=bucket)

            start_time = time.time()
            for day in df['day'].unique():
                logging.info(f"Processing day: {day} of month {df['month'].unique()} of year {df['year'].unique()}")
                df_day = df[df['day'] == day]
                process_csv_folder_to_partitioned_parquet(df_day, output_dir, timezone=timezone)
            end_time = time.time()
            logging.warning(f"Processing done inside {output_dir} in {end_time - start_time:.2f} seconds")

            # check data quality of the output dir
            logging.info(f"Checking data quality of the output dir...")
            df_check = pd.read_parquet(output_dir)
            logging.info(f"Data quality check done. {len(df_check)} rows")
            logging.info(f"First head: {df_check.head(10)}")
            logging.info(f"Columns: {df_check.columns}")

            logging.info(f"Uploading to s3...")
            s3_client = get_s3_client()
            upload_folder_to_s3(output_dir, bucket_output, s3_root_folder, s3_client=s3_client)
            logging.info(f"Uploading to s3 done")
            sys.exit(0)
        except Exception as e:
            logging.error(f"Error: {e}")
            sys.exit(1)