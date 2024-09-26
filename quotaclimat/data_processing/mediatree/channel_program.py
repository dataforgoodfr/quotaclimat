import modin.pandas as pd
import logging
import os
from datetime import datetime
import json
from quotaclimat.data_processing.mediatree.utils import get_epoch_from_datetime, EPOCH__5MIN_MARGIN, EPOCH__1MIN_MARGIN

def format_hour_minute(time: str) -> pd.Timestamp:
    date_str = "1970-01-01"
    logging.debug(f"format_hour_minute with : {time}")
    return pd.to_datetime(date_str + " " + time)

def get_programs():
    logging.debug("Getting program tv/radio...")
    try:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        json_file_path = os.path.join(current_dir, 'channel_program.json')
        logging.info(f"Reading {json_file_path}")
        with open(json_file_path, 'r') as file:
            json_data = json.load(file)
            df_programs = pd.DataFrame(json_data)

            df_programs[['start', 'end']] = df_programs.apply(lambda x: pd.Series({
                'start': format_hour_minute(x['start']),
                'end': format_hour_minute(x['end'])
            }), axis=1)

    except (Exception) as error:
        logging.error(f"Could not read channel_program.json {error}")
        raise Exception
    
    return df_programs

def add_channel_program(df: pd.DataFrame): 
    logging.info("Adding channel program")
   
    try:
        df_programs = get_programs()
        merged_df = merge_program_subtitle(df, df_programs)
        return merged_df
    except (Exception) as error:
        logging.error("Could not merge program and subtitle df", error)
        raise Exception

def compare_weekday(df_program_weekday: str, start_weekday: int) -> bool:
    try:
        logging.debug(f"Comparing weekday {start_weekday} with row value : {df_program_weekday}")
        result = False
        match not df_program_weekday.isdigit():
            case False: #int case
                result = (start_weekday == int(df_program_weekday))
            case True: # string case
                match df_program_weekday:
                    case '*':
                        result = True
                    case 'weekday':
                        result = (start_weekday < 5)
                    case 'weekend':
                        result = (start_weekday > 4)
                    case _ :
                        result = False
        logging.debug(f"result compare_weekday: {result}")
        return result
    except Exception as e:
        logging.error(f"Error in compare_weekday: {e}")
        return False

def get_hour_minute(time: pd.Timestamp):
    # http://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.dt.tz_localize.html
    start_time = format_hour_minute(time.strftime("%H:%M"))
    logging.debug(f"time was {time} now is start_time subtitle {start_time}")

    return start_time

# with Monday=0 and Sunday=6.
def get_day_of_week(time: pd.Timestamp) -> int:
    start_weekday = int(time.dayofweek)
    logging.debug(f"start_weekday subtitle {start_weekday}")
    return start_weekday

def get_matching_program_hour(df_program: pd.DataFrame, start_time: pd.Timestamp):
    number_of_rows_to_filter = len(df_program)
    logging.debug(f"df_program {df_program['start']}")
    logging.debug(f"{start_time + pd.Timedelta(seconds=EPOCH__5MIN_MARGIN + EPOCH__1MIN_MARGIN)}")
    logging.debug(f"df_program {df_program['end']}")
    logging.debug(f"number_of_rows_to_filter {number_of_rows_to_filter}")
    start_time = get_hour_minute(start_time)
    matching_rows = df_program[
                         (df_program['start'] <= (start_time + pd.Timedelta(seconds=EPOCH__5MIN_MARGIN + EPOCH__1MIN_MARGIN))) &
                         (df_program['end'] > (start_time - pd.Timedelta(seconds=EPOCH__5MIN_MARGIN + EPOCH__1MIN_MARGIN)))
                    ]
    
    number_of_result = len(matching_rows)
    logging.info(f"matching_rows {matching_rows}")
    if(number_of_result > 1): # no margin necessary because programs are next to each others
        return matching_rows.head(1)
    elif(number_of_result == 0 & number_of_rows_to_filter > 0):
        logging.warning("No results from hour filter")
        return None
    else:
        return matching_rows
    
def get_matching_program_weekday(df_program: pd.DataFrame, start_time: pd.Timestamp, channel_name: str):
    logging.debug(f"get_matching_program_weekday {start_time} {channel_name}")
    start_weekday = get_day_of_week(start_time)
    logging.debug(df_program['weekday'].unique())
    if "weekday_mask" in df_program.columns:
        df_program.drop(columns=["weekday_mask"], inplace=True)
    df_program["weekday_mask"] = df_program['weekday'].apply(lambda x: compare_weekday(x, start_weekday), axis=1)

    matching_rows = df_program[
                        (df_program['channel_name'] == channel_name) &
                        (df_program["weekday_mask"] == True)
                    ]

    matching_rows.drop(columns=['weekday_mask'], inplace=True)
    matching_rows.drop(columns=['weekday'], inplace=True)
    
    if matching_rows.empty:
        logging.warning(f"Program tv : no matching rows found {channel_name} for weekday {start_weekday} - {start_time}")

    return matching_rows

def get_a_program_with_start_timestamp(df_program: pd.DataFrame, start_time: pd.Timestamp, channel_name: str):
    matching_rows = get_matching_program_weekday(df_program, start_time, channel_name)
    matching_rows = get_matching_program_hour(matching_rows, start_time)

    if not matching_rows.empty:
        logging.debug(f"matching_rows {matching_rows}")
        return matching_rows.iloc[0]['program_name'], matching_rows.iloc[0]['program_type']
    else:
        logging.warning(f"no programs found for {channel_name} - {start_time}")
        return "", ""

def process_subtitle(row, df_program):
        channel_program, channel_program_type = get_a_program_with_start_timestamp(df_program, row['start'], row['channel_name'])
        row['channel_program'] = str(channel_program)
        row['channel_program_type'] = str(channel_program_type)
        return row

def merge_program_subtitle(df_subtitle: pd.DataFrame, df_program: pd.DataFrame):
    merged_df = df_subtitle.apply(lambda subtitle : process_subtitle(subtitle, df_program), axis=1)

    return merged_df

def set_day_with_hour(programs_of_a_day, day: datetime):
    programs_of_a_day['start'] = programs_of_a_day['start'].apply(lambda dt: dt.replace(year=day.year, month=day.month, day=day.day))
    programs_of_a_day['end'] = programs_of_a_day['end'].apply(lambda dt: dt.replace(year=day.year, month=day.month, day=day.day))
    return programs_of_a_day

def get_programs_for_this_day(day: datetime, channel_name: str, df_program: pd.DataFrame):
    logging.debug(f"get_programs_for_this_day {day} {channel_name}")
    start_time = pd.Timestamp(day)

    programs_of_a_day = get_matching_program_weekday(df_program, start_time, channel_name)
    logging.debug(f"programs_of_a_day {programs_of_a_day}")
    programs_of_a_day = set_day_with_hour(programs_of_a_day, day)
    logging.debug(f"after programs_of_a_day set_day_with_hour {programs_of_a_day}")
    programs_of_a_day[['start', 'end']] = programs_of_a_day.apply(lambda row: pd.Series({
        'start': get_epoch_from_datetime(row['start'].tz_localize("Europe/Paris")),
        'end': get_epoch_from_datetime(row['end'].tz_localize("Europe/Paris"))
    }), axis=1)
    logging.info(f"Program of {channel_name} : {programs_of_a_day}")
    return programs_of_a_day

def get_channel_title_for_name(channel_name: str) -> str:
    match channel_name:  
        case "tf1":
            return "TF1"
        case "france2":
            return "France 2"
        case "fr3-idf":
            return "France 3-idf"
        case "m6":
            return "M6"
        case "arte":
            return "Arte"
        case "d8":
            return "C8"
        case "bfmtv":
            return "BFM TV"
        case "lci":
            return "LCI"
        case "franceinfotv":
            return "France Info"
        case "itele":
            return "CNews"
        case "europe1":
            return "Europe 1"
        case "france-culture":
            return "France Culture"
        case "france-inter":
            return "France Inter"
        case "sud-radio":
            return "Sud Radio"
        case "rmc":
            return "RMC"
        case "rtl":
            return "RTL"
        case "france24":
            return "France 24"
        case "france-info":
            return "FranceinfoRadio"
        case "rfi":
            return "RFI"
        case _:
            logging.error(f"Channel_name unknown {channel_name}")
            return ""