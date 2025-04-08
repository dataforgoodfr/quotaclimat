import modin.pandas as pd
import logging
import os
from datetime import datetime
import json
from quotaclimat.data_processing.mediatree.utils import get_epoch_from_datetime, EPOCH__5MIN_MARGIN, EPOCH__1MIN_MARGIN, get_timestamp_from_yyyymmdd,format_hour_minute
from quotaclimat.data_processing.mediatree.channel_program_data import channels_programs
from quotaclimat.data_ingestion.scrap_sitemap import get_consistent_hash
from quotaclimat.data_processing.mediatree.i8n.country import *

def generate_program_id(channel_name, weekday, program_name, program_grid_start) -> str:
    data_str = f"{channel_name}-{weekday}-{program_name}-{program_grid_start}"
    pk: str = get_consistent_hash(data_str)
    logging.debug(f"adding for {data_str} pk {pk}")
    return pk

def get_programs(country: CountryMediaTree = FRANCE):
    logging.debug("Getting program tv/radio...")
    try:
        logging.debug(f"Reading channels_programs of {country}")
        if country.programs is not None:
            df_programs = pd.DataFrame(country.programs)

            df_programs[['start', 'end', 'program_grid_start', 'program_grid_end', 'program_grid_start_str']] = df_programs.apply(lambda x: pd.Series({
                'start': format_hour_minute(x['start']),
                'end': format_hour_minute(x['end']),
                'program_grid_start': get_timestamp_from_yyyymmdd(x['program_grid_start']),
                'program_grid_end': get_timestamp_from_yyyymmdd(x['program_grid_end']),
                'program_grid_start_str': x['program_grid_start']
            }), axis=1)
            return df_programs
        else:
            logging.warning(f"Programs is empty for {country}")
            return None
    except (Exception) as error:
        logging.error(f"Could not read channel_program_data.py {error}")
        raise Exception


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
    logging.debug(f"start_weekday {start_weekday} based on {time}")
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
    logging.debug(f"get_matching_program_hour matching_rows {matching_rows}")
    if(number_of_result > 1): # no margin necessary because programs are next to each others
        closest_result = df_program[
                            (df_program['start'] <= (start_time)) &
                            (df_program['end'] > (start_time)) # stricly > to avoid overlapping programs
        ]
        if(len(closest_result) == 0):
            return matching_rows.head(1)
        else:
            return closest_result
    elif(number_of_result == 0 & number_of_rows_to_filter > 0):
        logging.warning("No results from hour filter")
        return None
    else:
        return matching_rows
    
def get_matching_program_weekday(df_program: pd.DataFrame, start_time: pd.Timestamp, channel_name: str):
    logging.info(f"get_matching_program_weekday {start_time} {channel_name}")

    start_weekday = get_day_of_week(start_time)
    logging.debug(f"start_weekday {start_weekday}")
    if "weekday_mask" in df_program.columns:
        df_program.drop(columns=["weekday_mask"], inplace=True)
    df_program["weekday_mask"] = df_program['weekday'].apply(lambda x: compare_weekday(x, start_weekday))

    matching_rows = df_program[
                        (df_program['channel_name'] == channel_name) &
                        (df_program["weekday_mask"] == True) &
                        (df_program["program_grid_start"] <= start_time) &
                        (df_program["program_grid_end"] >= start_time)
                    ]
    
    # add program id for keywords foreign key
    matching_rows['id'] = matching_rows.apply(lambda x: \
                                              generate_program_id(channel_name, start_weekday, x['program_name'], x['program_grid_start_str'])\
                                        ,axis=1)

    matching_rows.drop(columns=['weekday_mask'], inplace=True)
    matching_rows.drop(columns=['weekday'], inplace=True)
    matching_rows.drop(columns=['program_grid_start'], inplace=True)
    matching_rows.drop(columns=['program_grid_end'], inplace=True)
    matching_rows.drop(columns=['program_grid_start_str'], inplace=True)
    logging.debug(f"matching_rows {matching_rows}")
    if matching_rows.empty:
        logging.warning(f"Program tv : no matching rows found {channel_name} for weekday {start_weekday} - {start_time}")

    return matching_rows

def get_a_program_with_start_timestamp(df_program: pd.DataFrame, start_time: pd.Timestamp, channel_name: str):
    matching_rows = get_matching_program_weekday(df_program, start_time, channel_name=channel_name)
    matching_rows = get_matching_program_hour(matching_rows, start_time)

    if not matching_rows.empty:
        logging.debug(f"return matching_rows{matching_rows.iloc[0]['program_name'], matching_rows.iloc[0]['program_type'], matching_rows.iloc[0]['id']}")
        logging.debug(f"matching_rows {matching_rows}")
        # TODO should return closest to start_time
        return matching_rows.iloc[0]['program_name'], matching_rows.iloc[0]['program_type'], matching_rows.iloc[0]['id']
    else:
        logging.warning(f"no programs found for {channel_name} - {start_time}")
        return "", "", None

def process_subtitle(row, df_program):
        channel_program, channel_program_type, id = get_a_program_with_start_timestamp(df_program, \
                                                                                   row['start'], \
                                                                                   row['channel_name']
                                                                                )
        row['channel_program'] = str(channel_program)
        row['channel_program_type'] = str(channel_program_type)
        row['program_metadata_id'] = str(id)
        return row

def merge_program_subtitle(df_subtitle: pd.DataFrame, df_program: pd.DataFrame):
    merged_df = df_subtitle.apply(lambda subtitle : process_subtitle(subtitle, df_program), axis=1)

    return merged_df

def set_day_with_hour(programs_of_a_day, day: datetime):
    programs_of_a_day['start'] = programs_of_a_day['start'].apply(lambda dt: dt.replace(year=day.year, month=day.month, day=day.day))
    programs_of_a_day['end'] = programs_of_a_day['end'].apply(lambda dt: dt.replace(year=day.year, month=day.month, day=day.day))
    return programs_of_a_day

def get_programs_for_this_day(day: datetime, channel_name: str, df_program: pd.DataFrame, timezone = FRANCE.timezone):
    logging.debug(f"get_programs_for_this_day {day} {channel_name}")
    start_time = pd.Timestamp(day)

    programs_of_a_day = get_matching_program_weekday(df_program, start_time, channel_name)
    logging.debug(f"programs_of_a_day {programs_of_a_day}")
    programs_of_a_day = set_day_with_hour(programs_of_a_day, day)
    logging.debug(f"after programs_of_a_day set_day_with_hour {programs_of_a_day}")
    programs_of_a_day[['start', 'end']] = programs_of_a_day.apply(lambda row: pd.Series({
        'start': get_epoch_from_datetime(row['start'].tz_localize(timezone)),
        'end': get_epoch_from_datetime(row['end'].tz_localize(timezone))
    }), axis=1)
    logging.info(f"Program of {channel_name} : {programs_of_a_day}")
    return programs_of_a_day

def apply_update_program(row, df_programs):
    return get_a_program_with_start_timestamp(df_program=df_programs, start_time=row['start'], channel_name=row['channel_name'])

def update_programs_and_filter_out_of_scope_programs_from_df(df: pd.DataFrame, df_programs: pd.DataFrame) -> pd.DataFrame :
    try:
        if df_programs not None:
            df[['channel_program', 'channel_program_type', 'program_metadata_id']] = df.apply(
                lambda row: apply_update_program(row, df_programs),
                axis=1,
                result_type='expand'
            )
            
            logging.debug("drop out of perimeters rows")
            df = df.dropna(subset=['channel_program'], how='any') # any is for None values
            df.drop(columns=['id'], inplace=True, errors='ignore') # as replaced by program_metadata_id
        else: 
            logging.info(f"Not updating programs because df is none")
        return df
    except Exception as err:
        logging.error(f"Could not update_programs_and_filter_out_of_scope_programs_from_df {err}")
        raise Exception