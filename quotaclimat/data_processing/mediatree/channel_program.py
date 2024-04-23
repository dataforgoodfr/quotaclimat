import modin.pandas as pd
import logging
import os

def get_programs():
    logging.debug("Getting program tv/radio...")
    try:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        json_file_path = os.path.join(current_dir, 'channel_program.json')
        df_programs = pd.read_json(json_file_path, lines=True)

        df_programs['start'] = pd.to_datetime(df_programs['start'], format='%H:%M').dt.tz_localize('Europe/Paris')
        df_programs['end'] = pd.to_datetime(df_programs['end'], format='%H:%M').dt.tz_localize('Europe/Paris')
    except (Exception) as error:
        logging.error("Could not read channel_program.json", error)
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

def compare_weekday(df_program_weekday, start_weekday: int) -> bool:
    logging.debug(f"Comparing weekday {start_weekday} with df_program_weekday value : {df_program_weekday}")
    match isinstance(df_program_weekday, str):
        case False: #int case
            return start_weekday == df_program_weekday
        case True: # string case
            match df_program_weekday:
                case '*': return True
                case 'weekday':
                    return start_weekday < 5
                case 'weekend':
                    return start_weekday > 4
                case _ : return False
    
def get_hour_minute(time: pd.Timestamp):
    # http://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.dt.tz_localize.html
    start_time = pd.to_datetime(time.strftime("%H:%M"), format="%H:%M").tz_localize('Europe/Paris')
    logging.debug(f"time was {time} now is start_time subtitle {start_time}")

    return start_time

# with Monday=0 and Sunday=6.
def get_day_of_week(time: pd.Timestamp):
    start_weekday = int(time.dayofweek)
    logging.debug(f"start_weekday subtitle {start_weekday}")
    return start_weekday

def get_program_with_start_timestamp(df_program: pd.DataFrame, start_time: pd.Timestamp, channel_name: str):
    start_time = get_hour_minute(start_time)
    start_weekday = get_day_of_week(start_time)
    df_program["weekday_mask"] = df_program['weekday'].apply(
        lambda x: compare_weekday(x, start_weekday)
    )     

    matching_rows =  df_program[
                        (df_program['channel_name'] == channel_name) &
                          df_program["weekday_mask"]  &
                         (df_program['start'] <= start_time) &
                         (df_program['end'] > start_time) # stricly > to avoid overlapping programs
                        ]
    
    if(len(matching_rows) > 1):
        logging.error(f"Several programs name for the same channel and time {channel_name} and {start_time} / weekday {start_weekday} - {matching_rows}")
        
    if not matching_rows.empty:
        logging.debug(f"matching_rows {matching_rows}")
        return matching_rows.iloc[0]['program_name'], matching_rows.iloc[0]['program_type']
    else:
        logging.warn(f"Program tv : no matching rows found {channel_name} for weekday {start_weekday} - {start_time}")
        return "", ""

def process_subtitle(row, df_program):
        channel_program, channel_program_type = get_program_with_start_timestamp(df_program, row['start'], row['channel_name'])
        row['channel_program'] = channel_program
        row['channel_program_type'] = channel_program_type
        return row

def merge_program_subtitle(df_subtitle: pd.DataFrame, df_program: pd.DataFrame):
    merged_df = df_subtitle.apply(lambda subtitle : process_subtitle(subtitle, df_program), axis=1)

    return merged_df