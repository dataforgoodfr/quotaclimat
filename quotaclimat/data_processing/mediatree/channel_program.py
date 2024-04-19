import modin.pandas as pd
import logging
import os

def get_programs():
    logging.debug("Getting program tv/radio...")
    current_dir = os.path.dirname(os.path.abspath(__file__))
    json_file_path = os.path.join(current_dir, 'channel_program.json')
    df_programs = pd.read_json(json_file_path)
   
    df_programs['start'] = pd.to_datetime(df_programs['start'], format='%H:%M')
    df_programs['end'] = pd.to_datetime(df_programs['end'], format='%H:%M')

    return df_programs

def add_channel_program(df: pd.DataFrame): 
    logging.debug("Adding channel program")
   
    try:
        df_programs = get_programs()
        merged_df = merge_program_subtitle(df, df_programs)
        return merged_df
    except (Exception) as error:
        logging.error("Could not merge program and subtitle df", error)
        raise Exception

# to avoid repeating our channel_program.json file for every day of the week
def is_news_channel(channel_name: str) -> bool:
    news_channels = ['bfmtv', 'itele', 'lci', 'france-info', 'franceinfotv', 'france24']
    return channel_name in news_channels

def compare_weekday(df_program_weekday, start_weekday: int) -> bool:
    logging.info(f"Comparing weekday {start_weekday} with df_program_weekday value : {df_program_weekday}")
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
    

def merge_program_subtitle(df_subtitle: pd.DataFrame, df_program: pd.DataFrame):
    merged_data = []
    for index, subtitle in df_subtitle.iterrows():
        start_time = pd.to_datetime(subtitle['start'].strftime("%H:%M"), format="%H:%M")
        logging.debug(f"start_time {start_time}")
        # with Monday=0 and Sunday=6.
        start_weekday = int(subtitle['start'].dayofweek)
        logging.debug(f"start_weekday {start_weekday}")

        matching_rows = df_program[
                        (df_program['channel_name'] == subtitle['channel_name']) &
                          compare_weekday(df_program['weekday'], start_weekday)  & # fix me
                         (df_program['start'] <= start_time) &
                         (df_program['end'] >= start_time)
                        ]
        if not matching_rows.empty:
            subtitle['program_name'] = matching_rows.iloc[0]['program_name']
            subtitle['program_type'] = matching_rows.iloc[0]['program_type']
        else:
            logging.warn(f"Program tv : no matching rows found {subtitle['channel_name']} for weekday {start_weekday} - {start_time}")
            subtitle['program_name'] = ""
            subtitle['program_type'] = ""
        merged_data.append(subtitle)

    # Convert the list of dictionaries to a DataFrame
    merged_df = pd.DataFrame(merged_data)

    return merged_df