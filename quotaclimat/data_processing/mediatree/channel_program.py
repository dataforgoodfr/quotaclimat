import modin.pandas as pd
import logging
import os

def get_programs():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    json_file_path = os.path.join(current_dir, 'channel_program.json')
    df_programs = pd.read_json(json_file_path)
    df_programs['start'] = pd.to_datetime(df_programs['start'], format='%H:%M')
    df_programs['end'] = pd.to_datetime(df_programs['end'], format='%H:%M')

    logging.info(f"df_programs : {df_programs.head(2).to_string()}")

    return df_programs

df_programs = get_programs()

def add_channel_program(df: pd.DataFrame): 
    logging.debug("Adding channel program")
    
    try:
        merged_df = merge_program_subtitle(df, df_programs)
        return merged_df
    except (Exception) as error:
        logging.error("Could not merge program and subtitle df", error)
        raise Exception

def merge_program_subtitle(df_subtitle: pd.DataFrame, df_program: pd.DataFrame):
    for index, subtitle in df_subtitle.iterrows():
        # TODO fix : compare timezone here
        start_time = pd.to_datetime(subtitle['start'].strftime("%H:%M"), format="%H:%M")
        logging.info(f"start_time {start_time}")
        # with Monday=0 and Sunday=6.
        start_weekday = subtitle['start'].dayofweek
        logging.info(f"start_weekday {start_weekday}")

        logging.info(f"subtitle {df_subtitle.head(2).to_string()}")

        matching_rows = df_program[
                        (df_program['channel_name'] == subtitle['channel_name']) &
                         (df_program['weekday'] == start_weekday)  &
                         (df_program['start'] >= start_time) &
                         (df_program['end'] <= start_time)
                        ]

    logging.info(f"Matching rows {matching_rows}")
    logging.info(f"type {type(matching_rows)}")
    if(len(matching_rows) == 0):
        logging.error("Program tv : no matching rows found")

    return matching_rows