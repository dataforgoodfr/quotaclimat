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



def add_channel_program(df: pd.DataFrame): 
    logging.debug("Adding channel program")
   
    try:
        df_programs = get_programs()
        merged_df = merge_program_subtitle(df, df_programs)
        return merged_df
    except (Exception) as error:
        logging.error("Could not merge program and subtitle df", error)
        raise Exception

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
                         (df_program['weekday'] == start_weekday)  &
                         (df_program['start'] <= start_time) &
                         (df_program['end'] >= start_time)
                        ]
        # Check if matching rows were found
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

#TODO
# E       Attribute "dtype" are different
# E       [left]:  datetime64[ns, Europe/Paris]
# E       [right]: int64
