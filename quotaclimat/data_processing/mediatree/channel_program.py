import modin.pandas as pd
import logging

def get_programs():
    df_programs = pd.read_json('channel_program.json')
    df_programs['start'] = pd.to_datetime(df_programs['start'], format='%H:%M')
    df_programs['end'] = pd.to_datetime(df_programs['end'], format='%Hh%M')
    return df_programs

df_programs = get_programs()

def add_channel_program(df: pd.Dataframe): 
    logging.info("Adding channel program")
    try:
        return (
            df["start"], df["channel_name"]
        ).apply(get_program_name)
    except (Exception) as error:
        logging.error(error)
        return ""

def get_program_name(start: pd.Timestamp, channel_name: str) -> str :
    programs = get_all_programs_for_a_channel(channel_name)
    hourMinute = pd.Timestamp.strptime(start, "%Hh%M")
    weekday, hour, is_weekend = get_weekday_hour(start)
    logging.debug(f"getting program info for {hourMinute} - {channel_name}_{weekday}_{hour}")

def get_weekday_hour(start: pd.Timestamp):
    weekday = start.weekday()
    return weekday, start.hour, is_weekend(weekday)

def is_weekend(weekday: int):
    if weekday >= 5:
        return 'weekday'
    else:
        return 'weekend'

def custom_merge(df_subtitle, df_program):
    merged_rows = []

    for index, row1 in df_subtitle.iterrows():
        # Convert the start time string to a pandas Timestamp object
        start_time = pd.Timestamp.strptime(row1['start'], "%Hh%M")

        matching_rows = df_program[df_program['channel_name'] == row1['channel_name']]
        
        # Iterate over filtered rows of the second DataFrame
        for index, row2 in matching_rows.iterrows():
            # Convert the 'start' and 'end' time strings to pandas Timestamp objects
            start_time2 = pd.Timestamp.strptime(row2['start'], "%Hh%M")
            end_time2 = pd.Timestamp.strptime(row2['end'], "%Hh%M")
            
            # Check if the start time of df_subtitle falls within the time range of df_program
            if start_time >= start_time2 and start_time <= end_time2:
                # If there's a match, merge the rows and add to the merged list
                merged_row = pd.concat([row1, row2])
                merged_rows.append(merged_row)
    
    # Create a new DataFrame from the merged rows
    merged_df = pd.DataFrame(merged_rows)
    
    return merged_df