# generate program metadata for postgres database "program metadata" content

import json
from datetime import datetime
import hashlib
import sys
import logging
logging.basicConfig(level = logging.INFO)

# Function to calculate duration in minutes between two time strings
def calculate_duration(start_time, end_time):
    fmt = '%H:%M'
    start_dt = datetime.strptime(start_time, fmt)
    end_dt = datetime.strptime(end_time, fmt)
    duration_minutes = (end_dt - start_dt).seconds // 60
    return duration_minutes

# Function to generate a consistent hash based on channel_name, weekday, and program_name
def generate_program_id(channel_name, weekday, program_name):
    data_str = f"{channel_name}-{weekday}-{program_name}"
    return hashlib.sha256(data_str.encode()).hexdigest()

input_file_path = "quotaclimat/data_processing/mediatree/channel_program.json"
output_file_path = "postgres/program_metadata.json"

channel_mapping = {
    "bfmtv": "BFM TV",
    "d8": "C8",
    "europe1": "Europe 1",
    "fr3-idf": "France 3-idf",
    "france-culture": "France Culture",
    "france-info": "FranceinfoRadio",
    "france-inter": "France Inter",
    "france2": "France 2",
    "france24": "France 24",
    "franceinfotv": "France Info",
    "itele": "CNews",
    "lci": "LCI",
    "m6": "M6",
    "rfi": "RFI",
    "rmc": "RMC",
    "rtl": "RTL",
    "sud-radio": "Sud Radio",
    "tf1": "TF1",
    "arte": "Arte"
}

with open(input_file_path, 'r', encoding='utf-8') as input_file:
    data = input_file.readlines()

programs = []

for line in data:
    program_data = json.loads(line.strip())
    start_time = program_data['start']
    end_time = program_data['end']
    duration_minutes = calculate_duration(start_time, end_time)
    
    # Add duration to the program data
    program_data['duration'] = duration_minutes

    # Map channel_name to channel_title
    channel_name = program_data['channel_name']
    if channel_name in channel_mapping:
        program_data['channel_title'] = channel_mapping[channel_name]
    else:
        program_data['channel_title'] = channel_name  # Default to channel_name if mapping not found

    # Handle special cases for weekdays
    weekday = program_data['weekday']
    if weekday == '*':
        # Create separate entries for each weekday (1 to 7)
        for day in range(1, 8):
            new_program_data = program_data.copy()
            new_program_data['weekday'] = day
            programs.append(new_program_data)
    elif weekday == 'weekend':
        # Create separate entries for weekend days (Saturday and Sunday)
        for day in [6, 7]:
            new_program_data = program_data.copy()
            new_program_data['weekday'] = day
            programs.append(new_program_data)
    elif weekday == 'weekday':
        # Create separate entries for weekdays (Monday to Friday)
        for day in [1, 2, 3, 4, 5]:
            new_program_data = program_data.copy()
            new_program_data['weekday'] = day
            programs.append(new_program_data)
    else:
        # from 1 to 7 to simplify SQL queries 
        new_program_data = program_data.copy()
        new_program_data['weekday'] = new_program_data['weekday'] + 1
        programs.append(new_program_data)

for program in programs:
    # Generate program ID based on channel_name, weekday, and program_name
    program_id = generate_program_id(program['channel_name'], program['weekday'], program['program_name'])
    program['id'] = program_id
    
sorted_programs = sorted(programs, key=lambda x: (x['weekday'], x['channel_name']))

with open(output_file_path, 'w', encoding='utf-8') as output_file:
    json.dump(sorted_programs, output_file, ensure_ascii=False, indent=4)

logging.info(f"Output file {output_file_path} has been created and sorted by weekday and channel_name.")
sys.exit(0)