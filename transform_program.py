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

# Detailed information for each channel
channel_mapping = {
    "bfmtv": {
        "title": "BFM TV",
        "public": False,
        "infocontinue": True
    },
    "d8": {
        "title": "C8",
        "public": False,
        "infocontinue": False
    },
    "europe1": {
        "title": "Europe 1",
        "public": False,
        "infocontinue": False
    },
    "fr3-idf": {
        "title": "France 3-idf",
        "public": True,
        "infocontinue": False
    },
    "france-culture": {
        "title": "France Culture",
        "public": True,
        "infocontinue": False
    },
    "france-info": {
        "title": "FranceinfoRadio",
        "public": True,
        "infocontinue": True
    },
    "france-inter": {
        "title": "France Inter",
        "public": True,
        "infocontinue": False
    },
    "france2": {
        "title": "France 2",
        "public": True,
        "infocontinue": False
    },
    "france24": {
        "title": "France 24",
        "public": True,
        "infocontinue": True
    },
    "franceinfotv": {
        "title": "France Info",
        "public": True,
        "infocontinue": True
    },
    "itele": {
        "title": "CNews",
        "public": False,
        "infocontinue": True
    },
    "lci": {
        "title": "LCI",
        "public": False,
        "infocontinue": True
    },
    "m6": {
        "title": "M6",
        "public": False,
        "infocontinue": False
    },
    "rfi": {
        "title": "RFI",
        "public": True,
        "infocontinue": False
    },
    "rmc": {
        "title": "RMC",
        "public": False,
        "infocontinue": False
    },
    "rtl": {
        "title": "RTL",
        "public": False,
        "infocontinue": False
    },
    "sud-radio": {
        "title": "Sud Radio",
        "public": False,
        "infocontinue": False
    },
    "tf1": {
        "title": "TF1",
        "public": False,
        "infocontinue": False
    },
    "arte": {
        "title": "Arte",
        "public": True,
        "infocontinue": False
    }
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
        program_data['channel_title'] = channel_mapping[channel_name]['title']
        program_data['public'] = channel_mapping[channel_name]['public']
        program_data['infocontinue'] = channel_mapping[channel_name]['infocontinue']
    else:
        logging.error(f"Unknown channel_name {channel_name}")
        program_data['channel_title'] = channel_name  # Default to channel_name if mapping not found
        program_data['public'] = False  # Default public to False
        program_data['infocontinue'] = False  # Default infocontinue to False
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