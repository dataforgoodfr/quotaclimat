# generate program metadata for postgres database "program metadata" content

import json
from datetime import datetime
import sys
import logging
from quotaclimat.data_processing.mediatree.channel_program_data import channels_programs
from quotaclimat.data_processing.mediatree.channel_program import generate_program_id

logging.basicConfig(level = logging.INFO)

# Function to calculate duration in minutes between two time strings
def calculate_duration(start_time, end_time):
    fmt = '%H:%M'
    start_dt = datetime.strptime(start_time, fmt)
    end_dt = datetime.strptime(end_time, fmt)
    duration_minutes = (end_dt - start_dt).seconds // 60
    return duration_minutes


output_file_path = "postgres/program_metadata.json"

# Detailed information for each channel
channel_mapping = {
    "bfmtv": {
        "title": "BFM TV",
        "public": False,
        "infocontinue": True
        ,"radio": False
    },
    "d8": {
        "title": "C8",
        "public": False,
        "infocontinue": False
        ,"radio": False
    },
    "europe1": {
        "title": "Europe 1",
        "public": False,
        "infocontinue": False
        ,"radio": True
    },
    "fr3-idf": {
        "title": "France 3-idf",
        "public": True,
        "infocontinue": False
        ,"radio": False
    },
    "france-culture": {
        "title": "France Culture",
        "public": True,
        "infocontinue": False
        ,"radio": True
    },
    "france-info": {
        "title": "FranceinfoRadio",
        "public": True,
        "infocontinue": True
        ,"radio": True
    },
    "france-inter": {
        "title": "France Inter",
        "public": True,
        "infocontinue": False
        ,"radio": True
    },
    "france2": {
        "title": "France 2",
        "public": True,
        "infocontinue": False
        ,"radio": False
    },
    "france24": {
        "title": "France 24",
        "public": True,
        "infocontinue": True
        ,"radio": False
    },
    "franceinfotv": {
        "title": "France Info TV",
        "public": True,
        "infocontinue": True
        ,"radio": False
    },
    "itele": {
        "title": "CNews",
        "public": False,
        "infocontinue": True
        ,"radio": False
    },
    "lci": {
        "title": "LCI",
        "public": False,
        "infocontinue": True
        ,"radio": False
    },
    "m6": {
        "title": "M6",
        "public": False,
        "infocontinue": False
        ,"radio": False
    },
    "rfi": {
        "title": "RFI",
        "public": True,
        "infocontinue": False
        ,"radio": True
    },
    "rmc": {
        "title": "RMC",
        "public": False,
        "infocontinue": False
        ,"radio": True
    },
    "rtl": {
        "title": "RTL",
        "public": False,
        "infocontinue": False
        ,"radio": True
    },
    "sud-radio": {
        "title": "Sud Radio",
        "public": False,
        "infocontinue": False
        ,"radio": True
    },
    "tf1": {
        "title": "TF1",
        "public": False,
        "infocontinue": False
        ,"radio": False
    },
    "arte": {
        "title": "Arte",
        "public": True,
        "infocontinue": False
        ,"radio": False
    }
}


programs = []

for program_data in channels_programs:
    start_time = program_data['start']
    end_time = program_data['end']
    duration_minutes = calculate_duration(start_time, end_time)
    
    if program_data['program_grid_end'] == "":
        program_data['program_grid_end'] = '2100-01-01'

    # Add duration to the program data
    program_data['duration'] = duration_minutes

    # Map channel_name to channel_title
    channel_name = program_data['channel_name']
    if channel_name in channel_mapping:
        program_data['channel_title'] = channel_mapping[channel_name]['title']
        program_data['public'] = channel_mapping[channel_name]['public']
        program_data['infocontinue'] = channel_mapping[channel_name]['infocontinue']
        program_data['radio'] = channel_mapping[channel_name]['radio']
    else:
        logging.error(f"Unknown channel_name {channel_name}")
        program_data['channel_title'] = channel_name  # Default to channel_name if mapping not found
        program_data['public'] = False  # Default public to False
        program_data['infocontinue'] = False  # Default infocontinue to False
        program_data['radio'] = False  # Default infocontinue to False
    # Handle special cases for weekdays
    weekday = program_data['weekday']
    if weekday == '*':
        # Create separate entries for each weekday (0 to 6)
        for day in range(0, 7):
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
        new_program_data['weekday'] = int(new_program_data['weekday'])
        programs.append(new_program_data)

for program in programs:
    # Generate program ID based on channel_name, weekday, and program_name
    program_id = generate_program_id(program['channel_name'], program['weekday'], program['program_name'], program['program_grid_start'])
    program['id'] = program_id
    
sorted_programs = sorted(programs, key=lambda x: (x['weekday'], x['channel_name']))

with open(output_file_path, 'w', encoding='utf-8') as output_file:
    json.dump(sorted_programs, output_file, ensure_ascii=False, indent=4)

logging.info(f"Output file {output_file_path} has been created and sorted by weekday and channel_name.")
sys.exit(0)