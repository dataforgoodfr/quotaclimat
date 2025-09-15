# generate program metadata for postgres database "program metadata" content

import json
from datetime import datetime
import sys
import logging
from quotaclimat.data_processing.mediatree.channel_program import generate_program_id
from quotaclimat.data_processing.mediatree.i8n.france.channel_program import channels_programs_france
from quotaclimat.data_processing.mediatree.i8n.germany.channel_program  import channels_programs_germany
from quotaclimat.data_processing.mediatree.i8n.brazil.channel_program import channels_programs_brazil
from quotaclimat.data_processing.mediatree.i8n.poland.channel_program import channels_programs_poland
from quotaclimat.data_processing.mediatree.i8n.spain.channel_program import channels_programs_spain


from quotaclimat.data_processing.mediatree.i8n.country import (
    FRANCE,
    BRAZIL,
    GERMANY,
    POLAND,
    SPAIN,
)

logging.basicConfig(level = logging.INFO)

# Function to calculate duration in minutes between two time strings
def calculate_duration(start_time, end_time):
    fmt = '%H:%M'
    start_dt = datetime.strptime(start_time, fmt)
    end_dt = datetime.strptime(end_time, fmt)
    duration_minutes = (end_dt - start_dt).seconds // 60
    return duration_minutes


output_file_path = "postgres/program_metadata.json"


channel_mapping = {
    # 🇫🇷 France
    "tf1": {
        "title": "TF1",
        "public": False,
        "infocontinue": False,
        "radio": False,
        "country": FRANCE.name
    },
    "france2": {
        "title": "France 2",
        "public": True,
        "infocontinue": False,
        "radio": False,
        "country": FRANCE.name
    },
    "fr3-idf": {
        "title": "France 3-idf",
        "public": True,
        "infocontinue": False,
        "radio": False,
        "country": FRANCE.name
    },
    "m6": {
        "title": "M6",
        "public": False,
        "infocontinue": False,
        "radio": False,
        "country": FRANCE.name
    },
    "arte": {
        "title": "Arte",
        "public": True,
        "infocontinue": False,
        "radio": False,
        "country": FRANCE.name
    },
    "d8": {
        "title": "C8",
        "public": False,
        "infocontinue": False,
        "radio": False,
        "country": FRANCE.name
    },
    "bfmtv": {
        "title": "BFM TV",
        "public": False,
        "infocontinue": True,
        "radio": False,
        "country": FRANCE.name
    },
    "lci": {
        "title": "LCI",
        "public": False,
        "infocontinue": True,
        "radio": False,
        "country": FRANCE.name
    },
    "franceinfotv": {
        "title": "France Info TV",
        "public": True,
        "infocontinue": True,
        "radio": False,
        "country": FRANCE.name
    },
    "itele": {
        "title": "CNews",
        "public": False,
        "infocontinue": True,
        "radio": False,
        "country": FRANCE.name
    },
    "europe1": {
        "title": "Europe 1",
        "public": False,
        "infocontinue": False,
        "radio": True,
        "country": FRANCE.name
    },
    "france-culture": {
        "title": "France Culture",
        "public": True,
        "infocontinue": False,
        "radio": True,
        "country": FRANCE.name
    },
    "france-inter": {
        "title": "France Inter",
        "public": True,
        "infocontinue": False,
        "radio": True,
        "country": FRANCE.name
    },
    "sud-radio": {
        "title": "Sud Radio",
        "public": False,
        "infocontinue": False,
        "radio": True,
        "country": FRANCE.name
    },
    "rmc": {
        "title": "RMC",
        "public": False,
        "infocontinue": False,
        "radio": True,
        "country": FRANCE.name
    },
    "rtl": {
        "title": "RTL",
        "public": False,
        "infocontinue": False,
        "radio": True,
        "country": FRANCE.name
    },
    "france24": {
        "title": "France 24",
        "public": True,
        "infocontinue": True,
        "radio": False,
        "country": FRANCE.name
    },
    "france-info": {
        "title": "FranceinfoRadio",
        "public": True,
        "infocontinue": True,
        "radio": True,
        "country": FRANCE.name
    },
    "rfi": {
        "title": "RFI",
        "public": True,
        "infocontinue": False,
        "radio": True,
        "country": FRANCE.name
    },
    # 🇩🇪 Germany
    "daserste": {
        "title": "Das Erste",
        "public": True,
        "infocontinue": False,
        "radio": False,
        "country": GERMANY.name
    },
    "zdf-neo": {
        "title": "ZDFneo",
        "public": True,
        "infocontinue": False,
        "radio": False,
        "country": GERMANY.name
    },
    "rtl-television": {
        "title": "RTL",
        "public": False,
        "infocontinue": False,
        "radio": False,
        "country": GERMANY.name
    },
    "sat1": {
        "title": "Sat.1",
        "public": False,
        "infocontinue": False,
        "radio": False,
        "country": GERMANY.name
    },
    "prosieben": {
        "title": "ProSieben",
        "public": False,
        "infocontinue": False,
        "radio": False,
        "country": GERMANY.name
    },
    "kabel-eins": {
        "title": "Kabel Eins",
        "public": False,
        "infocontinue": False,
        "radio": False,
        "country": GERMANY.name
    },
    # 🇧🇷 Brazil
    "tvglobo": {
        "title": "TV Globo",
        "public": True,
        "infocontinue": False,
        "radio": False,
        "country": BRAZIL.name
    },
    "tvrecord": {
        "title": "TV Record",
        "public": False,
        "infocontinue": False,
        "radio": False,
        "country": BRAZIL.name
    },
    "sbt": {
        "title": "SBT",
        "public": False,
        "infocontinue": False,
        "radio": False,
        "country": BRAZIL.name
    },
    "redebandeirantes": {
        "title": "Band",
        "public": False,
        "infocontinue": False,
        "radio": False,
        "country": BRAZIL.name
    },
    "jovempan": {
        "title": "Jovem Pan",
        "public": False,
        "infocontinue": False,
        "radio": True,
        "country": BRAZIL.name
    },
    "cnnbrasil": {
        "title": "CNN Brasil",
        "public": False,
        "infocontinue": True,
        "radio": False,
        "country": BRAZIL.name
    },
    # Poland
    "polskie-radio": {
        "title": "Polskie Radio",
        "public": True,
        "infocontinue": False,
        "radio": True,
        "country": POLAND.name
    },
    "radio-zet": {
        "title": "Radio Zet",
        "public": False,
        "infocontinue": False,
        "radio": True,
        "country": POLAND.name
    },
    "eska": {
        "title": "Eska",
        "public": False,
        "infocontinue": False,
        "radio": True,
        "country": POLAND.name
    },
    "polsat": {
        "title": "Polsat",
        "public": False,
        "infocontinue": False,
        "radio": False,
        "country": POLAND.name
    },
    "tvn": {
        "title": "TVN",
        "public": False,
        "infocontinue": False,
        "radio": False,
        "country": POLAND.name
    },
    "tvp": {
        "title": "TVP",
        "public": True,
        "infocontinue": False,
        "radio": False,
        "country": POLAND.name
    },
    "tokfm": {
        "title": "TOKFM",
        "public": False,
        "infocontinue": True,
        "radio": True,
        "country": POLAND.name
    },
    # Spain
    "antenna-3": {
        "title": "Antenna 3",
        "public": False,
        "infocontinue": False,
        "radio": False,
        "country": SPAIN.name
    },
    "rtve-24h": {
        "title": "RTVE 24h",
        "public": True,
        "infocontinue": True,
        "radio": False,
        "country": SPAIN.name
    },
    "rtve-la-1": {
        "title": "RTVE La 1",
        "public": True,
        "infocontinue": False,
        "radio": False,
        "country": SPAIN.name
    },
    "lasexta-news": {
        "title": "LaSexta News",
        "public": False,
        "infocontinue": True,
        "radio": False,
        "country": SPAIN.name
    },
    "telecinco-news": {
        "title": "Telecinco News",
        "public": False,
        "infocontinue": True,
        "radio": False,
        "country": SPAIN.name
    },
    "cuatro-news": {
        "title": "Cuatro News",
        "public": False,
        "infocontinue": True,
        "radio": False,
        "country": SPAIN.name
    },
}
programs = []

channels_programs = channels_programs_france + channels_programs_brazil + channels_programs_germany + channels_programs_poland + channels_programs_spain
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
    print(f"program channel_name {program_data['channel_name']} - channel_mapping[channel_name]['title']")
    if channel_name in channel_mapping:
        program_data['channel_title'] = channel_mapping[channel_name]['title']
        program_data['public'] = channel_mapping[channel_name]['public']
        program_data['infocontinue'] = channel_mapping[channel_name]['infocontinue']
        program_data['radio'] = channel_mapping[channel_name]['radio']
        program_data['country'] = channel_mapping[channel_name]['country']
    else:
        logging.error(f"Unknown channel_name {channel_name}")
        program_data['channel_title'] = channel_name  # Default to channel_name if mapping not found
        program_data['public'] = False  # Default public to False
        program_data['infocontinue'] = False  # Default infocontinue to False
        program_data['radio'] = False  # Default infocontinue to False
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
        new_program_data['weekday'] = int(new_program_data['weekday']) + 1
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