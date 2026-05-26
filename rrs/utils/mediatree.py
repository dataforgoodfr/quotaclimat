import pandas as pd

def get_start_and_end_of_chunk(start):
    start = pd.to_datetime(start)
    two_minutes = 120
    timestamp = str(int(start.timestamp()))
    timestamp_end = str(int(start.timestamp() + two_minutes))
    return timestamp, timestamp_end


def get_url_mediatree(date, channel) -> str:
    # https://keywords.mediatree.fr/player/?fifo=france-inter&start_cts=1729447079&end_cts=1729447201&position_cts=1729447080
    timestamp, timestamp_end = get_start_and_end_of_chunk(date)
    return f"https://keywords.mediatree.fr/player/?fifo={channel}&start_cts={timestamp}&end_cts={timestamp_end}&position_cts={timestamp}"
