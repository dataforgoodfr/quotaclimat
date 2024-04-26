import logging
import os

def get_localhost():
    localhost = ""
    if(os.environ.get("ENV") == "docker"):
        localhost ="http://nginxtest:80"
    else:
        localhost = "http://localhost:8000"
    return localhost

def debug_df(df):
    logging.warning("--------------------DEBUG DF-------------------")
    logging.info((1).to_string())
    logging.warning("--------------------DEBUG DF-------------------")