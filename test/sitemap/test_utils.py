import logging
import os
import pandas as pd
def get_localhost():
    localhost = ""
    if(os.environ.get("ENV") == "docker"):
        localhost ="http://nginxtest:80"
    else:
        localhost = "http://localhost:8000"
    return localhost

def debug_df(df: pd.DataFrame):
    pd.set_option('display.max_columns', None) 
    logging.warning("--------------------DEBUG DF-------------------")
    logging.info(df.dtypes)
    logging.info(df.head(1))
    logging.warning("--------------------DEBUG DF-------------------")


def list_of_dicts_to_set_of_frozensets(list_of_dicts):
    # Convert each dictionary to a frozenset to make it hashable
    return {frozenset(d.items()) for d in list_of_dicts}

def compare_unordered_lists_of_dicts(list1, list2):
    # Convert each list of dictionaries to a set of frozensets
    set1 = list_of_dicts_to_set_of_frozensets(list1)
    set2 = list_of_dicts_to_set_of_frozensets(list2)

    # Check if the sets are equal
    return set1 == set2