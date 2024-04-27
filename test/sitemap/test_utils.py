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


def list_of_dicts_to_set_of_frozensets(list_of_dicts):
    # Convert each dictionary to a frozenset to make it hashable
    return {frozenset(d.items()) for d in list_of_dicts}

def compare_unordered_lists_of_dicts(list1, list2):
    # Convert each list of dictionaries to a set of frozensets
    set1 = list_of_dicts_to_set_of_frozensets(list1)
    set2 = list_of_dicts_to_set_of_frozensets(list2)

    # Check if the sets are equal
    return set1 == set2