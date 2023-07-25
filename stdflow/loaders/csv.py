import glob
import os
from collections import namedtuple
from collections.abc import MutableMapping

import pandas as pd
from box import Box


# Converter function
def convert_data(data, return_type):
    if return_type == "dict":
        return data
    elif return_type == "list":
        return list(map(namedtuple("NamedDataFrame", ["name", "dataframe"])._make, data.items()))
    elif return_type == "dotdict":
        return Box(data)
    else:
        raise ValueError('return_type must be either "dict", "list", or "dotdict"')


# Load CSV files
def load_csv_files(directory):
    file_pattern = os.path.join(directory, "*.csv")
    file_list = glob.glob(file_pattern)

    data = {}
    for file in file_list:
        key = os.path.splitext(os.path.basename(file))[0]
        data[key] = pd.read_csv(file)
    return data


# Load Excel files
def load_excel_files(directory):
    file_pattern = os.path.join(directory, "*.xlsx")
    file_list = glob.glob(file_pattern)

    data = {}
    for file in file_list:
        key = os.path.splitext(os.path.basename(file))[0]
        data[key] = pd.read_excel(file)
    return data
