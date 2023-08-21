import glob
import os

import pandas as pd

# from collections import namedtuple
# from collections.abc import MutableMapping


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
