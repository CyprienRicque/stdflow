import os
import re


def list_files_regex_all_depth(directory, pattern):
    matched_files = []

    # Walk through each file in the directory
    for root, dirs, files in os.walk(directory):
        for file in files:
            # If the file matches the pattern, add it to the list
            if re.search(pattern, file):
                full_path = os.path.join(root, file)
                relative_path = os.path.relpath(full_path, directory)
                matched_files.append(relative_path)

    return matched_files


def list_files_regex(directory, pattern):
    matched_files = []

    # List each file in the directory
    for file in os.listdir(directory):
        # Construct the full path
        full_path = os.path.join(directory, file)
        # Check if it's a file (not a subdirectory)
        if os.path.isfile(full_path):
            # If the file matches the pattern, add it to the list
            if re.search(pattern, file):
                relative_path = os.path.relpath(full_path, directory)
                matched_files.append(relative_path)

    return matched_files


# using glob
def list_files_glob(directory, pattern):
    import glob

    paths = glob.glob(os.path.join(directory, pattern))
    return [os.path.relpath(path, directory) for path in paths]


def list_excel_files(directory):
    return list_files_glob(directory, "*.xlsx") + list_files_glob(directory, "*.xls")


def list_csv_files(directory):
    return list_files_glob(directory, "*.csv")


if __name__ == "__main__":
    print(list_files_regex_all_depth(directory="./src", pattern=".*\.py$"))
    print(list_files_regex(directory="./src/data_analysis", pattern=".*\.py"))
    print(list_files_glob(directory="./src/data_analysis", pattern="*.py"))
