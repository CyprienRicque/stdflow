import os
import re

import stdflow as sf

import pandas as pd
import os
import shutil
import pytest


import stdflow as sf


def test_sf_load():
    # Define a test directory name
    test_dir = "test_dir"

    # Create the test directory
    os.makedirs(test_dir, exist_ok=True)

    # Create a test dataframe
    df = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})

    # Save the test dataframe to csv in the test directory
    df.to_csv(os.path.join(test_dir, "test.csv"), index=False)

    # Load the dataframe using sf.load
    loaded_df = sf.load(root='./', attrs=test_dir)

    # Check if the loaded dataframe is equal to the original one
    pd.testing.assert_frame_equal(df, loaded_df)

    # Remove the test directory after test
    shutil.rmtree(test_dir)


def test_sf_load_glob():
    # Define a test directory name
    test_dir = "test_dir"

    # Create the test directory
    os.makedirs(test_dir, exist_ok=True)

    # Create a test dataframe
    df = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})

    # Save the test dataframe to csv in the test directory
    df.to_csv(os.path.join(test_dir, "test.csv"), index=False)

    # Load the dataframe using sf.load
    loaded_df = sf.load(file_name="*.csv", root='./', attrs=test_dir, file_glob=True)

    # Check if the loaded dataframe is equal to the original one
    pd.testing.assert_frame_equal(df, loaded_df)

    # Remove the test directory after test
    shutil.rmtree(test_dir)


def test_auto_save():
    test_dir = "test_dir"
    os.makedirs(test_dir, exist_ok=True)
    df = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})
    df.to_csv(os.path.join(test_dir, "test.csv"), index=False)

    step = sf.Step(root='./', step_out="coucou", attrs=test_dir)
    # Load the dataframe using sf.load
    loaded_df = step.load()

    # Check if the loaded dataframe is equal to the original one
    pd.testing.assert_frame_equal(df, loaded_df)

    step.save(loaded_df, version=None)
    success = os.path.exists(os.path.join(test_dir, "step_coucou", "test.csv"))
    # Remove the test directory after test
    shutil.rmtree(test_dir)

    assert success, f"File not found: {os.path.join(test_dir, 'step_coucou', 'test.csv')}"



def test_twice_same_load():
    test_dir = "test_dir"
    os.makedirs(test_dir, exist_ok=True)
    df = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})
    df.to_csv(os.path.join(test_dir, "test.csv"), index=False)
    step = sf.Step(root='./', step_out="coucou", attrs=test_dir)

    # Load the dataframe using sf.load
    loaded_df = step.load(file_name="*.csv", file_glob=True)
    loaded_df = step.load(file_name="*.csv", file_glob=True)

    # Check if the loaded dataframe is equal to the original one
    pd.testing.assert_frame_equal(df, loaded_df)

    assert len(step.data_l) == 1
    assert len(step.data_l_in) == 1

    # Remove the test directory after test
    shutil.rmtree(test_dir)


# Run the test
pytest.main(["-v", "your_test_file.py"])
