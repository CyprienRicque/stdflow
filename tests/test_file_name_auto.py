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


# Run the test
pytest.main(["-v", "your_test_file.py"])
