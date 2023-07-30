import os
import pandas as pd
import pytest


def test_create_fake_data():
    # Check for France dataset
    from stdflow import Step

    step = Step(
        exec_file_path="tests/twitter_data_flow/notebooks/generate.ipynb",
        exec_variables={
            "country": "spain",
            "version": "test"
        },
    )
    step.run()

    # assert path ./data/twitter/france/step_raw/v_test/ exists
    assert os.path.exists("./data/twitter/france/step_raw/v_test/"), "France data does not exist."

    step = Step(root="./data", step_in="raw")
    df = step.load(attrs=["twitter", "france"], version="test")
    assert list(df.columns) == ["tweEts", "SeX"], "France data columns are incorrect."
    assert len(df) == 100, "France data does not have the correct number of rows."

    df = step.load(attrs=["twitter", "china"], version="test")
    assert list(df.columns) == ["tweEts", "SeX"], "China data columns are incorrect."
    assert len(df) == 100, "China data does not have the correct number of rows."

    df = step.load(attrs=["twitter", "spain"], version="test")
    assert list(df.columns) == ["tweetS", "sEx"], "Spain data columns are incorrect."
    assert len(df) == 100, "Spain data does not have the correct number of rows."
