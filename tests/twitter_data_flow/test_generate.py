import os
import pandas as pd
import pytest

from stdflow import StepRunner, Step


def test_create_fake_data():
    # Check for France dataset

    print("exists?", os.path.exists("tests/twitter_data_flow/notebooks/load/generate.ipynb"))
    step = StepRunner(
        file_path="tests/twitter_data_flow/notebooks/load/generate.ipynb",
        variables={
            "country": "spain",
            "version": "test"
        },
    )
    step.run()

    # assert path ./data/twitter/france/step_raw/v_test/ exists
    assert os.path.exists("./tests/data/twitter/france/step_raw/v_test/"), "France data does not exist."

    step = Step(root="./tests/data", step_in="raw")
    df = step.load(attrs=["twitter", "france"], version="test")
    assert list(df.columns) == ["tweEts", "SeX"], "France data columns are incorrect."
    assert len(df) == 100, "France data does not have the correct number of rows."

    df = step.load(attrs=["twitter", "china"], version="test")
    assert list(df.columns) == ["tweEts", "SeX"], "China data columns are incorrect."
    assert len(df) == 100, "China data does not have the correct number of rows."

    df = step.load(attrs=["twitter", "spain"], version="test")
    assert list(df.columns) == ["tweetS", "sEx"], "Spain data columns are incorrect."
    assert len(df) == 100, "Spain data does not have the correct number of rows."
