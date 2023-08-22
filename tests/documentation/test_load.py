import os
import shutil

import pandas as pd
import pytest

from stdflow import Step
from stdflow.stdflow_doc.documenter import DROPPED, IMPORTED


def setup():
    import pandas as pd

    # Sample datasets
    data1 = {"A": [1, 2, 3], "B": [4, 5, 6]}
    data2 = {"C": [7, 8, 9], "D": [10, 11, 12]}

    df1 = pd.DataFrame(data1)
    df2 = pd.DataFrame(data2)

    # rm -rf "./data/raw/test"
    shutil.rmtree("./data/test/step_raw/", ignore_errors=True)
    # recursive created needed dirs
    os.makedirs("./data/test/step_raw/", exist_ok=True)

    # Save datasets to paths
    df1.to_csv("./data/test/step_raw/basic_data.csv", index=False)
    df2.to_csv("./data/test/step_raw/advanced_data.csv", index=False)


def test_basic_documentation():
    setup()
    step = Step(root="./data", attrs=["test"], step_in="raw", step_out="processed")
    df = step.load(file_name="basic_data.csv", version=None, alias="basic_data")
    step.col_step("basic_data::A", "Loaded from raw data.", ["basic_data::A"])
    step.col_step("basic_data::B", "Loaded from raw data.", ["basic_data::B"])

    assert step.doc.get_documentation("A", "basic_data") == ["Imported", "Loaded from raw data."]
    assert step.doc.get_documentation("B", "basic_data") == ["Imported", "Loaded from raw data."]

    df["A+B"] = df["A"] + df["B"]
    df["A*B"] = df["A"] * df["B"]

    step.col_step("basic_data::A+B", "Column A plus column B.", ["basic_data::A", "basic_data::B"])

    assert step.doc.get_documentation("A", "basic_data") == ["Imported", "Loaded from raw data."]
    assert step.doc.get_documentation("B", "basic_data") == ["Imported", "Loaded from raw data."]
    assert step.doc.get_documentation("A+B", "basic_data") == [
        ["Imported", "Loaded from raw data."],
        ["Imported", "Loaded from raw data."],
        "Column A plus column B.",
    ]

    step.save(df, file_name="basic_data.csv", version=None, alias="basic_data")

    assert step.doc.get_documentation("A+B", "basic_data") == [
        ["Imported", "Loaded from raw data."],
        ["Imported", "Loaded from raw data."],
        "Column A plus column B.",
    ]


def test_drop():
    setup()
    step = Step(root="./data", attrs=["test"], step_in="raw", step_out="processed")
    df = step.load(file_name="basic_data.csv", version=None, alias="basic_data")
    step.col_step("basic_data::A", "Loaded from raw data.", ["basic_data::A"])
    step.col_step("basic_data::B", "Loaded from raw data.", ["basic_data::B"])

    df["A+B"] = df["A"] + df["B"]
    df["A*B"] = df["A"] * df["B"]

    step.col_step("basic_data::A+B", "Column A plus column B.", ["basic_data::A", "basic_data::B"])

    df.drop(columns=["A", "B"], inplace=True)
    step.col_step("A", DROPPED, ["basic_data::A"])

    assert step.doc.get_documentation("A", "basic_data", include_dropped=True) == [
        "Imported",
        "Loaded from raw data.",
        DROPPED,
    ]

    step.save(
        df, file_name="basic_data.csv", version=None, alias="basic_data"
    )  # automatically document drops for non documented dropped columns

    step = Step(root="./data", attrs=["test"], step_in="processed", step_out="processed_2")

    step.load(file_name="basic_data.csv", version=None, alias="basic_data")
    assert step.doc.get_documentation("B", "basic_data", include_dropped=True) == [
        "Imported",
        "Loaded from raw data.",
        DROPPED,
    ]
    assert step.doc.get_documentation("A", "basic_data", include_dropped=True) == [
        "Imported",
        "Loaded from raw data.",
        DROPPED,
    ]


def test_load_twice_same_dataset_same_alias():
    setup()

    # First with the same aliases
    step = Step(
        root="./data",
        attrs=["test"],
        step_in="raw",
        step_out="processed",
        version_in=None,
        version_out=None,
    )

    # Load the dataset the first time
    df1 = step.load(file_name="basic_data.csv", version=None, alias="basic_data")
    df1["A"] = df1["A"] * 2  # Sample operation
    step.col_step("basic_data::A", "Doubled column A values.", ["basic_data::A"])

    # Load the dataset the second time with the same alias
    df2 = step.load(file_name="basic_data.csv", version=None, alias="basic_data")

    df2["A"] = df2["A"] * 2
    step.col_step("basic_data::A", "Doubled column A values again.", ["basic_data::A"])

    assert df1["A"].equals(df2["A"])
    assert step.doc.get_documentation("A", "basic_data") == [
        "Imported",
        "Doubled column A values.",
        "Doubled column A values again.",
    ]


def test_load_twice_same_dataset_diff_alias():
    setup()

    # First with the same aliases
    step = Step(
        root="./data",
        attrs=["test"],
        step_in="raw",
        step_out="processed",
        version_in=None,
        version_out=None,
    )

    df1 = step.load(file_name="basic_data.csv", alias="basic_data_1")
    df2 = step.load(file_name="basic_data.csv", alias="basic_data_2")
    df1["A"] = df1["A"] * 2  # Sample operation
    step.col_step("basic_data_1::A", "Doubled column A values.", ["basic_data_1::A"])

    df2["A"] = df2["A"] * 2
    step.col_step("basic_data_2::A", "Doubled column A values.", ["basic_data_2::A"])

    assert df1["A"].equals(df2["A"])
    assert step.doc.get_documentation("A", "basic_data_1") == [
        "Imported",
        "Doubled column A values.",
    ]
    assert step.doc.get_documentation("A", "basic_data_2") == [
        "Imported",
        "Doubled column A values.",
    ]
    # save the 2
    step.save(df1, file_name="basic_data1.csv", alias="basic_data_1")
    step.save(df2, file_name="basic_data2.csv", alias="basic_data_2")

    step = Step(
        root="./data", attrs=["test"], step_in="processed", step_out="processed_2", version_out=None
    )
    step.load(file_name="basic_data1.csv", alias="basic_data_1")
    step.load(file_name="basic_data2.csv", alias="basic_data_2")
    # documentation check
    assert step.doc.get_documentation("A", "basic_data_1") == [
        "Imported",
        "Doubled column A values.",
    ]
    assert step.doc.get_documentation("A", "basic_data_2") == [
        "Imported",
        "Doubled column A values.",
    ]


def test_split():
    setup()
    step = Step(root="./data", attrs=["test"], step_in="raw", step_out="processed")
    df = step.load(file_name="basic_data.csv", version=None, alias="basic_data")
    split1 = df[["A"]]
    split2 = df[["B"]]

    step.save(split1, file_name="split1.csv", version=None, alias="split1")
    step.save(split2, file_name="split2.csv", version=None, alias="split2")


def test_split_2():
    import stdflow as sf
    setup()
    sf.reset()
    sf.root = "./data"
    sf.attrs = ["test"]
    sf.step_in = "raw"
    sf.step_out = "processed"
    df = sf.load(file_name="basic_data.csv", version=None, alias="basic_data")

    for col in set(df.columns):
        sf.col_step(f"base::{col}", IMPORTED, [])
        assert sf.get_doc(f"base::{col}") == [IMPORTED]
        sf.col_origin(f"base::{col}", "CMI Offline Sales")
        sf.col_step(f"indo::{col}", "country: indonesia", in_cols=[f"base::{col}"])
        sf.col_step(f"india::{col}", "country: india", in_cols=[f"base::{col}"])

        assert sf.get_doc(f"india::{col}") == ['Imported', 'origin: CMI Offline Sales', 'country: india']
        assert sf.get_doc(f"indo::{col}") == ['Imported', 'origin: CMI Offline Sales', 'country: indonesia']

    sf.save(df, file_name="basic_data_indo.csv", version=None, alias="indo")  # TODO check behavior if saving twice with same file name and loading and getting documentation
    sf.save(df, file_name="basic_data_india.csv", version=None, alias="india")

    sf.reset()
    sf.root = "./data"
    sf.attrs = ["test"]
    sf.step_in = "processed"
    df_indo = sf.load(file_name="basic_data_indo.csv", version=None, alias="indo")
    assert sf.get_doc(f"indo::A") == ['Imported', 'origin: CMI Offline Sales', 'country: indonesia']


def test_rename():
    setup()
    step = Step(root="./data", attrs=["test"], step_in="raw", step_out="processed")
    df = step.load(file_name="basic_data.csv", version=None, alias="basic_data")

    df.rename(columns={"A": "A_renamed"}, inplace=True)

    # test assert
    with pytest.raises(ValueError):
        step.col_origin("basic_data::A_renamed", "Renamed column A.")

    step.col_origin("A_renamed", "test", in_cols="A")
    step.col_origin("A_renamed", "test2", ["A_renamed"])
    step.col_origin("A_renamed", "test3", "A_renamed")
    step.col_origin("A_renamed", "test4")

    assert step.get_doc("A_renamed") == [
        "Imported",
        "origin: test",
        "origin: test2",
        "origin: test3",
        "origin: test4",
    ]

    step.save(df, file_name="split1.csv", version=None, alias="df")


# if __name__ == "__main__":
#     test_load()
#     test_load_no_version()
