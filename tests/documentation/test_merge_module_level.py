import os

import pandas as pd
import shutil

import stdflow as sf
import pytest

from stdflow.stdflow_doc.documenter import DROPPED


def setup():
    # Sample datasets
    df1 = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})
    df2 = pd.DataFrame({"C": [7, 8, 9], "D": [10, 11, 12]})

    # rm -rf "./data/raw/test"
    shutil.rmtree("./data/test/step_raw/", ignore_errors=True)
    # recursive created needed dirs
    os.makedirs("./data/test/step_raw/", exist_ok=True)

    # Save datasets to paths
    df1.to_csv("./data/test/step_raw/basic_data.csv", index=False)
    df2.to_csv("./data/test/step_raw/advanced_data.csv", index=False)


def test_export_only():
    # Sample datasets
    df1 = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})
    df2 = pd.DataFrame({"C": [7, 8, 9], "D": [10, 11, 12]})

    # rm -rf "./data/raw/test"
    shutil.rmtree("./data/test/step_raw/", ignore_errors=True)
    # recursive created needed dirs
    os.makedirs("./data/test/step_raw/", exist_ok=True)

    sf.reset()
    sf.root = "./data"
    sf.attrs = ["test"]
    sf.step_in = "raw"
    sf.step_out = "processed"

    # test assert
    with pytest.raises(ValueError):
        sf.col_origin("A", "random_origin")

    # document random origin
    sf.col_step("basic_data::A", "random_origin", [])
    sf.col_step("basic_data::B", "random_origin", [])

    sf.save(df1, file_name="basic_data.csv", version=None, alias="basic_data")
    sf.save(df2, file_name="advanced_data.csv", version=None, alias="advanced_data")

    sf.reset()
    sf.root = "./data"
    sf.attrs = ["test"]
    sf.step_in = "processed"
    sf.step_out = "processed_2"
    
    sf.load(file_name="basic_data.csv", version=None, alias="basic_data")
    assert sf.get_doc("A", "basic_data") == ["random_origin"]

    sf.reset()
    sf.root = "./data"
    sf.attrs = ["test"]
    sf.step_in = "processed"
    sf.step_out = "processed_2"

    sf.load(file_name="basic_data.csv", version=None)
    assert sf.get_doc("A") == ["random_origin"]


def test_merge_new_alias():
    setup()
    sf.reset()
    sf.root = "./data"
    sf.attrs = ["test"]
    sf.step_in = "raw"
    sf.step_out = "processed"

    df_basic = sf.load(
        file_name="basic_data.csv",
        version=None,
        alias="basic_data",
    )
    df_advanced = sf.load(
        file_name="advanced_data.csv",
        version=None,
        alias="advanced_data",
    )

    merged_df = pd.merge(df_basic, df_advanced, left_on="A", right_on="C", how="inner")
    sf.save(
        merged_df,
        file_name="merged_data.csv",
        version=None,
        alias="merged_data",
    )
    # check that the documentation for the merged dataset is empty
    assert sf.get_doc("A", "merged_data") == []
    assert sf.get_doc("B", "merged_data") == []
    assert sf.get_doc("C", "merged_data") == []
    assert sf.get_doc("D", "merged_data") == []
    # save to rely on auto "Created" step
    sf.save(merged_df, file_name="merged_data.csv", alias="merged_data")

    sf.reset()
    sf.root = "./data"
    sf.attrs = "test"
    sf.step_in = "processed"
    sf.step_out = "processed_2"

    sf.load(file_name="merged_data.csv", alias="merged_data")
    # check that the documentation for the merged dataset has created step
    assert sf.get_doc("A", "merged_data") == ["Created"]
    assert sf.get_doc("B", "merged_data") == ["Created"]
    assert sf.get_doc("C", "merged_data") == ["Created"]
    assert sf.get_doc("D", "merged_data") == ["Created"]


def test_merge_same_alias_set_documentation():
    setup()

    sf.reset()
    sf.root = "./data"
    sf.attrs = ["test"]
    sf.step_in = "raw"
    sf.step_out = "processed"
    sf.version_in = None


    df_basic = sf.load(file_name="basic_data.csv", alias="data")
    df_advanced = sf.load(file_name="advanced_data.csv", alias="data")

    df_advanced["D"] = df_advanced["D"] + df_basic["A"]
    sf.col_step("data::D", "Added column A", ["data::D", "data::A"])

    df_basic["A"] = df_basic["A"].astype(int)
    sf.col_step("data::A", "Converted to int", ["data::A"])

    df_advanced["C"] = df_advanced["C"] + df_advanced["D"]
    sf.col_step("data::C", "Added column D", ["data::C", "data::D"])

    merged_df = pd.merge(df_basic, df_advanced, left_on="A", right_on="C", how="inner")

    sf.col_step("data::A", "Merged", ["data::A"])
    sf.col_step("data::C", "Merged", ["data::C"])
    sf.col_step("data::D", "Merged", ["data::D"])

    # check that the documentation for the merged dataset is empty
    assert sf.get_doc("A", "data") == [
        "Imported",
        "Converted to int",
        "Merged",
    ]
    assert sf.get_doc("B", "data") == ["Imported"]
    assert sf.get_doc("D", "data") == [
        ["Imported"],
        ["Imported"],
        "Added column A",
        "Merged",
    ]
    assert sf.get_doc("C", "data") == [
        ["Imported"],
        [["Imported"], ["Imported"], "Added column A"],
        "Added column D",
        "Merged",
    ]
    # save to rely on auto "Created" step
    sf.save(merged_df, file_name="data.csv", alias="data")

    sf.reset()
    sf.root = "./data"
    sf.attrs = ["test"]
    sf.step_in = "processed"
    sf.step_out = "processed_2"


    sf.load(file_name="data.csv", alias="data")
    # check that the documentation for the merged dataset has created step
    assert sf.get_doc("A", "data") == [
        "Imported",
        "Converted to int",
        "Merged",
    ]
    assert sf.get_doc("B", "data") == ["Imported"]
    assert sf.get_doc("D", "data") == [
        ["Imported"],
        ["Imported"],
        "Added column A",
        "Merged",
    ]
    assert sf.get_doc("C", "data") == [
        ["Imported"],
        [["Imported"], ["Imported"], "Added column A"],
        "Added column D",
        "Merged",
    ]


def test_merge_same_alias_same_data():
    setup()

    sf.reset()
    sf.root = "./data"
    sf.attrs = ["test"]
    sf.step_in = "raw"
    sf.step_out = "processed"
    sf.version_in = None

    df_basic = sf.load(file_name="basic_data.csv", alias="data")
    df_advanced = sf.load(file_name="basic_data.csv", alias="adv")

    df_basic["A"] = df_basic["A"] + df_basic["B"]
    sf.col_step("data::A", "Added A to B", ["data::B", "data::A"])

    df_advanced["A"] = df_advanced["A"].astype(int)
    sf.col_step("adv::A", "Converted to int", ["adv::A"])

    assert sf.get_doc("A", "data") == [
        ["Imported"],
        ["Imported"],
        "Added A to B",
    ]
    assert sf.get_doc("B", "data") == ["Imported"]
    assert sf.get_doc("A", "adv") == ["Imported", "Converted to int"]
    assert sf.get_doc("B", "adv") == ["Imported"]

    sf.save(df_basic, file_name="data.csv", alias="data", index=False)
    sf.save(df_advanced, file_name="adv.csv", alias="adv", index=False)

    sf.reset()
    sf.root = "./data"
    sf.attrs = ["test"]
    sf.step_in = "processed"
    sf.step_out = "processed_2"

    df_basic = sf.load(file_name="data.csv", alias="data")
    # test if the following raise ValueError
    with pytest.raises(ValueError):
        df_advanced = sf.load(file_name="adv.csv", alias="data")

    # merged_df = pd.merge(df_basic, df_advanced, left_on="A", right_on="A", how="inner")

    # check that the documentation for the merged dataset is empty
    # assert sf.get_doc("A", "data") == ['Imported', "Converted to int", "Merged"]


def test_ambigous():
    setup()

    sf.reset()
    sf.root = "./data"
    sf.attrs = ["test"]
    sf.step_in = "raw"
    sf.step_out = "processed"
    sf.version_in = None

    df_basic = sf.load(file_name="basic_data.csv", alias="data")
    df_advanced = sf.load(file_name="advanced_data.csv", alias="adv")

    df_basic["A"] = df_basic["A"] + df_advanced["C"]
    sf.col_step("data::A", "Added A to C", ["data::B", "adv::C"])
    # Here data:A exists once from the original A col and once from the one just created from B and C

    with pytest.raises(ValueError):
        assert sf.get_doc("A", "data") == [
            ["Imported"],
            ["Imported"],
            "Added A to C",
        ]


def test_auto_propagation():
    setup()
    sf.reset()
    sf.root = "./data"
    sf.attrs = ["test"]
    sf.step_in = "raw"
    sf.step_out = "processed"
    sf.version_in = None


    df_basic = sf.load(file_name="basic_data.csv", alias="data")
    df_advanced = sf.load(file_name="advanced_data.csv", alias="adv")

    df_basic["A"] = df_basic["A"] + df_advanced["C"]
    sf.col_step("data::A", "Added A to C", ["A", "C"])

    df_advanced["D"] = df_advanced["D"].astype(int)
    sf.col_step("D", "Converted to int", ["D"])

    assert sf.get_doc("A", "data") == [
        ["Imported"],
        ["Imported"],
        "Added A to C",
    ]
    assert sf.get_doc("B", "data") == ["Imported"]
    assert sf.get_doc("D", "adv") == ["Imported", "Converted to int"]
    assert sf.get_doc("C", "adv") == ["Imported"]

    sf.save(df_basic, file_name="data.csv", alias="data", index=False)
    sf.save(df_advanced, file_name="adv.csv", alias="adv", index=False)

    # Step 2

    sf.reset()
    sf.root = "./data"
    sf.attrs = ["test"]
    sf.step_in = "processed"
    sf.step_out = "processed_2"

    df_basic = sf.load(file_name="data.csv")
    df_advanced = sf.load(file_name="adv.csv")

    sf.save(df_basic, file_name="data.csv", index=False)
    sf.save(df_advanced, file_name="adv.csv", index=False)

    # Step 3

    sf.reset()
    sf.root = "./data"
    sf.attrs = ["test"]
    sf.step_in = "processed_2"
    sf.step_out = "processed_3"


    df_basic = sf.load(file_name="data.csv", alias="data")
    df_advanced = sf.load(file_name="adv.csv", alias="adv")

    assert sf.get_doc("A", "data") == [
        ["Imported"],
        ["Imported"],
        "Added A to C",
    ]
    assert sf.get_doc("B", "data") == ["Imported"]
    assert sf.get_doc("D", "adv") == ["Imported", "Converted to int"]
    assert sf.get_doc("C", "adv") == ["Imported"]

    sf.save(df_basic, file_name="data.csv", index=False)
    sf.save(df_advanced, file_name="adv.csv", index=False)

    # assert sf.get_doc("A", "data") == [['Imported'], ['Imported'], 'Added A to B']


def test_auto_propagation2():
    setup()

    sf.reset()
    sf.root = "./data"
    sf.attrs = ["test"]
    sf.step_in = "raw"
    sf.step_out = "processed"
    sf.version_in = None

    df_basic = sf.load(file_name="basic_data.csv", alias="data")
    df_advanced = sf.load(file_name="advanced_data.csv", alias="adv")

    df_basic["A"] = df_basic["A"] + df_advanced["C"]
    sf.col_step("data::A", "Added A to C", ["data::A", "adv::C"])

    df_advanced["D"] = df_advanced["D"].astype(int)
    sf.col_step("adv::D", "Converted to int", ["adv::D"])

    assert sf.get_doc("A", "data") == [
        ["Imported"],
        ["Imported"],
        "Added A to C",
    ]
    assert sf.get_doc("B", "data") == ["Imported"]
    assert sf.get_doc("D", "adv") == ["Imported", "Converted to int"]
    assert sf.get_doc("C", "adv") == ["Imported"]

    sf.save(df_basic, file_name="data.csv", alias="data", index=False)
    sf.save(df_advanced, file_name="adv.csv", alias="adv", index=False)

    # Step 2

    sf.reset()
    sf.root = "./data"
    sf.attrs = ["test"]
    sf.step_in = "processed"
    sf.step_out = "processed_2"


    df_basic = sf.load(file_name="data.csv")

    sf.save(df_basic, file_name="data2.csv", index=False)

    # Step 3

    sf.reset()
    sf.root = "./data"
    sf.attrs = ["test"]
    sf.step_in = "processed_2"
    sf.step_out = "processed_3"


    df_basic = sf.load(file_name="data2.csv", alias="data")

    assert sf.get_doc("A", "data") == [
        ["Imported"],
        ["Imported"],
        "Added A to C",
    ]
    assert sf.get_doc("B", "data") == ["Imported"]


def test_save_with_alias_load_without():
    sf.reset()
    sf.root = "./data"
    sf.attrs = ["test"]
    sf.step_in = "raw"
    sf.step_out = "processed"
    sf.version = None

    df_basic = sf.load(file_name="basic_data.csv")

    sf.save(df_basic, file_name="data.csv", alias="data", index=False)


def test_merge_new_alias_with_origin():
    setup()

    sf.reset()
    sf.root = "./data"
    sf.attrs = ["test"]
    sf.step_in = "raw"
    sf.step_out = "processed"

    df_basic = sf.load(file_name="basic_data.csv", alias="data")
    df_advanced = sf.load(file_name="advanced_data.csv", alias="data")

    sf.col_step("data::A", "to int", ["data::A"])
    sf.col_step("data::A", "to int 2", ["data::A"])
    sf.col_origin("A", "basic_data.csv")
    sf.col_origin("B", "basic_data.csv")
    sf.col_origin("C", "advanced_data.csv")
    sf.col_origin("D", "advanced_data.csv")

    df_basic["AD"] = df_basic["A"] + df_advanced["D"]

    assert sf.get_origins_raw("A", "data") == ["origin: basic_data.csv"]

    sf.col_step("data::AD", "Added A to D", ["data::A", "data::D"])

    merged_df = pd.merge(df_basic, df_advanced, left_on="A", right_on="C", how="inner")

    sf.save(merged_df, file_name="merged_data.csv", version=None, alias="data")

    # Check origins
    assert sf.get_origins_raw("A", "data") == ["origin: basic_data.csv"]
    assert sf.get_origins_raw("B", "data") == ["origin: basic_data.csv"]
    assert sf.get_origins_raw("C", "data") == ["origin: advanced_data.csv"]
    assert sf.get_origins_raw("D", "data") == ["origin: advanced_data.csv"]
    assert sf.get_origins_raw("AD", "data") == [
        ["origin: basic_data.csv"],
        ["origin: advanced_data.csv"],
    ]

    assert sf.get_origins("A", "data") == ["basic_data.csv"]
    assert sf.get_origins("B", "data") == ["basic_data.csv"]
    assert sf.get_origins("C", "data") == ["advanced_data.csv"]
    assert sf.get_origins("D", "data") == ["advanced_data.csv"]
    assert sf.get_origins("AD", "data") == ["basic_data.csv", "advanced_data.csv"]
