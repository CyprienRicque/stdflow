import os
import shutil

import pandas as pd
import pytest

from stdflow import Step
from stdflow.stdflow_doc.documenter import DROP, CREATE, NO_DETAILS, IMPORT, ORIGIN_NAME


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

    step = Step(root="./data", attrs=["test"], step_in="raw", step_out="processed")

    # test assert
    with pytest.raises(ValueError):
        step.col_origin_name("A", "random_origin")

    # document random origin
    step.col_step("basic_data::A", "random_origin", [])
    step.col_step("basic_data::B", "random_origin", [])

    step.save(df1, file_name="basic_data.csv", version=None, alias="basic_data")
    step.save(df2, file_name="advanced_data.csv", version=None, alias="advanced_data")

    step = Step(root="./data", attrs=["test"], step_in="processed", step_out="processed_2")
    step.load(file_name="basic_data.csv", version=None, alias="basic_data")
    assert step.get_doc("A", "basic_data") == ["random_origin"]

    step = Step(root="./data", attrs=["test"], step_in="processed", step_out="processed_2")
    step.load(file_name="basic_data.csv", version=None, alias=None)
    assert step.get_doc("A") == ["random_origin"]


def test_merge_new_alias():
    setup()
    step = Step(root="./data", attrs=["test"], step_in="raw", step_out="processed")
    df_basic = step.load(
        file_name="basic_data.csv",
        version=None,
        alias="basic_data",
    )
    df_advanced = step.load(
        file_name="advanced_data.csv",
        version=None,
        alias="advanced_data",
    )

    merged_df = pd.merge(df_basic, df_advanced, left_on="A", right_on="C", how="inner")
    step.save(
        merged_df,
        file_name="merged_data.csv",
        version=None,
        alias="merged_data",
    )
    # check that the documentation for the merged dataset is empty
    assert step.doc.get_documentation("A", "merged_data") == []
    assert step.doc.get_documentation("B", "merged_data") == []
    assert step.doc.get_documentation("C", "merged_data") == []
    assert step.doc.get_documentation("D", "merged_data") == []
    # save to rely on auto "Created" step
    step.save(merged_df, file_name="merged_data.csv", alias="merged_data")

    step = Step(root="./data", attrs=["test"], step_in="processed", step_out="processed_2")
    step.load(file_name="merged_data.csv", alias="merged_data")
    # check that the documentation for the merged dataset has created step
    assert step.doc.get_documentation("A", "merged_data") == [CREATE + NO_DETAILS]
    assert step.doc.get_documentation("B", "merged_data") == [CREATE + NO_DETAILS]
    assert step.doc.get_documentation("C", "merged_data") == [CREATE + NO_DETAILS]
    assert step.doc.get_documentation("D", "merged_data") == [CREATE + NO_DETAILS]


def test_merge_same_alias_set_documentation():
    setup()
    step = Step(root="./data", attrs=["test"], step_in="raw", step_out="processed", version_in=None)
    df_basic = step.load(file_name="basic_data.csv", alias="data")
    df_advanced = step.load(file_name="advanced_data.csv", alias="data")

    df_advanced["D"] = df_advanced["D"] + df_basic["A"]
    step.col_step("data::D", "Added column A", ["data::D", "data::A"])

    df_basic["A"] = df_basic["A"].astype(int)
    step.col_step("data::A", "Converted to int", ["data::A"])

    df_advanced["C"] = df_advanced["C"] + df_advanced["D"]
    step.col_step("data::C", "Added column D", ["data::C", "data::D"])

    merged_df = pd.merge(df_basic, df_advanced, left_on="A", right_on="C", how="inner")

    step.col_step("data::A", "Merged", ["data::A"])
    step.col_step("data::C", "Merged", ["data::C"])
    step.col_step("data::D", "Merged", ["data::D"])

    # check that the documentation for the merged dataset is empty
    assert step.doc.get_documentation("A", "data") == [IMPORT + NO_DETAILS, "Converted to int", "Merged"]
    assert step.doc.get_documentation("B", "data") == [IMPORT + NO_DETAILS]
    assert step.doc.get_documentation("D", "data") == [
        [IMPORT + NO_DETAILS],
        [IMPORT + NO_DETAILS],
        "Added column A",
        "Merged",
    ]
    assert step.doc.get_documentation("C", "data") == [
        [IMPORT + NO_DETAILS],
        [[IMPORT + NO_DETAILS], [IMPORT + NO_DETAILS], "Added column A"],
        "Added column D",
        "Merged",
    ]
    # save to rely on auto "Created" step
    step.save(merged_df, file_name="data.csv", alias="data")

    step = Step(root="./data", attrs=["test"], step_in="processed", step_out="processed_2")
    step.load(file_name="data.csv", alias="data")
    # check that the documentation for the merged dataset has created step
    assert step.doc.get_documentation("A", "data") == [IMPORT + NO_DETAILS, "Converted to int", "Merged"]
    assert step.doc.get_documentation("B", "data") == [IMPORT + NO_DETAILS]
    assert step.doc.get_documentation("D", "data") == [
        [IMPORT + NO_DETAILS],
        [IMPORT + NO_DETAILS],
        "Added column A",
        "Merged",
    ]
    assert step.doc.get_documentation("C", "data") == [
        [IMPORT + NO_DETAILS],
        [[IMPORT + NO_DETAILS], [IMPORT + NO_DETAILS], "Added column A"],
        "Added column D",
        "Merged",
    ]


def test_merge_same_alias_same_data():
    setup()
    step = Step(root="./data", attrs=["test"], step_in="raw", step_out="processed", version_in=None)
    df_basic = step.load(file_name="basic_data.csv", alias="data")
    df_advanced = step.load(file_name="basic_data.csv", alias="adv")

    df_basic["A"] = df_basic["A"] + df_basic["B"]
    step.col_step("data::A", "Added A to B", ["data::B", "data::A"])

    df_advanced["A"] = df_advanced["A"].astype(int)
    step.col_step("A", "Converted to int", ["A"], alias='adv')

    assert step.doc.get_documentation("A", "data") == [[IMPORT + NO_DETAILS], [IMPORT + NO_DETAILS], "Added A to B"]
    assert step.doc.get_documentation("B", "data") == [IMPORT + NO_DETAILS]
    assert step.doc.get_documentation("A", "adv") == [IMPORT + NO_DETAILS, "Converted to int"]
    assert step.doc.get_documentation("B", "adv") == [IMPORT + NO_DETAILS]

    step.save(df_basic, file_name="data.csv", alias="data", index=False)
    step.save(df_advanced, file_name="adv.csv", alias="adv", index=False)

    step = Step(root="./data", attrs=["test"], step_in="processed", step_out="processed_2")

    df_basic = step.load(file_name="data.csv", alias="data")
    # test if the following raise ValueError
    with pytest.raises(ValueError):
        df_advanced = step.load(file_name="adv.csv", alias="data")

    # merged_df = pd.merge(df_basic, df_advanced, left_on="A", right_on="A", how="inner")

    # check that the documentation for the merged dataset is empty
    # assert step.documentation.get_documentation("A", "data") == [IMPORT + NO_DETAILS, "Converted to int", "Merged"]


def test_ambigous():
    setup()
    step = Step(root="./data", attrs=["test"], step_in="raw", step_out="processed", version_in=None)
    df_basic = step.load(file_name="basic_data.csv", alias="data")
    df_advanced = step.load(file_name="advanced_data.csv", alias="adv")

    df_basic["A"] = df_basic["A"] + df_advanced["C"]
    step.col_step("data::A", "Added A to C", ["data::B", "adv::C"])
    # Here data:A exists once from the original A col and once from the one just created from B and C

    with pytest.raises(ValueError):
        assert step.doc.get_documentation("A", "data") == [
            [IMPORT + NO_DETAILS],
            [IMPORT + NO_DETAILS],
            "Added A to C",
        ]


def test_auto_propagation():
    setup()
    step = Step(root="./data", attrs=["test"], step_in="raw", step_out="processed", version_in=None)
    df_basic = step.load(file_name="basic_data.csv", alias="data")
    df_advanced = step.load(file_name="advanced_data.csv", alias="adv")

    df_basic["A"] = df_basic["A"] + df_advanced["C"]
    step.col_step("data::A", "Added A to C", ["A", "C"])

    df_advanced["D"] = df_advanced["D"].astype(int)
    step.col_step("D", "Converted to int", ["D"])

    assert step.doc.get_documentation("A", "data") == [[IMPORT + NO_DETAILS], [IMPORT + NO_DETAILS], "Added A to C"]
    assert step.doc.get_documentation("B", "data") == [IMPORT + NO_DETAILS]
    assert step.doc.get_documentation("D", "adv") == [IMPORT + NO_DETAILS, "Converted to int"]
    assert step.doc.get_documentation("C", "adv") == [IMPORT + NO_DETAILS]

    step.save(df_basic, file_name="data.csv", alias="data", index=False)
    step.save(df_advanced, file_name="adv.csv", alias="adv", index=False)

    # Step 2

    step = Step(root="./data", attrs=["test"], step_in="processed", step_out="processed_2")

    df_basic = step.load(file_name="data.csv", alias=None)
    df_advanced = step.load(file_name="adv.csv", alias=None)

    step.save(df_basic, file_name="data.csv", index=False, alias=None)
    step.save(df_advanced, file_name="adv.csv", index=False, alias=None)

    # Step 3

    step = Step(root="./data", attrs=["test"], step_in="processed_2", step_out="processed_3")

    df_basic = step.load(file_name="data.csv", alias="data")
    df_advanced = step.load(file_name="adv.csv", alias="adv")

    assert step.doc.get_documentation("A", "data") == [[IMPORT + NO_DETAILS], [IMPORT + NO_DETAILS], "Added A to C"]
    assert step.doc.get_documentation("B", "data") == [IMPORT + NO_DETAILS]
    assert step.doc.get_documentation("D", "adv") == [IMPORT + NO_DETAILS, "Converted to int"]
    assert step.doc.get_documentation("C", "adv") == [IMPORT + NO_DETAILS]

    step.save(df_basic, file_name="data.csv", index=False)
    step.save(df_advanced, file_name="adv.csv", index=False)

    # assert step.documentation.get_documentation("A", "data") == [[IMPORT + NO_DETAILS], [IMPORT + NO_DETAILS], 'Added A to B']


def test_auto_propagation2():
    setup()
    step = Step(root="./data", attrs=["test"], step_in="raw", step_out="processed", version_in=None)
    df_basic = step.load(file_name="basic_data.csv", alias="data")
    df_advanced = step.load(file_name="advanced_data.csv", alias="adv")

    df_basic["A"] = df_basic["A"] + df_advanced["C"]
    step.col_step("data::A", "Added A to C", ["data::A", "adv::C"])

    df_advanced["D"] = df_advanced["D"].astype(int)
    step.col_step("adv::D", "Converted to int", ["adv::D"])

    assert step.doc.get_documentation("A", "data") == [[IMPORT + NO_DETAILS], [IMPORT + NO_DETAILS], "Added A to C"]
    assert step.doc.get_documentation("B", "data") == [IMPORT + NO_DETAILS]
    assert step.doc.get_documentation("D", "adv") == [IMPORT + NO_DETAILS, "Converted to int"]
    assert step.doc.get_documentation("C", "adv") == [IMPORT + NO_DETAILS]

    step.save(df_basic, file_name="data.csv", alias="data", index=False)
    step.save(df_advanced, file_name="adv.csv", alias="adv", index=False)

    # Step 2

    step = Step(root="./data", attrs=["test"], step_in="processed", step_out="processed_2")

    df_basic = step.load(file_name="data.csv", alias=None)

    step.save(df_basic, file_name="data2.csv", index=False, alias=None)

    # Step 3

    step = Step(root="./data", attrs=["test"], step_in="processed_2", step_out="processed_3")

    df_basic = step.load(file_name="data2.csv", alias="data")

    assert step.doc.get_documentation("A", "data") == [[IMPORT + NO_DETAILS], [IMPORT + NO_DETAILS], "Added A to C"]
    assert step.doc.get_documentation("B", "data") == [IMPORT + NO_DETAILS]


def test_save_with_alias_load_without():
    step = Step(root="./data", attrs=["test"], step_in="raw", step_out="processed", version_in=None)
    df_basic = step.load(file_name="basic_data.csv")

    step.save(df_basic, file_name="data.csv", alias="data", index=False)


def test_merge_new_alias_with_origin():
    setup()
    step = Step(root="./data", attrs=["test"], step_in="raw", step_out="processed")

    df_basic = step.load(file_name="basic_data.csv", alias="data")
    df_advanced = step.load(file_name="advanced_data.csv", alias="data")

    step.col_step("data::A", "to int", ["data::A"])
    step.col_step("data::A", "to int 2", ["data::A"])
    step.col_origin_name("A", "basic_data.csv")
    step.col_origin_name("B", "basic_data.csv")
    step.col_origin_name("C", "advanced_data.csv")
    step.col_origin_name("D", "advanced_data.csv")
    df_basic["AD"] = df_basic["A"] + df_advanced["D"]

    assert step.get_origin_names_raw("A", "data") == [ORIGIN_NAME + "basic_data.csv"]

    step.col_step("data::AD", "Added A to D", ["data::A", "data::D"])

    merged_df = pd.merge(df_basic, df_advanced, left_on="A", right_on="C", how="inner")

    step.save(merged_df, file_name="merged_data.csv", version=None, alias="data")

    # Check origins
    assert step.get_origin_names_raw("A", "data") == [ORIGIN_NAME + "basic_data.csv"]
    assert step.get_origin_names_raw("B", "data") == [ORIGIN_NAME + "basic_data.csv"]
    assert step.get_origin_names_raw("C", "data") == [ORIGIN_NAME + "advanced_data.csv"]
    assert step.get_origin_names_raw("D", "data") == [ORIGIN_NAME + "advanced_data.csv"]
    assert step.get_origin_names_raw("AD", "data") == [
        [ORIGIN_NAME + "basic_data.csv"],
        [ORIGIN_NAME + "advanced_data.csv"],
    ]

    assert step.get_origin_names("A", "data") == ["basic_data.csv"]
    assert step.get_origin_names("B", "data") == ["basic_data.csv"]
    assert step.get_origin_names("C", "data") == ["advanced_data.csv"]
    assert step.get_origin_names("D", "data") == ["advanced_data.csv"]
    assert step.get_origin_names("AD", "data") == ["basic_data.csv", "advanced_data.csv"]


def test_repeated_load():
    setup()
    step = Step(root="./data", attrs=["test"], step_in="raw", step_out="processed")

    df_basic = step.load(file_name="basic_data.csv")
    df_basic = step.load(file_name="basic_data.csv")
    df_basic = step.load(file_name="basic_data.csv")
    df_basic = step.load(file_name="basic_data.csv")
    df_basic = step.load(file_name="basic_data.csv", alias=None)
    assert step.get_doc("A") == [IMPORT + NO_DETAILS]

    step.col_step("A", "to int", ["A"])
    assert step.get_doc("A") == [IMPORT + NO_DETAILS, "to int"]

    df_basic = step.load(file_name="basic_data.csv")
    df_basic = step.load(file_name="basic_data.csv")
    assert step.get_doc("A") == [IMPORT + NO_DETAILS, "to int"]


def test_cols_as_index():
    setup()
    step = Step(root="./data", attrs=["test"], step_in="raw")
    df = step.load(file_name="basic_data.csv", alias="data")

    cols = df.columns
    df.A = df.A + df.B

    assert isinstance(cols, pd.Index)
    step.col_step("data::A", "sum over all cols", cols)

    assert step.get_doc("A") == [[IMPORT + NO_DETAILS], [IMPORT + NO_DETAILS], "sum over all cols"]


def test_cols_as_series():
    setup()
    step = Step(root="./data", attrs=["test"], step_in="raw")
    df = step.load(file_name="basic_data.csv", alias="data")

    cols = df.columns
    cols = pd.Series(cols)
    df.A = df.A + df.B

    assert isinstance(cols, pd.Series)
    step.col_step("data::A", "sum over all cols", cols)

    assert step.get_doc("A") == [[IMPORT + NO_DETAILS], [IMPORT + NO_DETAILS], "sum over all cols"]
