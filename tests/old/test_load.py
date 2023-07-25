import pandas as pd

import stdflow as sf
from tests.old import setup

setup()


def test_load():
    step = (
        sf.Step()
    )  # only necessary when doing custom pipeline, otherwise functions are accessible at package level

    df = step.load(root="./data", attrs="fr", step="raw", version="1", file_name="random.csv")
    assert df.shape == (100, 4)

    df = step.load(root="./data", attrs="fr", step="raw", version=":last", file_name="random.csv")
    assert df.shape == (100, 4)

    df = step.load(
        root="./data", attrs="fr", step="raw", method=pd.read_csv, file_name="random.csv"
    )
    assert df.shape == (100, 4)


def test_load_no_version():
    step = (
        sf.Step()
    )  # only necessary when doing custom pipeline, otherwise functions are accessible at package level

    df = step.load(root="./data", attrs="es", step="raw", version=None, file_name="random.csv")
    assert df.shape == (100, 5)


def test_load_no_v_no_s():
    step = sf.Step()

    df = step.load(root="./data", attrs="es", file_name="random_base.csv")
    assert df.shape == (100, 2)


if __name__ == "__main__":
    test_load()
    test_load_no_version()
