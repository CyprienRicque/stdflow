import pytest

from stdflow.stdflow_utils import path_to_str, str_to_path
from stdflow.stdflow_path import Path, DataPath


def test_basic_case():
    s = path_to_str("test", "raw", "1", "test.csv")
    assert s == "attrs=test::step_name=raw::version=1::file_name=test.csv"
    d = str_to_path(s)
    assert d == dict(attrs="test", step_name="raw", version="1", file_name="test.csv")


def test_path():
    path = DataPath("./data", attrs="fr", step_name="raw", version=":last")
    assert path.full_path == "./data/fr/step_raw/v_2/", f"src.full_path: {path.full_path}"

    assert str(path) == "attrs=fr::step_name=raw::version=2::file_name=None"
    assert str_to_path(str(path)) == dict(
        attrs="fr", step_name="raw", version="2", file_name=None
    )
    assert DataPath.from_str(str(path)) == path


def test_path_file_name_auto():
    path = DataPath("./data", attrs=["fr", "idontexist"], step_name="raw", version="coucou", file_name=':auto')
    assert path.full_path == "./data/fr/idontexist/step_raw/v_coucou/", f"src.full_path: {path.full_path}"

    assert str(path) == "attrs=fr/idontexist::step_name=raw::version=coucou::file_name=None"
    assert str_to_path(str(path)) == dict(
        attrs="fr/idontexist", step_name="raw", version="coucou", file_name=None
    )
    assert DataPath.from_str(str(path)) == path


def test_path_file_name_auto():
    path = DataPath("./data", attrs=["fr", "idont=exist"], step_name="raw", version="coucou", file_name=':auto')
    assert path.full_path == "./data/fr/idont=exist/step_raw/v_coucou/", f"src.full_path: {path.full_path}"

    assert str(path) == "attrs=fr/idont=exist::step_name=raw::version=coucou::file_name=None"
    assert str_to_path(str(path)) == dict(
        attrs="fr/idont=exist", step_name="raw", version="coucou", file_name=None
    )
    assert DataPath.from_str(str(path)) == path
