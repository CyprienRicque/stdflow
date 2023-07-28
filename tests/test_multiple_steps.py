import os
import re

import stdflow as sf


def load_and_process_digi_sentiments_indonesia(catalog, brand_dict=None):
    print("load_and_process_digi_sentiments_indonesia")
    conf = catalog["digimind_sentiments"]

    step = sf.Step()
    step.reset()
    print("attrs in", step.attrs_in)
    step.root = "./data"
    step.attrs = conf["attrs"]
    step.step_in = conf["step_in"]
    step.step_out = conf["step_out"]
    print("attrs in", step.attrs_in)


def load_digi_mentions_indonesia(step):
    print("load_digi_mentions_indonesia")
    path = os.path.join(step.root, step.attrs, f"step_{step.step_in}")
    print("path", path)
    digi_files = ["coucou*.xls"]
    dfs = []
    pattern = r"ID_Daily\sMentions_CPD\s([\w\s]+)"
    for i in digi_files:
        try:
            new_digi = step.load(file_name=i, attrs=step.attrs, method="excel", version=None, header=1)
        except:
            print(f"Dir contained unrecognized file {i}, ignored")
            continue



def load_and_process_digi_mentions_indonesia(catalog, brand_dict=None):
    print("load_and_process_digi_mentions_indonesia")
    conf = catalog["digimind_mentions"]

    step = sf.Step()
    step.reset()
    step.root = "./data"
    step.attrs = conf["attrs"]
    step.step_in = conf["step_in"]
    step.step_out = conf["step_out"]

    print(conf["attrs"], step.attrs)

    df = load_digi_mentions_indonesia(step)

    return df


def load_and_process_digi_indonesia(catalog, brand_dict=None):
    print("load_and_process_digi_indonesia")
    conf = catalog["digimind"]

    step = sf.Step()
    step.reset()
    step.root = "./data"
    step.attrs_out = conf["attrs_out"]
    step.step_out = conf["step_out"]

    mentions = load_and_process_digi_mentions_indonesia(catalog, brand_dict=brand_dict)
    sentiments = load_and_process_digi_sentiments_indonesia(catalog, brand_dict=brand_dict)


def test_ms():
    load_and_process_digi_indonesia(
        {
            "digimind": {"attrs_out": "oui", "step_out": "non"},
            "digimind_mentions": {"attrs": "oui", "step_out": "non", "step_in": "non"},
            "digimind_sentiments": {"attrs": "oui", "step_out": "non", "step_in": "non"},
        }
    )
