from __future__ import annotations

import os
from datetime import datetime
from typing import Optional, Union

import pandas as pd

__version__ = "0.0.1"

from stdflow.loaders import DataLoader
from stdflow.step import GStep, Step

step: Step = GStep()


@property
def step_name():
    return step.name


@step_name.setter
def step_name(value):
    step.name = value


def load(
    path: str,
    method: Union[str | object] = "auto",
    attrs: list = None,
    file: str = None,
    step: Union[bool, str] = True,
    version: str = None,
    *args,
    **kwargs,
) -> pd.DataFrame:
    """

    :param path:
    :param paths:
    :param step: True if in_step to be auto-detected if any, No if no in_step. str to specify the name of the step
    :param data_loader:
    :param last_version:
    :return:
    """

    return step.load(path, method, attrs, file, step, version, *args, **kwargs)
