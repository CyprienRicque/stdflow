from __future__ import annotations

import os
from datetime import datetime
from typing import Literal, Optional, Union

import pandas as pd

__version__ = "0.0.5"

import sys

from stdflow.loaders import DataLoader
from stdflow.step import GStep, Step
from stdflow.types.strftime_type import Strftime

s_step = GStep()


def get_step_in() -> str:
    return s_step.step_in


def set_step_in(step_name: str) -> None:
    s_step.step_in = step_name


def load(
    *,
    root: str | Literal[":default"] = ":default",
    attrs: list | str | None | Literal[":default"] = ":default",
    step: str | None | Literal[":default"] = ":default",
    version: str | None | Literal[":default", ":last", ":first"] = ":default",
    file_name: str | Literal[":default", ":auto"] = ":default",
    method: str | object | Literal[":default", ":auto"] = ":default",
    verbose: bool = False,
    **kwargs,
) -> pd.DataFrame:
    return s_step.load(
        root=root,
        attrs=attrs,
        step=step,
        version=version,
        file_name=file_name,
        method=method,
        verbose=verbose,
        **kwargs,
    )


step_in = property(get_step_in, set_step_in)
