from __future__ import annotations

import os
from datetime import datetime
from typing import Optional, Union

import pandas as pd

__version__ = "0.0.2"

from stdflow.loaders import DataLoader
from stdflow.step import GStep, Step

s_step: Step = GStep()  # Singleton Step


@property
def step_in() -> str:
    return s_step.step_in


@step_in.setter
def step_in(step_name: str) -> None:
    s_step.step_in = step_name


@property
def version_in() -> str:
    return s_step.version_in


@version_in.setter
def version_in(version_name: str) -> None:
    s_step.version_in = version_name


@property
def path_in() -> list | str:
    return s_step.path_in


@path_in.setter
def path_in(path: list | str) -> None:
    s_step.path_in = path


@property
def file_in() -> str:
    return s_step.file_in


@file_in.setter
def file_in(file_name: str) -> None:
    s_step.file_in = file_name


@property
def method_in() -> str | object:
    return s_step.method_in


@method_in.setter
def method_in(method: str | object) -> None:
    s_step.method_in = method


@property
def root_in() -> str:
    return s_step.root_in


@root_in.setter
def root_in(root: str) -> None:
    s_step.root_in = root


@property
def step_out() -> str:
    return s_step.step_out


@step_out.setter
def step_out(step_name: str) -> None:
    s_step.step_out = step_name


@property
def version_out() -> str:
    return s_step.version_out


@version_out.setter
def version_out(version_name: str) -> None:
    s_step.version_out = version_name


@property
def path_out() -> list | str:
    return s_step.path_out


@path_out.setter
def path_out(path: list | str) -> None:
    s_step.path_out = path


@property
def file_name_out() -> str:
    return s_step.file_name_out


@file_name_out.setter
def file_name_out(file_name: str) -> None:
    s_step.file_name_out = file_name


@property
def method_out() -> str | object:
    return s_step.method_out


@method_out.setter
def method_out(method: str | object) -> None:
    s_step.method_out = method


@property
def root_out() -> str:
    return s_step.root_out


@root_out.setter
def root_out(root: str) -> None:
    s_step.root_out = root


@property
def root() -> str:
    return s_step.root


@root.setter
def root(root: str) -> None:
    s_step.root = root


def load(
    root: str,
    method: str | object = ":auto",
    path: list | str = None,
    step: str = None,
    version: str | None = ":last",
    file_name: str = True,
    *args,
    **kwargs,
) -> pd.DataFrame:
    return s_step.load(root, method, path, step, version, file_name, *args, **kwargs)


def save(
    data: pd.DataFrame,
    root: str = None,
    method: str | object = ":auto",
    path: list | str = None,
    step: str = None,
    version: str = None,
    file_name: str = None,
    html_export: bool = True,
    *args,
    **kwargs,
):
    return s_step.save(
        data,
        root,
        method,
        path,
        step,
        version,
        file_name,
        html_export,
        *args,
        **kwargs,
    )


def reset(self):
    return s_step.reset()
