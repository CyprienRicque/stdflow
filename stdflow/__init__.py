from __future__ import annotations

import os
from datetime import datetime

try:
    from typing import Any, Literal, Optional, Tuple, Union
except ImportError:
    from typing_extensions import Literal, Optional, Union, Tuple, Any

import pandas as pd

__version__ = "0.0.20"

import logging
import sys

from stdflow.stdflow_loaders import DataLoader
from stdflow.step import GStep, Step
from stdflow.stdflow_types.strftime_type import Strftime

logging.basicConfig()
logger = logging.getLogger(__name__)


class Module(object):
    def __init__(self, module):
        self.__module = module

    def __getattr__(self, name):
        return getattr(self.__module, name)

    @property
    def step(self):
        return GStep()

    @property
    def attr(self):
        return self.__attr

    @attr.setter
    def attr(self, value):
        self.__attr = value

    @property
    def step_in(self) -> str:
        return self.step.step_in

    @step_in.setter
    def step_in(self, step_name: str) -> None:
        self.step.step_in = step_name

    @property
    def version_in(self) -> str:
        return self.step.version_in

    @version_in.setter
    def version_in(self, version_name: str) -> None:
        self.step.version_in = version_name

    @property
    def attrs_in(self) -> list | str:
        return self.step.attrs_in

    @attrs_in.setter
    def attrs_in(self, path: list | str) -> None:
        self.step.attrs_in = path

    @property
    def file_name_in(self) -> str:
        return self.step.file_name_in

    @file_name_in.setter
    def file_name_in(self, file_name: str) -> None:
        self.step.file_name_in = file_name

    @property
    def method_in(self) -> str | object:
        return self.step.method_in

    @method_in.setter
    def method_in(self, method: str | object) -> None:
        self.step.method_in = method

    @property
    def root_in(self) -> str:
        return self.step.root_in

    @root_in.setter
    def root_in(self, root: str) -> None:
        self.step.root_in = root

    @property
    def step_out(self) -> str:
        return self.step.step_out

    @step_out.setter
    def step_out(self, step_name: str) -> None:
        self.step.step_out = step_name

    @property
    def version_out(self) -> str:
        return self.step.version_out

    @version_out.setter
    def version_out(self, version_name: str) -> None:
        self.step.version_out = version_name

    @property
    def attrs_out(self) -> list | str:
        return self.step.attrs_out

    @attrs_out.setter
    def attrs_out(self, path: list | str) -> None:
        self.step.attrs_out = path

    @property
    def file_name_out(self) -> str:
        return self.step.file_name_out

    @file_name_out.setter
    def file_name_out(self, file_name: str) -> None:
        self.step.file_name_out = file_name

    @property
    def method_out(self) -> str | object:
        return self.step.method_out

    @method_out.setter
    def method_out(self, method: str | object) -> None:
        self.step.method_out = method

    @property
    def root_out(self) -> str:
        return self.step.root_out

    @root_out.setter
    def root_out(self, root: str) -> None:
        self.step.root_out = root

    @property
    def root(self) -> str:
        return self.step.root

    @root.setter
    def root(self, root: str) -> None:
        self.step.root = root

    @property
    def file_name(self) -> str:
        return self.step.file_name

    @property
    def attrs(self) -> list | str:
        return self.step.attrs

    @attrs.setter
    def attrs(self, attrs: list | str) -> None:
        self.step.attrs = attrs

    @file_name.setter
    def file_name(self, file_name: str) -> None:
        self.step.file_name = file_name

    def load(
        self,
        *,
        root: str | Literal[":default"] = ":default",
        attrs: list | str | None | Literal[":default"] = ":default",
        step: str | None | Literal[":default"] = ":default",
        version: str | None | Literal[":default", ":last", ":first"] = ":default",
        file_name: str | Literal[":default", ":auto"] = ":default",
        method: str | object | Literal[":default", ":auto"] = ":default",
        descriptions: bool = False,
        verbose: bool = False,
        **kwargs,
    ) -> Tuple[Any, dict] | Any:
        return self.step.load(
            root=root,
            attrs=attrs,
            step=step,
            version=version,
            file_name=file_name,
            method=method,
            verbose=verbose,
            **kwargs,
        )

    def save(
        self,
        data: pd.DataFrame,
        *,
        root: str | Literal[":default"] = ":default",
        attrs: list | str | None | Literal[":default"] = ":default",
        step: str | None | Literal[":default"] = ":default",
        version: str | None | Literal[":default"] | Strftime = ":default",
        file_name: str | Literal[":default", ":auto"] = ":default",
        method: str | object | Literal[":default", ":auto"] = ":default",
        html_export: bool = ":default",
        verbose: bool = False,
        **kwargs,
    ):
        return self.step.save(
            data,
            root=root,
            attrs=attrs,
            step=step,
            version=version,
            file_name=file_name,
            method=method,
            html_export=html_export,
            verbose=verbose,
            **kwargs,
        )

    def reset(self):
        return self.step.reset()


if __name__ == "__main__":  # test if run as a script
    import doctest

    sys.exit(doctest.testmod().failed)
else:  # normal import, use `Module` class to provide `attr` property
    logger.debug(f"loading {__name__} as a module")
    sys.modules[__name__] = Module(sys.modules[__name__])


# self.step: Step = Step()  # Singleton Step


#######################################################################
# Just a copy of the above class directly in the file for completion
#######################################################################


@property
def step():
    ...


@property
def attr():
    ...


@attr.setter
def attr(value):
    ...


@property
def step_in() -> str:
    ...


@step_in.setter
def step_in(step_name: str) -> None:
    ...


@property
def version_in() -> str:
    ...


@version_in.setter
def version_in(version_name: str) -> None:
    ...


@property
def attrs_in() -> list | str:
    ...


@attrs_in.setter
def attrs_in(path: list | str) -> None:
    ...


@property
def file_name_in() -> str:
    ...


@file_name_in.setter
def file_name_in(file_name: str) -> None:
    ...


@property
def method_in() -> str | object:
    ...


@method_in.setter
def method_in(method: str | object) -> None:
    ...


@property
def root_in() -> str:
    ...


@root_in.setter
def root_in(root: str) -> None:
    ...


@property
def step_out() -> str:
    ...


@step_out.setter
def step_out(step_name: str) -> None:
    ...


@property
def version_out() -> str:
    ...


@version_out.setter
def version_out(version_name: str) -> None:
    ...


@property
def attrs_out() -> list | str:
    ...


@attrs_out.setter
def attrs_out(path: list | str) -> None:
    ...


@property
def file_name_out() -> str:
    ...


@file_name_out.setter
def file_name_out(file_name: str) -> None:
    ...


@property
def method_out() -> str | object:
    ...


@method_out.setter
def method_out(method: str | object) -> None:
    ...


@property
def root_out() -> str:
    ...


@root_out.setter
def root_out(root: str) -> None:
    ...


@property
def root() -> str:
    ...


@root.setter
def root(root: str) -> None:
    ...


@property
def file_name() -> str:
    ...


@file_name.setter
def file_name(file_name: str) -> None:
    ...


def load(
    *,
    root: str | Literal[":default"] = ":default",
    attrs: list | str | None | Literal[":default"] = ":default",
    step: str | None | Literal[":default"] = ":default",
    version: str | None | Literal[":default", ":last", ":first"] = ":default",
    file_name: str | Literal[":default", ":auto"] = ":default",
    method: str | object | Literal[":default", ":auto"] = ":default",
    descriptions: bool = False,
    verbose: bool = False,
    **kwargs,
) -> Tuple[Any, dict] | Any:
    ...


def save(
    data: pd.DataFrame,
    *,
    root: str | Literal[":default"] = ":default",
    attrs: list | str | None | Literal[":default"] = ":default",
    step: str | None | Literal[":default"] = ":default",
    version: str | None | Literal[":default"] | Strftime = ":default",
    file_name: str | Literal[":default", ":auto"] = ":default",
    method: str | object | Literal[":default", ":auto"] = ":default",
    html_export: bool = ":default",
    verbose: bool = False,
    **kwargs,
):
    ...


def reset():
    ...
