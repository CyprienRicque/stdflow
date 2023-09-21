from __future__ import annotations

import os

from stdflow.config import RUN_ENV_KEY
from stdflow.stdflow_path.data_path import DataPath

# isort: off
# -> Following is used by end user
# from stdflow.dataframe_ext import pandas_document
from stdflow.step import Step
from stdflow.step_runner import StepRunner
from stdflow.pipeline import Pipeline
from stdflow.stdflow_doc.documenter import IMPORT, CREATE, DROP, ORIGIN_NAME, ORIGIN_PATH, NO_DETAILS

# isort: on

try:
    from typing import Any, Literal, Optional, Tuple, Union, Iterable
except ImportError:
    from typing_extensions import Literal, Optional, Union, Tuple, Any

import pandas as pd

__version__ = "0.0.63"

import logging
import sys

from stdflow.stdflow_types.strftime_type import Strftime
from stdflow.step import GStep

logging.basicConfig()
logger = logging.getLogger(__name__)


class Module(object):
    def __init__(self, module):
        self.__module = module

    def __getattr__(self, name):
        return getattr(self.__module, name)

    @staticmethod
    def from_pipeline():
        return os.getenv(RUN_ENV_KEY, False)

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
        alias: str = ":ignore",
        file_glob: bool = False,
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
            alias=alias,
            file_glob=file_glob,
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
        alias: str = ":ignore",
        export_viz_tool: bool = False,
        verbose: bool = False,
        **kwargs,
    ) -> DataPath:
        return self.step.save(
            data,
            root=root,
            attrs=attrs,
            step=step,
            version=version,
            file_name=file_name,
            method=method,
            alias=alias,
            export_viz_tool=export_viz_tool,
            verbose=verbose,
            **kwargs,
        )

    def reset(self):
        return self.step.reset()

    def var(self, key, value, force=False):
        return self.step.var(key, value, force=force)

    def col_step(
        self,
        col: str,
        name: str,
        in_cols: pd.Index | pd.Series | list | str | None = None,
        alias: str = None,
    ):
        """
        syntactic sugar to document a column
        """
        return self.step.col_step(col, name, in_cols=in_cols, alias=alias)

    def create_col(self, col, comment: str = NO_DETAILS, alias: str = None):
        """
        syntactic sugar to document a column creation
        """
        return self.step.create_col(col, comment=comment, alias=alias)

    def import_col(self, col, comment: str = NO_DETAILS, alias: str = None):
        """
        syntactic sugar to document a column import
        """
        return self.step.import_col(col, comment=comment, alias=alias)

    def drop_col(self, col, comment: str = NO_DETAILS, alias: str = None):
        """
        syntactic sugar to document a column drop
        """
        return self.step.drop_col(col, comment=comment, alias=alias)

    def col_origin_name(
        self,
        col: str,
        origin_name: str,
        in_cols: str | Iterable | Literal[":auto"] = ":auto",
        alias: str | None = None,
    ):
        """
        :param col:
        :param origin_name:
        :param in_cols: default to the same as col
        :param alias:
        :return:
        """
        return self.step.col_origin_name(col, origin_name, in_cols=in_cols, alias=alias)

    def col_origin_path(
        self,
        col: str,
        origin_path: str,
        in_cols: str | Iterable | Literal[":auto"] = ":auto",
        alias: str | None = None,
    ):
        """
        :param col:
        :param origin_path:
        :param in_cols: default to the same as col
        :param alias:
        :return:
        """
        return self.step.col_origin_path(col, origin_path, in_cols=in_cols, alias=alias)

    def cols_step(
        self, cols: list, col_step: str, input_cols: pd.Index | pd.Series | list | str | None = None
    ):
        return self.step.cols_step(cols, col_step, in_cols=input_cols)

    def cols_steps(
        self, cols_steps: dict, input_cols: pd.Index | pd.Series | list | str | None = None
    ):
        return self.step.cols_steps(cols_steps, in_cols=input_cols)

    def get_doc(self, col: str, alias: str | None = None, starts_with: str | None = None):
        return self.step.get_doc(col, alias=alias, starts_with=starts_with)

    def get_origin_names_raw(self, col: str, alias: str):
        return self.step.get_origin_names_raw(col, alias)

    def get_origin_names(self, col: str, alias: str):
        return self.step.get_origin_names(col, alias)


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


def from_pipeline():
    ...


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
    alias: str = ":ignore",
    file_glob: bool = False,
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
    alias: str = ":ignore",
    export_viz_tool: bool = False,
    verbose: bool = False,
    **kwargs,
) -> DataPath:
    ...


def reset():
    ...


def var(key, value, force=False):
    ...


def col_step(

    col: str,
    name: str,
    in_cols: pd.Index | pd.Series | list | str | None = None,
    alias: str = None,
):
    """
    syntactic sugar to document a column
    """
    ...


def create_col(self, col, comment: str = NO_DETAILS, alias: str = None):
    """
    syntactic sugar to document a column creation
    """
    ...


def import_col(self, col, comment: str = NO_DETAILS, alias: str = None):
    """
    syntactic sugar to document a column import
    """
    ...


def drop_col(self, col, comment: str = NO_DETAILS, alias: str = None):
    """
    syntactic sugar to document a column drop
    """
    ...


def col_origin_name(

    col: str,
    origin_name: str,
    in_cols: str | Iterable | Literal[":auto"] = ":auto",
    alias: str | None = None,
):
    """
    :param col:
    :param origin_name:
    :param in_cols: default to the same as col
    :param alias:
    """
    ...


def col_origin_path(
    col: str,
    origin_path: str,
    in_cols: str | Iterable | Literal[":auto"] = ":auto",
    alias: str | None = None,
):
    """
    :param col:
    :param origin_path:
    :param in_cols: default to the same as col
    :param alias:
    """
    ...


def cols_step(
    cols: list, col_step: str, input_cols: pd.Index | pd.Series | list | str | None = None
):
    ...


def cols_steps(
    cols_steps: dict, input_cols: pd.Index | pd.Series | list | str | None = None
):
    ...


def get_doc(col: str, alias: str | None = None, starts_with: str | None = None):
    ...


def get_origin_names_raw(col: str, alias: str):
    ...


def get_origin_names(col: str, alias: str):
    ...