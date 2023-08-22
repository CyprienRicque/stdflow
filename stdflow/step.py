from __future__ import annotations

import copy
import glob
import json
import logging
import os
import uuid
import warnings
from datetime import datetime

# isort: off
from stdflow.environ_manager import FlowEnv
from stdflow.stdflow_doc.documenter import (
    Documenter,
    CREATE,
    IMPORT,
    DROP,
    ORIGIN_NAME,
    ORIGIN_PATH,
    NO_DETAILS,
)
from stdflow.stdflow_utils.caller_metadata import get_caller_metadata, get_notebook_path

# isort: on

try:
    from typing import Any, Literal, Optional, Tuple, Union, Iterable
except ImportError:
    from typing_extensions import Literal, Union, Any, Tuple

import pickle
from types import ModuleType

import pandas as pd

from stdflow.config import DEFAULT_DATE_VERSION_FORMAT, INFER
from stdflow.filemetadata import FileMetaData, get_file, get_file_md
from stdflow.stdflow_path import DataPath
from stdflow.stdflow_types.strftime_type import Strftime
from stdflow.stdflow_utils import export_viz_html, get_arg_value, string_to_uuid

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)


# TODO move this in utils
def save_to_pkl(obj, filename):
    with open(filename, "wb") as f:
        pickle.dump(obj, f)


def load_from_pkl(filename):
    with open(filename, "rb") as f:
        obj = pickle.load(f)
    return obj


loaders = dict(
    csv=pd.read_csv,
    excel=pd.read_excel,
    xlsx=pd.read_excel,
    xls=pd.read_excel,
    parquet=pd.read_parquet,
    json=pd.read_json,
    pickle=pd.read_pickle,
    feather=pd.read_feather,
    hdf=pd.read_hdf,
    sql=pd.read_sql,
    pkl=load_from_pkl,
)

savers = dict(
    csv=pd.DataFrame.to_csv,
    excel=pd.DataFrame.to_excel,
    xlsx=pd.DataFrame.to_excel,
    xls=pd.DataFrame.to_excel,
    parquet=pd.DataFrame.to_parquet,
    json=pd.DataFrame.to_json,
    pickle=pd.DataFrame.to_pickle,
    feather=pd.DataFrame.to_feather,
    hdf=pd.DataFrame.to_hdf,
    sql=pd.DataFrame.to_sql,
    pkl=save_to_pkl,
)


def alias_from_file_metadata(file_metadata: FileMetaData):
    return file_metadata.uuid


def filter_list(lst, starts_with):
    """Remove all strings in a list that do not start with"""
    new_lst = []
    for item in lst:
        if isinstance(item, list):  # If the item is a list, call the function recursively
            new_lst.append(filter_list(item, starts_with))
        elif isinstance(item, str) and item.startswith(starts_with):
            new_lst.append(item)
    return new_lst


def flatten(lst):
    """Flatten a nested list."""
    flat_list = []
    for item in lst:
        if isinstance(item, list):
            flat_list.extend(flatten(item))
        else:
            flat_list.append(item)
    return flat_list


def nested_replace(lst, old, new):
    """
    Replace occurrences of 'old' with 'new' in nested lists.
    """
    for i, item in enumerate(lst):
        if isinstance(item, list):
            lst[i] = nested_replace(item, old, new)
        elif isinstance(item, str):
            lst[i] = item.replace(old, new)
    return lst


class GStep:
    """Singleton Step used at package level"""

    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = Step()
        return cls._instance


class Step(ModuleType):
    def __init__(
        self,
        root: str | None = "./data",
        attrs: str | list[str] | None = None,
        file_name: str | None = ":auto",
        method_in: str | object | None = ":auto",
        root_in: str | None = ":default",
        attrs_in: str | list[str] | None = ":default",
        step_in: str | None = None,
        version_in: str | None = ":last",
        file_name_in: str | None = ":default",
        method_out: str | object | None = ":auto",
        root_out: str | None = ":default",
        attrs_out: str | list[str] | None = ":default",
        step_out: str | None = None,
        version_out: str | None = DEFAULT_DATE_VERSION_FORMAT,
        file_name_out: str | None = ":default",
        md_all_files: list[FileMetaData] = None,
        md_direct_input_files: list[FileMetaData] = None,
    ):
        super().__init__("stdflow_step")

        self.env = FlowEnv()

        # === Exported === #
        # all inputs to this step
        self.md_all_files: list[FileMetaData] = md_all_files if md_all_files is not None else []
        # direct input to this step
        self.md_direct_input_files: list[FileMetaData] = (
            md_direct_input_files if md_direct_input_files is not None else []
        )
        # ================ #

        # Default values of load and save functions
        self._method_in = method_in
        self._root_in = root_in
        self._attrs_in = attrs_in
        self._step_in = step_in
        self._version_in = version_in
        self._file_name_in = file_name_in

        self._method_out = method_out
        self._root_out = root_out
        self._attrs_out = attrs_out
        self._step_out = step_out
        self._version_out = version_out
        self._file_name_out = file_name_out

        self._root = root
        self._attrs = attrs
        self._file_name = file_name

        # Used when actually using the step to save the variables set
        self._var_set = {}

        self.doc = Documenter()

    def set_defaults(  # TODO some are not implemented
        self,
        *,
        root: str | None = "./data",
        attrs: str | list[str] | None = None,
        file_name: str | None = ":auto",
        method_in: str | object | None = ":auto",
        root_in: str | None = ":default",
        attrs_in: str | list[str] | None = ":default",
        step_in: str | None = None,
        version_in: str | None = ":last",
        file_name_in: str | None = ":default",
        method_out: str | object | None = ":auto",
        root_out: str | None = ":default",
        attrs_out: str | list[str] | None = ":default",
        step_out: str | None = None,
        version_out: str | None = DEFAULT_DATE_VERSION_FORMAT,
        file_name_out: str | None = ":default",
    ):
        self._root = root
        self._attrs = attrs
        self._file_name = file_name

        self._method_in = method_in
        self._root_in = root_in
        self._attrs_in = attrs_in
        self._step_in = step_in
        self._version_in = version_in
        self._file_name_in = file_name_in

        self._method_out = method_out
        self._root_out = root_out
        self._attrs_out = attrs_out
        self._step_out = step_out
        self._version_out = version_out
        self._file_name_out = file_name_out

    def var(self, key, value, force=False):
        env_var = self.env.var(key)

        if env_var is not None and not force:
            logger.debug(f"using {key} from environment variable")
            return env_var
        self._var_set[key] = value
        return value

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
        self.doc.document(col, name, in_cols=in_cols, alias=alias)

    def create_col(self, col, comment: str = NO_DETAILS, alias: str = None):
        """
        syntactic sugar to document a column creation
        """
        self.doc.document(col, name=CREATE + comment, alias=alias, in_cols=[])

    def import_col(self, col, comment: str = NO_DETAILS, alias: str = None):
        """
        syntactic sugar to document a column import
        """
        self.doc.document(col, name=IMPORT + comment, alias=alias, in_cols=[])

    def drop_col(self, col, comment: str = NO_DETAILS, alias: str = None):
        """
        syntactic sugar to document a column drop
        """
        self.doc.document(col, name=DROP + comment, alias=alias, in_cols=[])

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
        if in_cols == ":auto":
            in_cols = col
        self.doc.document(col, ORIGIN_NAME + origin_name, in_cols=in_cols, alias=alias)

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
        if in_cols == ":auto":
            in_cols = col
        self.doc.document(col, ORIGIN_PATH + origin_path, in_cols=in_cols, alias=alias)

    def cols_step(
        self, cols: list, col_step: str, input_cols: pd.Index | pd.Series | list | str | None = None
    ):
        for col in cols:
            self.col_step(col, col_step, in_cols=input_cols)

    def cols_steps(
        self, cols_steps: dict, input_cols: pd.Index | pd.Series | list | str | None = None
    ):
        for col, col_step in cols_steps.items():
            self.col_step(col, col_step, in_cols=input_cols)

    def get_doc(self, col: str, alias: str | None = None, starts_with: str | None = None):
        col_steps = self.doc.get_documentation(col, alias)
        if starts_with is None:
            return col_steps
        return filter_list(col_steps, starts_with)

    def get_origin_names_raw(self, col: str, alias: str):
        return self.get_doc(col, alias, ORIGIN_NAME)

    def get_origin_names(self, col: str, alias: str):
        return nested_replace(flatten(self.get_doc(col, alias, ORIGIN_NAME)), ORIGIN_NAME, "")

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
        """
        :param verbose:
        :param root: path to the root data folder. not exported in metadata.
        :param file_name: name of the file to load e.g. data.csv
        :param method: method to load the data to use. e.g. pd.read_csv, pd.read_excel, pd.read_parquet, ... or "csv" to use default csv...
            Method must use path as the first argument
        :param attrs: path from data folder to file. (optionally include step and version)
        :param step: input step name
        :param alias: alias of the dataset to document it and its columns
        :param file_glob: use glob to infer file name
        :param version: input version name. one of [":last", ":first", "<version_name>", None]
        :param kwargs: kwargs to send to the method
        :return:
        """
        original_logger_level = logger.level
        logger.setLevel(logging.INFO if verbose else logging.WARNING)

        # DEBUG prints
        caller_file_name, caller_function, caller_package = get_caller_metadata()
        if "ipykernel" in caller_file_name:
            notebook_path, notebook_name = get_notebook_path()
            logger.debug(f"Called from jupyter notebook {notebook_name} in {notebook_path}")
        elif caller_function == "<module>":
            logger.debug(f"Called from python file {caller_file_name}")
        else:
            logger.debug(f"Called from function {caller_function} in {caller_file_name}")

        logger.debug(f"caller_metadata: {caller_file_name, caller_function, caller_package}")
        # END DEBUG prints

        # if arguments are None, use step level arguments
        root = get_arg_value(get_arg_value(root, self._root_in), self._root)
        attrs = get_arg_value(get_arg_value(attrs, self._attrs_in), self._attrs)
        file_name = get_arg_value(get_arg_value(file_name, self._file_name_in), self._file_name)
        step = get_arg_value(step, self._step_in)
        version = get_arg_value(version, self._version_in)
        method = get_arg_value(method, self._method_in)

        # if self.env.running() and root is None:
        #     raise ValueError("root is None. Must be set when running from pipeline")
        # if root is not None:
        #     root = self.env.get_adjusted_worker_path(root)

        path: DataPath = DataPath.from_input_params(
            root, attrs, step, version, file_name, glob=file_glob
        )
        if not path.file_name:
            raise ValueError(f"file_name is None. path: {path}")

        if method == ":auto":
            method = path.extension
        if isinstance(method, str):
            if method not in loaders:
                raise ValueError(f"method {method} not in {list(loaders.keys())}")
            method = loaders[method]

        # Load data
        logger.info(f"Loading data from {path.full_path}")
        data = method(path.full_path, **kwargs)
        logger.info(f"Data loaded from {path.full_path}")

        # Add metadata
        previous_step: Step = Step._from_path(path)

        def fake_step():
            previous_step_ = Step()
            previous_step_.md_all_files = [FileMetaData.from_data(path, data)]
            if alias != ":ignore":
                for md in previous_step_.md_all_files:
                    previous_step_.doc.set_dataframe(
                        columns=[c["name"] for c in md.columns],
                        col_steps=md.col_steps,
                        alias="tmp",
                    )
            return previous_step_

        def update_current_step_with_previous_step(previous_step_):
            file_md_: FileMetaData = get_file_md(previous_step_.md_all_files, path)
            if file_md_:
                input_files_ = previous_step_._files_needed_to_gen([file_md_]) + [file_md_]
            else:  # The file is not in the metadata file
                warnings.warn(
                    f"metadata file found but file {path.full_path} not present in it."
                    f"Quick fix: change the file location as it was not generated the same way as other files"
                    f"in this folder. current behavior: Using the file as having no previous step "
                    f"and ignoring the metadata file.",
                    category=UserWarning,
                )
                return None, None
            return file_md_, input_files_

        file_md: FileMetaData = None
        input_files = None
        if previous_step is not None:
            file_md, input_files = update_current_step_with_previous_step(previous_step)
        if input_files is None:
            previous_step = fake_step()
            file_md, input_files = update_current_step_with_previous_step(previous_step)

        # do not add the same file twice in self.data_l
        # 1. Keep the file one if same uuid
        # 2. Add if same path but different uuid: same file twice but with different timestamps (error from the dev)
        for input_file in input_files:
            if input_file not in [f for f in self.md_all_files]:  # file already added: same uuid
                self.md_all_files.append(input_file)

        # file loaded
        if file_md not in [f for f in self.md_direct_input_files]:  # file already added: same uuid
            self.md_direct_input_files.append(file_md)

        # Update documentation
        if alias != ":ignore":
            alias = alias or alias_from_file_metadata(file_md)

            self.doc.set_dataframe(
                columns=[c["name"] for c in file_md.columns],
                col_steps=file_md.col_steps,
                alias=alias,
            )

        logger.setLevel(original_logger_level)
        return data

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
        """
        :param data: data to save
        :param root: first part of the path to the root data folder that is not to export in metadata
        :param method: method to load the data to use. e.g. pd.read_csv, pd.read_excel, pd.read_parquet, ... or "csv" to use default csv...
            Method must use path as the first argument
        :param attrs: path from data folder to file. (optionally include step, version and file name)
        :param step: step
        :param version: last part of the full_path. one of [strftime str, "<version_name>", None]
        :param file_name: file name
        :param alias: alias of the dataset to document it and its columns. ignore if alias == ":ignore"
        :param export_viz_tool: if True, export html view of the data and the pipeline it comes from
        :param verbose:
        :param kwargs: kwargs to send to the method
        :return:
        """
        original_logger_level = logger.level
        logger.setLevel(logging.INFO if verbose else logging.WARNING)

        # if arguments are None, use step level arguments
        root = get_arg_value(get_arg_value(root, self._root_out), self._root)
        attrs = get_arg_value(get_arg_value(attrs, self._attrs_out), self._attrs)
        step = get_arg_value(step, self._step_out)
        version = get_arg_value(version, self._version_out)
        file = get_arg_value(get_arg_value(file_name, self._file_name_out), self._file_name)
        method = get_arg_value(method, self._method_out)

        if Strftime.__call__(version):
            version = datetime.now().strftime(version)

        if file == ":auto":
            # Use the same file name as the one use to create it
            # Should be only file in self.data_l_in. take its file name
            if len(self.md_direct_input_files) == 1:
                file = self.md_direct_input_files[0].path.file_name
            elif len(self.md_direct_input_files) > 1:
                raise ValueError(
                    f":auto takes the file name of the data source used to create the file.\n"
                    f"Multiple data sources detected: {self.md_direct_input_files}\n"
                    f"Use file_name argument to specify the file name."
                )
            else:
                raise ValueError(
                    f":auto takes the file name of the data source used to create the file."
                    f"No data source detected. Use file_name argument to specify the file name."
                )

        path: DataPath = DataPath.from_input_params(root, attrs, step, version, file)
        if not path.file_name:
            raise ValueError(f"file_name is None. path: {path}")

        # if the directory does not exist, create it recursively
        if not os.path.exists(path.dir_path):
            os.makedirs(path.dir_path)

        if method == ":auto":
            method = path.extension
        if isinstance(method, str):
            if method not in savers:
                raise ValueError(f"method {method} not in {list(savers.keys())}")
            method = savers[method]

        # Save data
        logger.info(f"Saving data to {path.full_path}")
        method(data, path.full_path, **kwargs)
        logger.info(f"Data saved to {path.full_path}")

        saved_file_md = FileMetaData.from_data(
            path, data, method.__str__(), self.md_direct_input_files
        )

        if alias != ":ignore":
            self.columns_documentation(alias, data, path, saved_file_md)

        self.md_all_files.append(saved_file_md)

        # export metadata file
        logger.info(f"Saving metadata to {path.dir_path}")
        self._to_file(path)

        if export_viz_tool:
            logger.info(f"Exporting viz tool to {path.dir_path}")
            export_viz_html(path.metadata_path, path.dir_path)

        logger.setLevel(original_logger_level)

        return path

    def columns_documentation(
        self, alias: str | None, data: Any, path: DataPath, saved_file_md: FileMetaData
    ) -> None:
        # FIXME step col should be at file level

        if alias == ":ignore":
            return

        # automatic input file detection
        if alias is None:
            # md_direct_input_files with same file name and attrs
            input_file = [
                file
                for file in self.md_direct_input_files
                if file.path.attrs == path.attrs and file.path.file_name == path.file_name
            ]

            # find initial loaded file
            if len(self.md_direct_input_files) == 1:
                alias = alias_from_file_metadata(self.md_direct_input_files[0])
            elif len(input_file) == 1:
                alias = alias_from_file_metadata(input_file[0])
            elif len(self.md_direct_input_files) == 0:
                alias = alias_from_file_metadata(saved_file_md)
            else:
                logger.warning(
                    f"Could not auto save cols documentation:"
                    f":auto takes the file name of the data source used to create the file.\n"
                    f"Multiple data sources detected: {self.md_direct_input_files}\n"
                    f"Use alias argument to specify the datasource to use. or set alias=':ignore'"
                )

        if alias is not None:
            saved_file_md.col_steps = self.doc.metadata(data, alias)

    def reset(self):
        # === Exported === #
        self.md_all_files: list[FileMetaData] = []
        self.md_direct_input_files: list[FileMetaData] = []  # direct input to this step file
        # ================ #

        # Default values of load and save functions
        self.set_defaults()

        # reset documentation
        self.doc.reset()

    # === Private === #

    def __dict__(self):
        return dict(
            files=[d.__dict__() for d in self.md_all_files],
        )

    def _files_needed_to_gen(self, files_to_gen: list[FileMetaData]) -> list[FileMetaData]:
        """
        Risk of infinite loop if there is a cycle in the graph TODO
        :param files_to_gen:
        :return:
        """

        def get_input_files(files: list[FileMetaData]) -> list[str]:
            uuids = list(
                {item["uuid"] for sublist in [e.input_files for e in files] for item in sublist}
            )

            if not len(uuids):
                return []
            return uuids + get_input_files([e for e in self.md_all_files if e.uuid in uuids])

        # recursive
        input_files = get_input_files(files_to_gen)
        return [e for e in self.md_all_files if e.uuid in input_files]

    @classmethod
    def _from_dict(cls, d):  # TODO clean
        """
        The detection of generated files only works because all files not generated by the export step are input files
        of other files. This in ensured by loading only files that are input of used files to this step.
        :param d:
        :return:
        """
        step = cls()
        step.md_all_files = [FileMetaData.from_dict(e) for e in d["files"]]
        # data_l_in are input_files of files that are never used as input_files
        input_files = {
            item["uuid"]
            for sublist in [e.input_files for e in step.md_all_files]
            for item in sublist
        }
        generated_files = [e for e in step.md_all_files if e.uuid not in input_files]
        step.md_direct_input_files = list(
            {
                item["uuid"]
                for sublist in [e.input_files for e in generated_files]
                for item in sublist
            }
        )
        step.md_direct_input_files = [
            e for e in step.md_all_files if e.uuid in step.md_direct_input_files
        ]  # useless?
        return step

    @classmethod
    def _from_file(cls, path):
        """
        tries to load json meta data file, if no file, returns None
        :param path:
        :return:
        """
        if not os.path.exists(path):
            logger.debug(f"no metadata file found in {path}")
            return None
        return cls._from_dict(json.load(open(path, "r")))

    @classmethod
    def _from_path(cls, path: DataPath):
        return Step._from_file(path.metadata_path)

    def _to_file(self, path: DataPath):
        """Save step to file"""
        file_path = os.path.join(path.dir_path, FileMetaData.file_name)
        if not os.path.exists(path.dir_path):
            os.makedirs(path.dir_path)
        if os.path.exists(file_path):
            logger.debug(f"metadata file already exists in {file_path}. Replacing")
        with open(file_path, "w") as f:
            logger.debug(f"Saving metadata file to {file_path}")
            # logger.debug(f"metadata: {self.__dict__()}")
            json.dump(self.__dict__(), f)

    # === Properties === #

    @property
    def step_in(self) -> str:
        return self._step_in

    @step_in.setter
    def step_in(self, step_name: str) -> None:
        self._step_in = step_name

    @property
    def version_in(self) -> str:
        return self._version_in

    @version_in.setter
    def version_in(self, version_name: str) -> None:
        self._version_in = version_name

    @property
    def attrs_in(self) -> list | str:
        return self._attrs_in

    @attrs_in.setter
    def attrs_in(self, attrs: list | str) -> None:
        self._attrs_in = attrs

    @property
    def file_name_in(self) -> str:
        return self._file_name_in

    @file_name_in.setter
    def file_name_in(self, file_name: str) -> None:
        self._file_name_in = file_name

    @property
    def method_in(self) -> str | object:
        return self._method_in

    @method_in.setter
    def method_in(self, method: str | object) -> None:
        self._method_in = method

    @property
    def root_in(self) -> str:
        return self._root_in

    @root_in.setter
    def root_in(self, root: str) -> None:
        self._root_in = root

    @property
    def step_out(self) -> str:
        return self._step_out

    @step_out.setter
    def step_out(self, step_name: str) -> None:
        self._step_out = step_name

    @property
    def version_out(self) -> str:
        return self._version_out

    @version_out.setter
    def version_out(self, version_name: str) -> None:
        self._version_out = version_name

    @property
    def attrs_out(self) -> list | str:
        return self._attrs_out

    @attrs_out.setter
    def attrs_out(self, attrs: list | str) -> None:
        self._attrs_out = attrs

    @property
    def file_name_out(self) -> str:
        return self._file_name_out

    @file_name_out.setter
    def file_name_out(self, file_name: str) -> None:
        self._file_name_out = file_name

    @property
    def method_out(self) -> str | object:
        return self._method_out

    @method_out.setter
    def method_out(self, method: str | object) -> None:
        self._method_out = method

    @property
    def root_out(self) -> str:
        return self._root_out

    @root_out.setter
    def root_out(self, root: str) -> None:
        self._root_out = root

    @property
    def root(self) -> str:
        return self._root

    @root.setter
    def root(self, root: str) -> None:
        self._root = root

    @property
    def file_name(self) -> str:
        return self._file_name

    @file_name.setter
    def file_name(self, file_name: str) -> None:
        self._file_name = file_name

    @property
    def attrs(self) -> list | str:
        return self._attrs

    @attrs.setter
    def attrs(self, attrs: list | str) -> None:
        self._attrs = attrs
