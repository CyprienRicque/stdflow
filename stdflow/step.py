from __future__ import annotations

import glob
import json
import logging
import os
import uuid
import warnings
from datetime import datetime
from typing import Literal, Optional, Union
from types import ModuleType

import pandas as pd

from stdflow.config import DEFAULT_DATE_VERSION_FORMAT, INFER
from stdflow.metadata import MetaData, get_file, get_file_md
from stdflow.path import Path
from stdflow.types.strftime_type import Strftime
from stdflow.utils import get_arg_value, to_html

logger = logging.getLogger(__name__)

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

logger.addHandler(ch)
logger.setLevel(logging.DEBUG)

loaders = dict(
    csv=pd.read_csv,
    excel=pd.read_excel,
    parquet=pd.read_parquet,
    json=pd.read_json,
    pickle=pd.read_pickle,
    feather=pd.read_feather,
    hdf=pd.read_hdf,
    sql=pd.read_sql,
)

savers = dict(
    csv=pd.DataFrame.to_csv,
    excel=pd.DataFrame.to_excel,
    parquet=pd.DataFrame.to_parquet,
    json=pd.DataFrame.to_json,
    pickle=pd.DataFrame.to_pickle,
    feather=pd.DataFrame.to_feather,
    hdf=pd.DataFrame.to_hdf,
    sql=pd.DataFrame.to_sql,
)


class GStep:
    """For when Step is used at package level"""

    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = Step()
        return cls._instance


class Step(ModuleType):
    def __init__(self, name: str = "Step"):
        super().__init__(name)
        # === Exported === #
        self.data_l: list[MetaData] = []
        self.data_l_in: list[MetaData] = []  # direct input to this step file
        # ================ #

        # Default values of load and save functions
        self._step_in: str | None = None
        self._version_in: str | None = ":last"
        self._attrs_in: str | list[str] | None = None
        self._file_name_in: str | None = ":auto"  # TODO
        self._method_in: str | object | None = ":auto"  # TODO
        self._root_in: str | None = ":default"

        self._step_out: str | None = None
        self._version_out: str | None = DEFAULT_DATE_VERSION_FORMAT
        self._attrs_out: str | list[str] | None = None
        self._file_name_out: str | None = None
        self._method_out: str | object | None = ":auto"
        self._root_out: str | None = ":default"

        self._root: str | None = "./data"

    # TODO fix this ugly function
    def load(
            self,
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
        """
        :param verbose:
        :param root: path to the root data folder. not exported in metadata.
        :param file_name: input file name
        :param method: method to load the data to use. e.g. pd.read_csv, pd.read_excel, pd.read_parquet, ... or "csv" to use default csv...
            Method must use path as the first argument
        :param attrs: path from data folder to file. (optionally include step and version)
        :param step: input step name
        :param version: input version name. one of [":last", ":first", "<version_name>", None]
        :param kwargs: kwargs to send to the method
        :return:
        """
        # if arguments are None, use step level arguments
        root = get_arg_value(get_arg_value(root, self._root_in), self._root)
        attrs = get_arg_value(attrs, self._attrs_in)
        file_name = get_arg_value(file_name, self._file_name_in)
        step = get_arg_value(step, self._step_in)
        version = get_arg_value(version, self._version_in)
        method = get_arg_value(method, self._method_in)

        path = Path.from_input_params(root, attrs, step, version, file_name)

        if method == ":auto":
            method = path.extension
        if isinstance(method, str):
            if method not in loaders:
                raise ValueError(f"method {method} not in {list(loaders.keys())}")
            method = loaders[method]

        # Load data
        data = method(path.full_path, **kwargs)

        # Add metadata
        previous_step = Step._from_path(path)
        if not previous_step:
            file_loaded = MetaData.from_data(path, data)
            new_files = [file_loaded]
        else:
            file_loaded = get_file_md(previous_step.data_l, path)
            if not file_loaded:
                warnings.warn(
                    f"metadata file found but file {path.full_path} not present in it."
                    f"Quick fix: change the file location as it was not generated the same way as other files "
                    f"in this folder. current behavior: Using the file as coming from no previous files",
                    category=ResourceWarning,
                )
                file_loaded = MetaData.from_data(path, data)
                new_files = [file_loaded]
            else:
                new_files = previous_step._files_needed_to_gen([file_loaded]) + [file_loaded]

        # do not add the same file twice in self.data_l
        # 1. Keep the file one if same uuid
        # 2. Replace the file if same full_path
        for new_file in new_files:
            if new_file in [f for f in self.data_l]:  # file already added: same uuid
                # logger.debug(f"File {md} already added. Skipping")
                pass
            elif new_file.path.full_path_from_root in [
                f.path.full_path_from_root for f in self.data_l
            ]:
                # warnings.warn(
                #     f"Replacing {new_file} by {file_loaded} as they have the same path but not the same uuid",
                #     category=ResourceWarning,
                # )
                to_rm = [
                    f
                    for f in self.data_l
                    if f.path.full_path_from_root == new_file.path.full_path_from_root
                ]
                assert len(to_rm) == 1
                to_rm = to_rm[0]
                self.data_l.remove(to_rm)
                self.data_l.append(new_file)
            else:
                self.data_l.append(new_file)

        # file loaded

        if file_loaded in [f for f in self.data_l_in]:  # file already added: same uuid
            # logger.debug(f"File {md} already added. Skipping")
            pass
        elif file_loaded.path.full_path_from_root in [
            f.path.full_path_from_root for f in self.data_l_in
        ]:
            to_rm = [
                f
                for f in self.data_l_in
                if f.path.full_path_from_root == file_loaded.path.full_path_from_root
            ]
            assert len(to_rm) == 1
            to_rm = to_rm[0]
            self.data_l_in.remove(to_rm)
            self.data_l_in.append(file_loaded)
        else:
            self.data_l_in.append(file_loaded)

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
            descriptions: dict[str | str] | None = None,
            html_export: bool = ":default",
            verbose: bool = False,
            **kwargs,
    ):
        """
        :param data: data to save
        :param root: first part of the path to the root data folder that is not to export in metadata
        :param method: method to load the data to use. e.g. pd.read_csv, pd.read_excel, pd.read_parquet, ... or "csv" to use default csv...
            Method must use path as the first argument
        :param attrs: path from data folder to file. (optionally include step, version and file name)
        :param step: step
        :param version: last part of the full_path. one of [strftime str, "<version_name>", None]
        :param file_name: file name
        :param descriptions: columns description of the dataset to save
        :param html_export: if True, export html view of the data and the pipeline it comes from
        :param verbose:
        :param kwargs: kwargs to send to the method
        :return:
        """
        # if arguments are None, use step level arguments
        root = get_arg_value(get_arg_value(root, self._root_out), self._root)
        attrs = get_arg_value(attrs, self._attrs_out)
        step = get_arg_value(step, self._step_out)
        version = get_arg_value(version, self._version_out)
        file = get_arg_value(file_name, self._file_name_out)
        method = get_arg_value(method, self._method_out)

        if Strftime.__call__(version):
            version = datetime.now().strftime(version)

        path = Path.from_input_params(root, attrs, step, version, file)

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
        method(data, path.full_path, **kwargs)

        self.data_l.append(MetaData.from_data(path, data, method.__str__(), self.data_l_in, descriptions))
        self._to_file(path)
        if html_export:
            to_html(path.metadata_path, path.dir_path)

    def reset(self):  # TODO
        # === Exported === #
        self.data_l: list[MetaData] = []
        self.data_l_in: list[MetaData] = []  # direct input to this step file
        # ================ #

        # Default values of load and save functions
        self._step_in: str | None = None
        self._version_in: str | None = ":last"
        self._attrs_in: str | list[str] | None = None
        self._file_name_in: str | None = ":auto"  # TODO
        self._method_in: str | object | None = ":auto"  # TODO
        self._root_in: str | None = ":default"

        self._step_out: str | None = None
        self._version_out: str | None = f":strftime {DEFAULT_DATE_VERSION_FORMAT}"  # TODO  date_string = date_string.split(" ")[1]
        self._attrs_out: str | list[str] | None = None
        self._file_name_out: str | None = None
        self._method_out: str | object | None = ":auto"
        self._root_out: str | None = ":default"

        self._root: str | None = "./data"

    # === Private === #

    def __dict__(self):
        return dict(
            files=[d.__dict__() for d in self.data_l],
        )

    def _files_needed_to_gen(self, files_to_gen: list[MetaData]) -> list[MetaData]:
        """
        Risk of infinite loop if there is a cycle in the graph TODO
        :param files_to_gen:
        :return:
        """

        def get_input_files(files: list[MetaData]) -> list[str]:
            uuids = list(
                {item["uuid"] for sublist in [e.input_files for e in files] for item in sublist}
            )

            if not len(uuids):
                return []
            return uuids + get_input_files([e for e in self.data_l if e.uuid in uuids])

        # recursive
        input_files = get_input_files(files_to_gen)
        return [e for e in self.data_l if e.uuid in input_files]

    @classmethod
    def _from_dict(cls, d):
        """
        The detection of generated files only works because all files not generated by the export step are input files
        of other files. This in ensured by loading only files that are input of used files to this step.
        :param d:
        :return:
        """
        step = cls()
        step.data_l = [MetaData.from_dict(e) for e in d["files"]]
        # data_l_in are input_files of files that are never used as input_files
        input_files = {
            item["uuid"] for sublist in [e.input_files for e in step.data_l] for item in sublist
        }
        generated_files = [e for e in step.data_l if e.uuid not in input_files]
        step.data_l_in = list(
            {
                item["uuid"]
                for sublist in [e.input_files for e in generated_files]
                for item in sublist
            }
        )
        step.data_l_in = [e for e in step.data_l if e.uuid in step.data_l_in]
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
    def _from_path(cls, path: Path):
        return Step._from_file(path.metadata_path)

    def _to_file(self, path: Path):
        """Save step to file"""
        file_path = os.path.join(path.dir_path, MetaData.file_name)
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
    def attrs_in(self, path: list | str) -> None:
        self._attrs_in = path

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
    def attrs_out(self, path: list | str) -> None:
        self._attrs_out = path

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
