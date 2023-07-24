from __future__ import annotations

import glob
import json
import logging
import os
import uuid
from datetime import datetime
from typing import Optional, Union

import pandas as pd

from stdflow.config import DATE_VERSION_FORMAT
from stdflow.path import Path

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
            cls._instance = super().__new__(cls)
        return cls._instance

    def get_step(self):
        return Step()


class Step:
    def __init__(self):
        # === Exported === #
        self.data_l: list[MetaData] = []  # file granularity
        # ================ #

        # All step level attributes are optional. If set, they will replace empty values in load and save functions
        self._step_in: Optional[str] = None  # input step name. default when exporting
        self._version_in: Optional[
            str
        ] = None  # input step version. default when exporting
        self._path_in: Optional[
            Union[list[str] | str]
        ] = None  # input step r_path. default when exporting
        self._file_in: Optional[str] = None  # input step file. default when exporting
        self._method_in: Optional[
            Union[str | object]
        ] = None  # input step method. default when exporting
        self._data_root_path_in: Optional[
            str
        ] = None  # input step path. default when exporting

        self._step_out: Optional[str] = None  # output step name. default when exporting
        self._version_out: Optional[
            str
        ] = None  # output step version. default when exporting
        self._path_out: Optional[
            Union[list[str] | str]
        ] = None  # output step r_path. default when exporting
        self._file_name_out: Optional[str] = None  # output step file. default when exporting
        self._method_out: Optional[
            Union[str | object]
        ] = None  # output step method. default when exporting
        self._data_root_path_out: Optional[
            str
        ] = None  # output step path. default when exporting

    def __dict__(self):
        return dict(
            files=[d.__dict__() for d in self.data_l],
        )

    @classmethod
    def from_dict(cls, d):
        step = cls()
        step.data_l = [MetaData.from_dict(e) for e in d["files"]]
        return step

    @classmethod
    def from_file(cls, path):
        """Load step from file"""
        return cls.from_dict(json.load(path))

    def to_file(self, path: Path):
        """Save step to file"""
        file_path = os.path.join(path.dir_path, MetaData.file_name)
        if not os.path.exists(path.dir_path):
            os.makedirs(path.dir_path)
        if os.path.exists(file_path):
            logger.debug(f"metadata file already exists in {file_path}. Replacing")
        with open(file_path, "w") as f:
            logger.debug(f"Saving metadata file to {file_path}")
            logger.debug(f"metadata: {self.__dict__()}")
            json.dump(self.__dict__(), f)

    def reset(self):
        # === Exported === #
        self.data_l: list[MetaData] = []  # file granularity
        # ================ #

        # All step level attributes are optional. If set, they will replace empty values in load and save functions
        self._step_in: Optional[str] = None  # input step name. default when exporting
        self._version_in: Optional[
            str
        ] = None  # input step version. default when exporting
        self._path_in: Optional[
            Union[list[str] | str]
        ] = None  # input step r_path. default when exporting
        self._file_in: Optional[str] = None  # input step file. default when exporting
        self._method_in: Optional[
            Union[str | object]
        ] = None  # input step method. default when exporting
        self._data_root_path_in: Optional[
            str
        ] = None  # input step path. default when exporting

        self._step_out: Optional[str] = None  # output step name. default when exporting
        self._version_out: Optional[
            str
        ] = None  # output step version. default when exporting
        self._path_out: Optional[
            Union[list[str] | str]
        ] = None  # output step r_path. default when exporting
        self._file_name_out: Optional[str] = None  # output step file. default when exporting
        self._method_out: Optional[
            Union[str | object]
        ] = None  # output step method. default when exporting
        self._data_root_path_out: Optional[
            str
        ] = None  # output step path. default when exporting

    def load(
        self,
        data_root_path: str,
        method: str | object = "auto",
        path: list | str = None,
        step: str = None,
        version: str | None = "last",
        file_name: str = True,
        *args,
        **kwargs,
    ) -> pd.DataFrame:
        """
        :param data_root_path: first part of the path to the root data folder that is not to export in metadata
        :param method: method to load the data to use. e.g. pd.read_csv, pd.read_excel, pd.read_parquet, ... or "csv" to use default csv...
            Method must use path as the first argument
        :param path: path from data folder to file. (optionally include step and version)
        :param step: if True: present in path
        :param version: if True: present in path. last part of the full_path. one of ["last", "first", "<version_name>", None]
        :param file_name: if True: present in path. file name
        :param args: args to send to the method
        :param kwargs: kwargs to send to the method
        :return:
        """
        # if arguments are None, use step level arguments
        data_root_path = data_root_path or self._data_root_path_in
        path = path or self._path_in
        file_name = file_name or self._file_in
        step = step or self._step_in
        version = version or self._version_in
        method = method or self._method_in

        path_obj = Path.from_input_params(data_root_path, path, step, version, file_name)

        if method == "auto":
            method = path_obj.extension
        if isinstance(method, str):
            if method not in loaders:
                raise ValueError(f"method {method} not in {list(loaders.keys())}")
            method = loaders[method]

        # Load data
        data = method(path_obj.full_path, *args, **kwargs)

        # Add metadata
        md = MetaData.from_file(path_obj)
        if not md:
            md = MetaData.from_data(path_obj, "unknown", data)
        self.data_l.append(md)

        return data

    def save(
        self,
        data: pd.DataFrame,
        data_root_path: str = None,
        method: str | object = "auto",
        path: list | str = None,
        step: str = None,
        version: str = None,
        file_name: str = None,
        *args,
        **kwargs,
    ):
        """
        :param data: data to save
        :param data_root_path: first part of the path to the root data folder that is not to export in metadata
        :param method: method to load the data to use. e.g. pd.read_csv, pd.read_excel, pd.read_parquet, ... or "csv" to use default csv...
            Method must use path as the first argument
        :param path: path from data folder to file. (optionally include step, version and file name)
        :param step: step
        :param version: last part of the full_path. one of ["new", "<version_name>", None]
        :param file_name: file name
        :param args: args to send to the method
        :param kwargs: kwargs to send to the method
        :return:
        """
        # if arguments are None, use step level arguments
        data_root_path = (
            data_root_path or self._data_root_path_out or self._data_root_path_in
        )
        path = path or self._path_out or self._path_in
        step = step or self._step_out
        version = version or self._version_out
        if version == "new":
            version = datetime.now().strftime(DATE_VERSION_FORMAT)
        file = file_name or self._file_name_out
        method = method or self._method_out

        path_obj = Path.from_input_params(data_root_path, path, step, version, file)

        # if the directory does not exist, create it recursively
        if not os.path.exists(path_obj.dir_path):
            os.makedirs(path_obj.dir_path)

        if method == "auto":
            method = path_obj.extension
        if isinstance(method, str):
            if method not in savers:
                raise ValueError(f"method {method} not in {list(savers.keys())}")
            method = savers[method]

        # Save data
        method(data, path_obj.full_path, *args, **kwargs)

        self.data_l.append(MetaData.from_data(path_obj, method.__str__(), data, self.data_l))
        self.to_file(path_obj)


class MetaData:
    file_name = "metadata.json"

    def __init__(self, path: Path, columns: list[dict], export_method_used: str, input_files: list[dict]):
        self.uuid = str(uuid.uuid4())
        self.path: Path = path
        self.columns: list[dict] = columns
        self.export_method_used: str = export_method_used
        self.input_files: list[dict] = input_files

    def __dict__(self):
        return dict(
            file_name=self.path.file_name_no_ext,
            file_type=self.path.extension,
            uuid=self.uuid,
            step=self.path.dict_step,
            columns=self.columns,
            export_method_used=self.export_method_used,
            input_files=self.input_files,
        )

    @classmethod
    def from_dict(cls, d):
        path = Path.from_dict(d["step"], d["file_name"], d["file_type"])

        return cls(
            path=path,
            columns=d["columns"],
            export_method_used=d["export_method_used"],
            input_files=d["input_files"],
        )

    @classmethod
    def from_data(
        cls, path: Path, export_method_used, data: pd.DataFrame, files: list[MetaData] = None
    ):
        print(path.step_dir)
        if files is not None:
            files = list(
                {"uuid": file.uuid}
                for file in files
                if file.path.step_dir != path.step_dir
            )
        columns = list(
            {"name": c, "type": t.name, "description": None}
            for c, t in zip(data.columns, data.dtypes)
        )
        return cls(path, columns, export_method_used, files or [])

    @classmethod
    def from_file(self, path: Path):
        """
        tries to load json meta data file, if no file, returns None
        :param path:
        :return:
        """
        file_path = path.dir_path
        if not os.path.exists(os.path.join(file_path, MetaData.file_name)):
            logger.debug(f"no metadata file found in {file_path}")
            return None
        with open(os.path.join(file_path, MetaData.file_name), "r") as f:
            return MetaData.from_dict(json.load(f))
