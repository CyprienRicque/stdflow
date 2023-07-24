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
from stdflow.metadata import MetaData, get_file, get_file_md
from stdflow.path import Path
from stdflow.utils import to_html

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
        self.data_l: list[
            MetaData
        ] = []  # all files in the pipeline to get to this step
        self.data_l_in: list[MetaData] = []  # direct input to this step file
        # ================ #

        # All step level attributes are optional. If set, they will replace empty values in load and save functions
        self._step_in: Optional[str] = None  # input step name. default when exporting
        self._version_in: Optional[
            str
        ] = None  # input step version. default when exporting
        self._path_in: Optional[
            Union[list[str] | str]
        ] = None  # input step path. default when exporting
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
        ] = None  # output step path. default when exporting
        self._file_name_out: Optional[
            str
        ] = None  # output step file. default when exporting
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

    def files_needed_to_gen(self, files_to_gen: list[MetaData]) -> list[MetaData]:
        """
        Risk of infinite loop if there is a cycle in the graph TODO
        :param files_to_gen:
        :return:
        """
        def get_input_files(files: list[MetaData]) -> list[str]:
            uuids = list(set([item['uuid'] for sublist in [e.input_files for e in files] for item in sublist]))
            if not len(uuids):
                return []
            return uuids + get_input_files([e for e in self.data_l if e.uuid in uuids])

        # recursive
        input_files = get_input_files(files_to_gen)
        return [e for e in self.data_l if e.uuid in input_files]

    @classmethod
    def from_dict(cls, d):
        """
        The detection of generated files only works because all files not generated by the export step are input files
        of other files. This in ensured by loading only files that are input of used files to this step.
        :param d:
        :return:
        """
        step = cls()
        step.data_l = [MetaData.from_dict(e) for e in d["files"]]
        # data_l_in are input_files of files that are never used as input_files
        input_files = set([item['uuid'] for sublist in [e.input_files for e in step.data_l] for item in sublist])
        generated_files = [e for e in step.data_l if e.uuid not in input_files]
        step.data_l_in = list(set([item['uuid'] for sublist in [e.input_files for e in generated_files] for item in sublist]))
        step.data_l_in = [e for e in step.data_l if e.uuid in step.data_l_in]
        return step

    @classmethod
    def from_file(cls, path):
        """
        tries to load json meta data file, if no file, returns None
        :param path:
        :return:
        """
        if not os.path.exists(path):
            logger.debug(f"no metadata file found in {path}")
            return None
        return cls.from_dict(json.load(open(path, "r")))

    @classmethod
    def from_path(cls, path: Path):
        return Step.from_file(path.metadata_path)

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
        ] = None  # input step path. default when exporting
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
        ] = None  # output step path. default when exporting
        self._file_name_out: Optional[
            str
        ] = None  # output step file. default when exporting
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

        path_obj = Path.from_input_params(
            data_root_path, path, step, version, file_name
        )

        if method == "auto":
            method = path_obj.extension
        if isinstance(method, str):
            if method not in loaders:
                raise ValueError(f"method {method} not in {list(loaders.keys())}")
            method = loaders[method]

        # Load data
        data = method(path_obj.full_path, *args, **kwargs)

        # Add metadata
        previous_step = Step.from_path(path_obj)
        if not previous_step:
            file = MetaData.from_data(path_obj, data)
            mds = [file]
        else:
            file = get_file_md(previous_step.data_l, path_obj)
            if not file:
                logger.warning(
                    f"metadata file found but file {path_obj.full_path} not present in it."
                    f"Quick fix: change the file location as it was not generated the same way as other files "
                    f"in this folder. current behavior: Using the file as coming from no previous files"
                )
                file = MetaData.from_data(path_obj, data)
                mds = [file]
            else:
                mds = previous_step.files_needed_to_gen([file]) + [file]

        # do not add the same file twice in self.data_l
        # 1. Keep the file one if same uuid
        # 2. Replace the file if same full_path
        for md in mds:
            if md in [f for f in self.data_l]:  # file already added: same uuid
                # logger.debug(f"File {md} already added. Skipping")
                pass
            elif md.path.full_path_from_root in [f.path.full_path_from_root for f in self.data_l]:
                logger.warning(f"Replacing {md} by {file} as they have the same path but not the same uuid")
                to_rm = [f for f in self.data_l if f.path.full_path_from_root == md.path.full_path_from_root][0]
                self.data_l.remove(to_rm)
                self.data_l.append(md)
                if md == file:
                    self.data_l_in.remove(to_rm)
                    self.data_l_in.append(md)
            else:
                self.data_l.append(md)
                if md == file:
                    self.data_l_in.append(md)

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
        html_export: bool = True,
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
        :param html_export: if True, export html view of the data and the pipeline it comes from
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

        self.data_l.append(
            MetaData.from_data(path_obj, data, method.__str__(), self.data_l_in)
        )
        self.to_file(path_obj)
        if html_export:
            to_html(path_obj.metadata_path, path_obj.dir_path)
