from __future__ import annotations

import json
import logging
import os
import uuid
from typing import Any

import pandas as pd

from stdflow.stdflow_path import DataPath
from stdflow.stdflow_utils import get_creation_time, string_to_uuid

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class MetaData:
    file_name = "metadata.json"

    def __init__(
        self,
        path: DataPath,
        columns: list[dict],
        export_method_used: str,
        input_files: list[dict],
        uuid_: str = None,
    ):
        # self.uuid = uuid_ or str(uuid.uuid4())
        self.file_creation_time = str(get_creation_time(path.full_path_from_root))
        self.uuid = uuid_ or str(
            string_to_uuid(f"{path.full_path_from_root}{self.file_creation_time}")
        )
        self.path: DataPath = path
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
        if not d:
            raise ValueError("d is empty")
        path = DataPath.from_dict(d["step"], d["file_name"], d["file_type"])

        return cls(
            path=path,
            columns=d["columns"],
            export_method_used=d["export_method_used"],
            input_files=d["input_files"],
            uuid_=d["uuid"],
        )

    @classmethod
    def from_data(
        cls,
        path: DataPath,
        data: pd.DataFrame | dict | Any,
        export_method_used: str = "unknown",
        input_files: list["MetaData"] = None,
        descriptions: dict[str, str] = None,
    ):
        if input_files is not None:
            input_files = list({"uuid": file.uuid} for file in input_files)

        # check if data is a dict with each value being a dataframe
        if isinstance(data, dict) and all(
            isinstance(v, pd.DataFrame) for v in data.values()
        ):  # multiple sheets from excel file
            columns = list(
                {
                    "name": c,
                    "type": t.name,
                    "description": (
                        descriptions.get(c, None) if isinstance(descriptions, dict) else None
                    ),
                }
                for data in data.values()
                for c, t in zip(data.columns, data.dtypes)
            )
        elif type(data) == dict:
            columns = list(
                {
                    "name": k,
                    "type": type(v).__name__,
                    "description": (descriptions.get(k, None) if isinstance(descriptions, dict) else None),
                }
                for k, v in data.items()
            )
        elif type(data) == pd.DataFrame:
            columns = list(
                {
                    "name": c,
                    "type": t.name,
                    "description": (
                        descriptions.get(c, None) if isinstance(descriptions, dict) else None
                    ),
                }
                for c, t in zip(data.columns, data.dtypes)
            )
        else:
            logger.warning(f"unknown data type: {type(data)}")
            columns = []
        return cls(path, columns, export_method_used, input_files or [], uuid_=None)

    def __eq__(self, other):
        if isinstance(other, DataPath):
            return self.path == other
        if isinstance(other, MetaData):
            return self.uuid == other.uuid
        raise ValueError(f"other must be of type Path or str, got {type(other)}")

    @property
    def descriptions(self):
        return {c["name"]: c["description"] for c in self.columns}

    def __str__(self):
        return f"MetaData(\n\tuuid[:6]={self.uuid[:6]}\n\tpath={self.path}\n\tinput_files={self.input_files}\n)"

    def __repr__(self):
        return self.__str__()


def get_file(files: list[dict], path: DataPath):
    return next(
        (
            f
            for f in files
            if DataPath.from_dict(f["step"], f["file_name"], f["file_type"]).full_path_from_root
               == path.full_path_from_root
        ),
        None,
    )


def get_file_md(files: list[MetaData], path: DataPath):
    return next(
        (f for f in files if f.path.full_path_from_root == path.full_path_from_root),
        None,
    )
