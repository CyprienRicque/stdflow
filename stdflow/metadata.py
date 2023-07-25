from __future__ import annotations

import json
import logging
import os
import uuid

import pandas as pd

from stdflow.path import Path

logger = logging.getLogger(__name__)

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

logger.addHandler(ch)
logger.setLevel(logging.DEBUG)


class MetaData:
    file_name = "metadata.json"

    def __init__(
        self,
        path: Path,
        columns: list[dict],
        export_method_used: str,
        input_files: list[dict],
        uuid_: str = None,
    ):
        self.uuid = uuid_ or str(uuid.uuid4())
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
        if not d:
            raise ValueError("d is empty")
        path = Path.from_dict(d["step"], d["file_name"], d["file_type"])

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
        path: Path,
        data: pd.DataFrame,
        export_method_used: str = "unknown",
        input_files: list["MetaData"] = None,
    ):
        if input_files is not None:
            input_files = list({"uuid": file.uuid} for file in input_files)
        columns = list(
            {"name": c, "type": t.name, "description": None}
            for c, t in zip(data.columns, data.dtypes)
        )
        return cls(path, columns, export_method_used, input_files or [], uuid_=None)

    def __eq__(self, other):
        if isinstance(other, Path):
            return self.path == other
        if isinstance(other, MetaData):
            return self.uuid == other.uuid
        raise ValueError(f"other must be of type Path or str, got {type(other)}")

    def __str__(self):
        return f"MetaData(\n\t{self.uuid[:6]=}\n\t{self.path=}\n\t{self.input_files=}\n)"

    def __repr__(self):
        return self.__str__()


def get_file(files: list[dict], path: Path):
    return next(
        (
            f
            for f in files
            if Path.from_dict(f["step"], f["file_name"], f["file_type"]).full_path_from_root
            == path.full_path_from_root
        ),
        None,
    )


def get_file_md(files: list[MetaData], path: Path):
    return next(
        (f for f in files if f.path.full_path_from_root == path.full_path_from_root),
        None,
    )
