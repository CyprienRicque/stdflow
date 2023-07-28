from __future__ import annotations

import logging
import os
from abc import ABC, abstractmethod

try:
    from typing import Literal, Optional, OrderedDict
except ImportError:
    from typing_extensions import Literal, Optional, OrderedDict

from stdflow.config import STEP_PREFIX, VERSION_PREFIX
from stdflow.stdflow_types.strftime_type import Strftime
from stdflow.stdflow_utils import detect_folders, fstep, fv, remove_dir, retrieve_from_path

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class Path(ABC):
    def __init__(
        self,
        root: str | None = "./data",
        file_name: str = None,
    ):
        """
        At this stage all information are present except the version which is to be detected if not specified
        :param root: first part of the full_path
        :param file_name: file name (optional)
        """
        self.root = root
        self.file_name = file_name

    @property
    def file_name_no_ext(self):
        return os.path.splitext(self.file_name)[0]

    @property
    @abstractmethod
    def full_path(self) -> str:
        ...

    @property
    @abstractmethod
    def full_path_from_root(self) -> str:
        ...

    @property
    @abstractmethod
    def dir_path(self) -> str:
        ...

    @staticmethod
    def _create_path(*args):
        return os.path.join(*[arg or "" for arg in args])

    @property
    def extension(self):
        return os.path.splitext(self.file_name)[-1][1:]

    def __str__(self):
        return self.full_path

    def __repr__(self):
        return self.full_path

