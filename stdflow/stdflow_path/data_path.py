from __future__ import annotations

import logging
import os

from stdflow.stdflow_path import Path

try:
    from typing import Literal, Optional
except ImportError:
    from typing_extensions import Literal, Optional

from stdflow.config import STEP_PREFIX, VERSION_PREFIX
from stdflow.stdflow_types.strftime_type import Strftime
from stdflow.stdflow_utils import detect_folders, fstep, fv, remove_dir, retrieve_from_path

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class DataPath(Path):
    def __init__(
        self,
        root: str | None = "./data",
        attrs: list | None | str = None,
        step_name: str | None = None,
        version: str | Literal[":last", ":first"] = ":last",
        file_name: str = None,
    ):
        """
        At this stage all information are present except the version which is to be detected if not specified
        :param root: first part of the full_path
        :param attrs: seconds parts of the full_path (optional)
        :param step_name: third part of the full_path (optional)
        :param version: last part of the full_path. one of [":last", ":first", "<version_name>", None]
        :param file_name: file name (optional)
        """
        # if step is str and contains step_, remove it
        super().__init__(root, file_name)
        if isinstance(step_name, str) and step_name.startswith(STEP_PREFIX):
            step_name = step_name[len(STEP_PREFIX) :]
        # if version is str and contains v_, remove it
        if isinstance(version, str) and version.startswith(VERSION_PREFIX):
            version = version[len(VERSION_PREFIX) :]

        self.attrs: str = "/".join(attrs) if isinstance(attrs, list) else attrs
        self.step_name = step_name

        self.version = None
        if version in [":last", ":first"]:
            if not os.path.isdir(self.dir_path):
                logger.error(f"Path {self.dir_path} does not exist")
            self.version = self.detect_version(self.dir_path, version)
        elif version is not None:
            self.version = version

    def detect_version(self, path, version_type):
        if version_type not in [":last", ":first"]:
            logger.warning(f"Unknown version type: {version_type}")
        # Check for versioned directories
        versions = detect_folders(path, VERSION_PREFIX)

        logger.debug(f"ordered versions: {versions}")
        if not versions:
            logger.warning(f"No versioned directories found in {path}")

        if version_type == ":last":
            return versions[-1] if versions else None
        elif version_type == ":first":
            return versions[0] if versions else None

        return None

    @property
    def full_path(self):
        return Path._create_path(
            self.root,
            self.attrs,
            fstep(self.step_name) if self.step_name else "",
            fv(self.version) if self.version else "",
            self.file_name,
        )

    @property
    def full_path_from_root(self):
        return Path._create_path(
            None,
            self.attrs,
            fstep(self.step_name) if self.step_name else "",
            fv(self.version) if self.version else "",
            self.file_name,
        )

    @property
    def dir_path(self):
        return Path._create_path(
            self.root,
            self.attrs,
            fstep(self.step_name) if self.step_name else "",
            fv(self.version) if self.version else "",
            None,
        )

    @property
    def step_dir(self):
        return Path._create_path(
            self.root,
            self.attrs,
            fstep(self.step_name) if self.step_name else None,
        )

    @property
    def dict_step(self):
        return dict(
            path=self.attrs,
            step_name=self.step_name,
            version=self.version,
        )

    @classmethod
    def from_dict(cls, step_dict, file_name, file_type):
        return cls(
            root=None,
            attrs=step_dict["path"],
            step_name=step_dict["step_name"],
            version=step_dict["version"],
            file_name=f"{file_name}.{file_type}",
        )

    @property
    def metadata_path(self):
        return os.path.join(self.dir_path, "metadata.json")

    @classmethod
    def from_input_params(cls, root, attrs, step, version, file_name):
        # if step is True:
        #     # extract step from path
        #     step = retrieve_from_path(path, STEP_PREFIX)
        #     path = remove_dir(path, fstep(step))
        # if version is True:
        #     # extract version from path
        #     version = retrieve_from_path(path, VERSION_PREFIX)
        #     path = remove_dir(path, fv(version))
        # if file_name is True:
        #     # extract file_name from path
        #     file_name = os.path.basename(path)
        #     path = os.path.dirname(path)

        return cls(
            root=root,
            attrs=attrs,
            step_name=step,
            version=version,
            file_name=file_name,
        )


if __name__ == "__main__":
    path = DataPath("./data", attrs="fr", step_name="raw", version=":last")
    assert path.full_path == "./data/fr/step_raw/v_2/", f"src.full_path: {path.full_path}"
