from __future__ import annotations

import logging
import os
from typing import Literal, Optional

from stdflow.config import STEP_PREFIX, VERSION_PREFIX
from stdflow.types.strftime_type import Strftime
from stdflow.utils import detect_folders, fstep, fv, remove_dir, retrieve_from_path

logger = logging.getLogger(__name__)

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

logger.addHandler(ch)
logger.setLevel(logging.DEBUG)


class Path:
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
        if isinstance(step_name, str) and step_name.startswith(STEP_PREFIX):
            step_name = step_name[len(STEP_PREFIX) :]
        # if version is str and contains v_, remove it
        if isinstance(version, str) and version.startswith(VERSION_PREFIX):
            version = version[len(VERSION_PREFIX) :]

        self.root = root
        self.path: str = "/".join(attrs) if isinstance(attrs, list) else attrs
        self.step_name = step_name
        self.file_name = file_name

        self.version = None
        if version in [":last", ":first"]:
            if not os.path.isdir(self.dir_path):
                logger.error(f"Path {self.dir_path} does not exist")
            self.version = self.detect_version(self.dir_path, version)
        elif version is not None:
            self.version = version

    @property
    def file_name_no_ext(self):
        return os.path.splitext(self.file_name)[0]

    # @classmethod
    # def from_full_path(cls, full_path):
    #     """
    #     :param full_path: full path to the file (included)
    #     :return: Path object
    #     """
    #     path = full_path
    #
    #     file_name = os.path.basename(full_path)
    #     version = retrieve_from_path(full_path, VERSION_PREFIX)
    #     step = retrieve_from_path(full_path, STEP_PREFIX)
    #
    #     path = os.path.dirname(path)
    #     path = remove_dir(path, file_name) if file_name else path
    #     path = remove_dir(path, fv(version)) if version else path
    #     path = remove_dir(path, fstep(step)) if step else path
    #
    #     return cls(
    #         root=path, step_name=step, version=version, file_name=file_name
    #     )

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
        return Path.full_path_(
            self.root,
            self.path,
            fstep(self.step_name) if self.step_name else "",
            fv(self.version) if self.version else "",
            self.file_name,
        )

    @property
    def full_path_from_root(self):
        return Path.full_path_(
            None,
            self.path,
            fstep(self.step_name) if self.step_name else "",
            fv(self.version) if self.version else "",
            self.file_name,
        )

    @property
    def dir_path(self):
        return Path.full_path_(
            self.root,
            self.path,
            fstep(self.step_name) if self.step_name else "",
            fv(self.version) if self.version else "",
            None,
        )

    @property
    def step_dir(self):
        return Path.full_path_(
            self.root,
            self.path,
            fstep(self.step_name) if self.step_name else None,
            None,
            None,
        )

    @staticmethod
    def full_path_(root, path, step, version, file_name):
        return os.path.join(root or "", path or "", step or "", version or "", file_name or "")

    @property
    def extension(self):
        return os.path.splitext(self.file_name)[-1][1:]

    @property
    def dict_step(self):
        return dict(
            path=self.path,
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

    def __str__(self):
        return self.full_path

    def __repr__(self):
        return self.full_path


if __name__ == "__main__":
    path = Path("./data", attrs="fr", step_name="raw", version=":last")
    assert path.full_path == "./data/fr/step_raw/v_2/", f"src.full_path: {path.full_path}"
