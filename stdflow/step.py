from __future__ import annotations

import glob
import json
import logging
import os
import uuid
import warnings
from datetime import datetime

from stdflow.stdflow_utils.execution import run_notebook, run_python_file, run_function

from stdflow.stdflow_utils.caller_metadata import (
    get_caller_metadata,
    get_calling_package__,
    get_notebook_path,
    get_notebook_name,
)

try:
    from typing import Any, Literal, Optional, Tuple, Union
except ImportError:
    from typing_extensions import Literal, Union, Any, Tuple

from types import ModuleType

import pandas as pd

from stdflow.config import DEFAULT_DATE_VERSION_FORMAT, INFER
from stdflow.metadata import MetaData, get_file, get_file_md
from stdflow.stdflow_path import DataPath
from stdflow.stdflow_types.strftime_type import Strftime
from stdflow.stdflow_utils import get_arg_value, export_viz_html

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

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
)


class GStep:
    """Singleton Step used at package level"""

    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = Step()
        return cls._instance


class Step(ModuleType):
    """
    environment variables set by stdflow:
    stdflow__run: if set, the step is executed from a pipeline run
    stdflow__run__file_path: name of the file executed
    stdflow__run__function_name: name of the function executed
    stdflow__vars: variables used to run the function
    """
    def __init__(
        self,
        step_in: str | None = None,
        version_in: str | None = ":last",
        attrs_in: str | list[str] | None = ":default",
        file_name_in: str | None = ":default",
        method_in: str | object | None = ":auto",
        root_in: str | None = ":default",

        step_out: str | None = None,
        version_out: str | None = DEFAULT_DATE_VERSION_FORMAT,
        attrs_out: str | list[str] | None = ":default",
        file_name_out: str | None = ":default",
        method_out: str | object | None = ":auto",
        root_out: str | None = ":default",

        root: str | None = "./data",
        attrs: str | list[str] | None = None,
        file_name: str | None = ":auto",

        # to create use the step as part of a pipeline
        exec_file_path: str | None = None,
        exec_function_name: str | None = None,
        exec_variables: dict[str, Any] | None = None,

        data_l: list[MetaData] = None,
        data_l_in: list[MetaData] = None,
    ):
        super().__init__("stdflow_step")

        # === Exported === #
        # all inputs to this step
        self.data_l = data_l if data_l is not None else []
        # direct input to this step
        self.data_l_in = data_l_in if data_l_in is not None else []
        # ================ #

        # Default values of load and save functions
        self._step_in = step_in
        self._version_in = version_in
        self._attrs_in = attrs_in
        self._file_name_in = file_name_in
        self._method_in = method_in
        self._root_in = root_in

        self._step_out = step_out
        self._version_out = version_out
        self._attrs_out = attrs_out
        self._file_name_out = file_name_out
        self._method_out = method_out
        self._root_out = root_out

        self._root = root
        self._file_name = file_name
        self._attrs = attrs

        # Used to execute the step from a pipeline
        self._exec_file_path = exec_file_path
        self._exec_function_name = exec_function_name
        self._exec_env_vars = exec_variables or {}

        # Used when actually using the step to save the variables set
        self._var_set = {}

    def run(self):
        """
        Run the function of the pipeline
        :return:
        """
        if not self._exec_file_path:
            raise ValueError("exec_file_path is None. Cannot run step.")
        if os.environ.get("stdflow__run", None) is not None:
            logger.debug("Step executed from a pipeline run")
            if os.environ.get("stdflow__run__file_path", None) == self._exec_file_path:
                warnings.warn(
                    f"Step executed from the current file. Ignoring the step run."
                    f"It is ok to do that to test the current step."
                    f"Step.run() is to be used to run steps from other files.",
                    category=UserWarning,
                )
                return "run ignored"
        extension = os.path.splitext(self._exec_file_path)[1]

        env_run = {}

        for env_var, value in self._exec_env_vars.items():
            env_run[f"stdflow__vars__{env_var}"] = str(value)

        env_run["stdflow__run"] = "True"
        env_run["stdflow__run__file_path"] = self._exec_file_path
        if self._exec_function_name:
            env_run["stdflow__run__function_name"] = self._exec_function_name

        if extension == ".ipynb" and not self._exec_function_name:
            run_notebook(path=self._exec_file_path, env_vars=env_run)
        elif extension == ".ipynb" and self._exec_function_name:
            raise NotImplementedError("run python function in notebooks not implemented yet")
        elif extension == ".py" and not self._exec_function_name:
            run_python_file(path=self._exec_file_path, env_vars=env_run)
        elif extension == ".py" and self._exec_function_name:
            run_function(self._exec_file_path, self._exec_function_name, env_vars=env_run)
        else:
            raise ValueError(f"extension {extension} not supported")

    def var(self, key, value, force=False):
        is_run = os.environ.get("stdflow__run", None) is not None
        env_var = os.environ.get(f"stdflow__vars__{key}", None)
        if is_run and env_var is not None and not force:
            logger.debug(f"using {key} from environment variable")
            return env_var
        self._var_set[key] = value
        return value

    def load(
        self,
        *,
        root: str | Literal[":default"] = ":default",
        attrs: list | str | None | Literal[":default"] = ":default",
        step: str | None | Literal[":default"] = ":default",
        version: str | None | Literal[":default", ":last", ":first"] = ":default",
        file_name: str | Literal[":default", ":auto"] = ":default",
        method: str | object | Literal[":default", ":auto"] = ":default",
        descriptions: bool = False,
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
        :param file_glob: use glob to infer file name
        :param version: input version name. one of [":last", ":first", "<version_name>", None]
        :param descriptions: whether to return the description of the file. `df, desc = sf.load(..., descriptions=True)`
        :param kwargs: kwargs to send to the method
        :return:
        """
        if verbose:
            logger.setLevel(logging.INFO)
        else:
            logger.setLevel(logging.WARNING)
        caller_file_name, caller_function, caller_package = get_caller_metadata()
        if "ipykernel" in caller_file_name:
            notebook_path, notebook_name = get_notebook_path()
            logger.debug(f"Called from jupyter notebook {notebook_name} in {notebook_path}")
        elif caller_function == '<module>':
            logger.debug(f"Called from python file {caller_file_name}")
        else:
            logger.debug(f"Called from function {caller_function} in {caller_file_name}")

        logger.debug(f"caller_metadata: {caller_file_name, caller_function, caller_package}")
        # other = get_calling_package__()
        # logger.info(f"get_calling_package: {other}")

        # if arguments are None, use step level arguments
        root = get_arg_value(get_arg_value(root, self._root_in), self._root)
        attrs = get_arg_value(get_arg_value(attrs, self._attrs_in), self._attrs)
        file_name = get_arg_value(get_arg_value(file_name, self._file_name_in), self._file_name)
        step = get_arg_value(step, self._step_in)
        version = get_arg_value(version, self._version_in)
        method = get_arg_value(method, self._method_in)

        path = DataPath.from_input_params(root, attrs, step, version, file_name, glob=file_glob)
        logger.debug(f"Loading data from {path.full_path}")
        if not path.file_name:
            raise ValueError(f"file_name is None. path: {path}")

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
        if previous_step:  # Metadata file in the folder of the file to load exists
            file_md: MetaData = get_file_md(previous_step.data_l, path)
            if file_md:
                new_files = previous_step._files_needed_to_gen([file_md]) + [file_md]
            else:  # The file is not in the metadata file
                warnings.warn(
                    f"metadata file found but file {path.full_path} not present in it."
                    f"Quick fix: change the file location as it was not generated the same way as other files"
                    f"in this folder. current behavior: Using the file as having no previous step "
                    f"and ignoring the metadata file.",
                    category=UserWarning,
                )
                previous_step = None

        if previous_step is None:
            file_md: MetaData = MetaData.from_data(path, data)
            new_files = [file_md]

        # do not add the same file twice in self.data_l
        # 1. Keep the file one if same uuid
        # 2. Add if same path but different uuid: same file twice but with different timestamps (error from the dev)
        for new_file in new_files:
            if new_file not in [f for f in self.data_l]:  # file already added: same uuid
                self.data_l.append(new_file)

        # file loaded
        if file_md not in [f for f in self.data_l_in]:  # file already added: same uuid
            self.data_l_in.append(file_md)

        if descriptions:
            return data, file_md.descriptions
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
        export_viz_tool: bool = ":default",
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
        :param export_viz_tool: if True, export html view of the data and the pipeline it comes from
        :param verbose:
        :param kwargs: kwargs to send to the method
        :return:
        """
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
            if len(self.data_l_in) == 1:
                file = self.data_l_in[0].path.file_name
            elif len(self.data_l_in) > 1:
                raise ValueError(
                    f":auto takes the file name of the data source used to create the file."
                    f"Multiple data sources detected: {self.data_l_in}"
                    f"Use file_name argument to specify the file name."
                )
            else:
                raise ValueError(
                    f":auto takes the file name of the data source used to create the file."
                    f"No data source detected. Use file_name argument to specify the file name."
                )
        path = DataPath.from_input_params(root, attrs, step, version, file)
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
        method(data, path.full_path, **kwargs)

        self.data_l.append(
            MetaData.from_data(path, data, method.__str__(), self.data_l_in, descriptions)
        )
        # export metadata file
        self._to_file(path)
        if export_viz_tool:
            export_viz_html(path.metadata_path, path.dir_path)

    def reset(self):  # TODO
        # === Exported === #
        self.data_l: list[MetaData] = []
        self.data_l_in: list[MetaData] = []  # direct input to this step file
        # ================ #

        # Default values of load and save functions
        self._step_in: str | None = None
        self._version_in: str | None = ":last"
        self._attrs_in: str | list[str] | None = ":default"
        self._file_name_in: str | None = ":default"  # TODO
        self._method_in: str | object | None = ":auto"  # TODO
        self._root_in: str | None = ":default"

        self._step_out: str | None = None
        self._version_out: str | None = DEFAULT_DATE_VERSION_FORMAT
        self._attrs_out: str | list[str] | None = ":default"
        self._file_name_out: str | None = ":default"  # TODO
        self._method_out: str | object | None = ":auto"
        self._root_out: str | None = ":default"

        self._root: str | None = "./data"
        self._file_name: str | None = ":auto"  # TODO
        self._attrs: str | list[str] | None = None

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
    def _from_path(cls, path: DataPath):
        return Step._from_file(path.metadata_path)

    def _to_file(self, path: DataPath):
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

