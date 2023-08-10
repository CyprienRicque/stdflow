from __future__ import annotations

import glob
import json
import logging
import os
import uuid
import warnings
from datetime import datetime

from environ_manager import FlowEnv
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
from stdflow.stdflow_utils import get_arg_value, export_viz_html, string_to_uuid

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)


class StepRunner:
    """
    environment variables set by stdflow:
    stdflow__run: if set, the step is executed from a pipeline run
    stdflow__run__files_path: names of the files executed split by :
    stdflow__run__ids: ids of the files executed split by :
    stdflow__run__function_name: name of the function executed
    stdflow__vars: variables used to run the function
    """

    def __init__(
        self,
        file_path: str,
        *,
        root: str | None = None,
        function: str | None = None,
        variables: dict[str, Any] | None = None,
    ):
        if function is not None:
            raise NotImplementedError("step runner for function not implemented yet")
        self.env = FlowEnv()

        self.worker_path = os.path.join(root, file_path) if root else file_path
        self.worker_path_adjusted = self.env.get_adjusted_worker_path(self.worker_path)

        self.exec_function_name = function

        self.env_vars: dict = variables or {}

    def run(self, **kwargs) -> Any:
        """
        Run the function of the pipeline
        :return:
        """
        if not self.is_valid():
            raise RuntimeError("invalid step.")

        if self.env.running():
            logger.debug("Step executed from a pipeline run")

            if self.env.path == self.worker_path_adjusted:
                warnings.warn(
                    f"Infinite pipeline loop detected. Not re running the step {self.worker_path_adjusted}",
                    category=UserWarning,
                )
                return "run ignored: infinite loop detected"

        extension = os.path.splitext(self.worker_path_adjusted)[1]

        self.env.start_run(self.worker_path_adjusted, self.env_vars)
        try:
            if extension == ".ipynb" and not self.exec_function_name:
                run_notebook(path=self.worker_path_adjusted, env_vars=self.env_vars, **kwargs)
            elif extension == ".ipynb" and self.exec_function_name:
                raise NotImplementedError("run python function in notebooks not implemented yet")
            elif extension == ".py" and not self.exec_function_name:
                # run_python_file(path=self.worker_path, env_vars=env_run, **kwargs)
                raise NotImplementedError("run python file not implemented yet")
            elif extension == ".py" and self.exec_function_name:
                # run_function(self.worker_path, self._exec_function_name, env_vars=env_run, **kwargs)
                raise NotImplementedError("run python function not implemented yet")
            else:
                raise ValueError(f"extension {extension} not supported")
        except Exception as e:
            raise e
        finally:
            self.env.end_run()

    def is_valid(self) -> bool:
        """
        Check if the step is valid
        :return:
        """
        if not self.worker_path_adjusted:
            logger.warning("file_path is None. Cannot run step.")
            return False
        if not os.path.exists(self.worker_path_adjusted):
            # print("adj", self.worker_path_adjusted)
            # print("ori", self.worker_path)
            # print("cwd", os.getcwd())
            logger.warning(
                f"file_path {self.worker_path_adjusted} does not exist. Cannot run step.\n"
                f"Current working directory: {os.getcwd()}"
            )
            return False
        return True
