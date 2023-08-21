from __future__ import annotations

import glob
import json
import logging
import os
import uuid
import warnings
from datetime import datetime

from stdflow.environ_manager import FlowEnv
from stdflow.stdflow_utils.execution import run_function, run_notebook, run_python_file

try:
    from typing import Any, Literal, Optional, Tuple, Union
except ImportError:
    from typing_extensions import Any, Literal, Tuple, Union


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
        workspace: str | None = None,
        function: str | None = None,
        variables: dict[str, Any] | None = None,
    ):
        if function is not None:
            raise NotImplementedError("step runner for function not implemented yet")
        self.env = FlowEnv()

        self.workspace = workspace or os.path.dirname(file_path)
        if len(self.workspace) == 0:
            self.workspace = "./"
        # file path is relative to the workspace
        self.path = os.path.relpath(file_path, self.workspace)
        self.worker_path = os.path.join(self.workspace, self.path)

        # self.worker_path_adjusted = self.env.get_adjusted_worker_path(self.worker_path)

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

            if self.env.path == self.path:
                warnings.warn(
                    f"Infinite pipeline loop detected. Not re running the step {self.worker_path}",
                    category=UserWarning,
                )
                return "run ignored: infinite loop detected"

        extension = os.path.splitext(self.worker_path)[1]

        self.env.start_run(self.workspace, self.path, self.env_vars)
        try:
            if extension == ".ipynb" and not self.exec_function_name:
                run_notebook(path=self.path, env_vars=self.env_vars, **kwargs)
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
        if not self.worker_path:
            logger.warning("file_path is None. Cannot run step.")
            return False
        if not os.path.exists(self.worker_path):
            # print("adj", self.worker_path_adjusted)
            # print("ori", self.worker_path)
            # print("cwd", os.getcwd())
            logger.warning(
                f"file_path {self.worker_path} does not exist. Cannot run step.\n"
                f"Current working directory: {os.getcwd()}"
            )
            return False
        return True
