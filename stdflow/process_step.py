from __future__ import annotations

from stdflow.stdflow_path.process_path import ProcessPath
from stdflow.stdflow_utils.execution import run_notebook, run_function

try:
    from typing import Literal, Protocol, Union
except ImportError:
    from typing_extensions import Literal, Protocol, Union

from stdflow.stdflow_path import DataPath


class ProcessStep:
    def __init__(self):
        self.path: ProcessPath = ProcessPath()
        self.variables: dict[str, Union[str, int, float, bool]] = {}
        self.type: Literal["notebook", "python_file",  "function"] | None = None
        self.function_name: str | None = None

    @property
    def env_variables(self):
        # for each variable add "__stdflow__" prefix
        return {f"__stdflow__{k}": v for k, v in self.variables.items()}

    def __call__(self, *args, **kwargs):
        if self.type == "notebook":
            run_notebook(self.path.full_path, self.env_variables)
        elif self.type == "python_file":
            run_python_file(self.path.full_path, self.env_variables)
        elif self.type == "function":
            run_function(self.path.full_path, self.function_name, self.env_variables)

    def to_file(self, path: str):
        pass
