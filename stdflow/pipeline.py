# AUTOGENERATED! DO NOT EDIT! File to edit: ../nbs/05_pipeline.ipynb.

# %% ../nbs/05_pipeline.ipynb 2
from __future__ import annotations

# %% auto 0
__all__ = ['logger', 'Pipeline']

# %% ../nbs/05_pipeline.ipynb 4
import logging
from typing import List

from colorama import Style
from tqdm.notebook import tqdm

from . import StepRunner
from .stdflow_utils.bt_print import print_header

try:
    from typing import Literal, Optional
except ImportError:
    from typing_extensions import Literal, Optional

from itertools import product


# %% ../nbs/05_pipeline.ipynb 5
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)


# %% ../nbs/05_pipeline.ipynb 6
class Pipeline:
    "Create pipeline of notebooks with optional variables"

    def __init__(self, steps: List[StepRunner] | StepRunner = None, *args):
        steps = [steps] if isinstance(steps, StepRunner) else steps or []
        steps += list(args) if args else []
        self.steps: List[StepRunner] = steps or []

    def verify(self) -> bool:
        "Verify that all steps are valid"
        is_valid = True
        for step in self.steps:
            is_valid = is_valid and step.is_valid()
        return is_valid

    def add_step(
        self,
        step: StepRunner | str = None,  # StepRunner or path to notebook
        **kwargs,  # kwargs to pass to StepRunner
    ):
        "Add step to pipeline"
        if isinstance(step, str):
            kwargs["file_path"] = step
            step = StepRunner(**kwargs)
        self.steps.append(step)
        return self

    def run(
        self,
        save_notebook: bool = False,  # Saves the output of cells in the notebook if True (default: False)
        progress_bar: bool = False,  # Whether to show progress bar
        kernel: Literal[":current", ":target", ":any_available"] | str = ":target",  # kernel name or :current to use current kernel, :target to use kernel specified in metadata of target notebook, :any_available to use any available kernel.
        kernels_on_fail: str | list = None,  # kernels to try if `kernel` does not exist / is not available (default: [":current", "python", ":any_available"])
        verbose=True,  # Whether to print output of cells
        **kwargs,  # kwargs to pass to StepRunner.run
    ):
        "Run pipeline"

        if kernels_on_fail is None:
            kernels_on_fail = [":current", "python", ":any_available"]
        # convert to list
        if isinstance(kernels_on_fail, str):
            kernels_on_fail = [kernels_on_fail]
            
        longest_worker_path_adjusted = max(
            [len(step.worker_path) for step in self.steps]
        )
        min_blank = 10

        it = enumerate(self.steps)
        if progress_bar:
            try:
                it = tqdm(enumerate(self.steps), desc="Pipeline")
            except ImportError as e:
                logger.warning(f"Could not use tqdm. {e.msg}")
                progress_bar = False

        for i, step in it:
            if progress_bar:
                it.desc = f"Pipeline: {step.worker_path}"

            text = step.worker_path
            print_header(text, i, longest_worker_path_adjusted, min_blank)
            print(f"Variables: {step.env_vars}")
            # Run step
            if not step.run(
                verbose=verbose,
                kernel=kernel,
                kernels_on_fail=kernels_on_fail,
                save_notebook=save_notebook,
                run_from_pipeline=True,
                **kwargs,
            ):
                logger.error(
                    f"Error while running step {i+1} of the pipeline. Pipeline stopped."
                )
                break

            print("", end="\n\n")

    def __call__(
        self,
        progress_bar: bool = False,  # Whether to show progress bar
        **kwargs,  # kwargs to pass to StepRunner.run
    ):
        "Run pipeline"
        self.run(progress_bar=progress_bar, **kwargs)

    def __str__(self):
        s = (
            Style.BRIGHT
            + """
================================
            PIPELINE            
================================

"""
            + Style.RESET_ALL
        )

        for i, step in enumerate(self.steps):
            s += f"""{Style.BRIGHT}STEP {i+1}{Style.RESET_ALL}
\tpath: {step.worker_path}
\tvars: {step.env_vars}

"""
        s += f"""{Style.BRIGHT}================================{Style.RESET_ALL}\n"""
        return s

    def __repr__(self):
        return str(self)

