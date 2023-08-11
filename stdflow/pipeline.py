from __future__ import annotations
from colorama import Fore, Style

from tqdm.notebook import tqdm
import logging
from typing import List

from stdflow import StepRunner
from stdflow.step import Step

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)


class Pipeline:
    def __init__(self, steps: List[StepRunner] | StepRunner = None, *args):
        steps = [steps] if isinstance(steps, StepRunner) else steps or []
        steps += list(args) if args else []
        self.steps: List[StepRunner] = steps or []

    def verify(self):
        is_valid = True
        for step in self.steps:
            is_valid = is_valid and step.is_valid()

    def add_step(self, step: StepRunner | str = None, **kwargs):
        if isinstance(step, str):
            kwargs["file_path"] = step
            step = StepRunner(**kwargs)
        self.steps.append(step)
        return self

    def run(self, progress_bar: bool = False, **kwargs):
        longest_worker_path_adjusted = max([len(step.worker_path) for step in self.steps])
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
            end = " " * 4
            start = f"    {i+1:02}."
            separator_line_len = max(longest_worker_path_adjusted + len(start) + min_blank + len(end), 25)
            separator_line = Style.BRIGHT + "=" * separator_line_len + Style.RESET_ALL
            blank = separator_line_len - len(start) - len(text) - len(end)

            print(separator_line)
            print(Style.BRIGHT + f"{start}{blank * ' '}" + f"{text}" + Style.RESET_ALL)
            print(separator_line + Style.RESET_ALL)

            print(f"Variables: {step.env_vars}")
            # Run step
            step.run(**kwargs)

            print("", end="\n\n")

    def __call__(self):
        self.run()

    def __str__(self):
        s = Style.BRIGHT + """
================================
            PIPELINE            
================================

""" + Style.RESET_ALL

        for i, step in enumerate(self.steps):
            s += f"""{Style.BRIGHT}STEP {i+1}{Style.RESET_ALL}
\tpath: {step.worker_path}
\tvars: {step.env_vars}

"""
        s += f"""{Style.BRIGHT}================================{Style.RESET_ALL}\n"""
        return s

    def __repr__(self):
        return str(self)


if __name__ == "__main__":
    ppl = Pipeline()
    ppl.add_step(exec_file_path="./demo/experiment_ntb.ipynb", exec_variables={"hello": "coucou"})
    ppl.run()


if __name__ == "__main__":
    from itertools import product

    countries = ["india", "indonesia"]
    targets = ["meta_impressions", "yt_impressions"]

    files = [
        "1_feature_eng_platform_focus.ipynb",
        "2_feature_eng_blanket.ipynb",
        "3_base_feature_selection.ipynb",
        # "4_feature_eng_linear_transformation.ipynb",
        # "5_feature_selection.ipynb",
        # "6_manual_feature_selection.ipynb",
        # "7_lrl_comp_split.ipynb",
    ]

    run_with_countries = files
    run_with_targets = [
        "3_base_feature_selection.ipynb",
        # "4_feature_eng_linear_transformation.ipynb",
        # "5_feature_selection.ipynb",
        # "6_manual_feature_selection.ipynb",
    ]

    ppl = Pipeline()

    for file in files:
        l = []
        l += [countries] if file in run_with_countries else [[None]]
        l += [targets] if file in run_with_targets else [[None]]
        for country, target in product(*l):
            env = {"country": country}
            if target:
                env["target"] = target
            ppl.add_step(StepRunner(exec_file_path=file, exec_variables=env))
    print(ppl)
