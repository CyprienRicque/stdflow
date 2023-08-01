from typing import List

from stdflow.step import Step


class Pipeline:
    def __init__(self, steps: List[Step] = None):
        self.steps: List[Step] = steps or []

    def verify(self):
        is_valid = True
        for step in self.steps:
            is_valid = is_valid and step.verify()

    def add_step(self, step: Step):
        self.steps.append(step)
        return self

    def run(self, **kwargs):
        for step in self.steps:
            print(f"Running step {step._exec_file_path}")  # FIXME
            print(f"Running with vars {step._exec_env_vars}")
            step.run(**kwargs)
            print(f"Step {step._exec_file_path} finished")

    def __call__(self):
        self.run()


