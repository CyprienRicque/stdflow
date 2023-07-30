from typing import List

from stdflow.step import Step


class Pipeline:
    def __init__(self, steps: List[Step] = None):
        self.steps = steps or []

    def verify(self):
        is_valid = True
        for step in self.steps:
            is_valid = is_valid and step.verify()

    def add_step(self, step: Step):
        self.steps.append(step)
        return self

    def run(self):
        for step in self.steps:
            step.run()

    def __call__(self):
        self.run()


