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

    def __str__(self):
        s = f"""==== PIPELINE ====\n\n"""

        for i, step in enumerate(self.steps):
            s += f"""STEP {i}
\tpath: {step._exec_file_path}
\tvars: {step._exec_env_vars}

"""
        s += f"""== END PIPELINE ==\n"""
        return s

    def __repr__(self):
        return str(self)


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
            ppl.add_step(Step(exec_file_path=file, exec_variables=env))
    print(ppl)
