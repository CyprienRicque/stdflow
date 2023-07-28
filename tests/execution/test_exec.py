import unittest
import os
import random
import string

from stdflow.stdflow_utils.execution import run_python_file, run_function, run_notebook


class TestEnvExport(unittest.TestCase):
    def setUp(self):
        self.env_vars = {
            f'__stdflow__{"".join(random.choices(string.ascii_letters, k=5))}': str(
                random.randint(0, 100)
            )
            for _ in range(10)
        }
        os.environ.update(self.env_vars)

    def test_env_export_notebook(self):
        run_notebook("tests/execution/env_export_notebook.ipynb", env_vars=self.env_vars)
        with open("/tmp/env_notebook.txt", "r") as f:
            content = f.read().splitlines()
        for var, value in self.env_vars.items():
            self.assertIn(f"{var}={value}", content)

    def test_env_export_function(self):
        run_function("tests/execution/env_export_function.py", "export_env_var", env_vars=self.env_vars)
        with open("/tmp/env_function.txt", "r") as f:
            content = f.read().splitlines()
        for var, value in self.env_vars.items():
            self.assertIn(f"{var}={value}", content)

    def test_env_export_script(self):
        run_python_file("tests/execution/env_export_script.py", env_vars=self.env_vars)
        with open("/tmp/env_script.txt", "r") as f:
            content = f.read().splitlines()
        for var, value in self.env_vars.items():
            self.assertIn(f"{var}={value}", content)


if __name__ == "__main__":
    unittest.main()
