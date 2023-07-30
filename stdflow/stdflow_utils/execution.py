import nbformat
from traitlets.config import Config
from nbconvert.preprocessors import ExecutePreprocessor

import os
import importlib.util

import logging


logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def run_notebook(path, env_vars=None, **kwargs):
    # Set environment variables
    try:
        if env_vars is not None:
            logger.debug(f"Setting environment variables: {env_vars}")
            print(f"Setting environment variables: {env_vars}")
            os.environ.update(env_vars)

        # Load notebook
        with open(path) as f:
            nb = nbformat.read(f, as_version=4)

        # list ipykernels
        # jupyter kernelspec list
        # show current default ipykernel
        # jupyter kernelspec list --json
        # list all ipykernels
        # jupyter kernelspec list --json


        # Configure and run the notebook
        c = Config()
        if "timeout" in kwargs:
            c.ExecutePreprocessor.timeout = kwargs["timeout"]
        # c.ExecutePreprocessor.timeout = 600   # Set execution timeout

        if "kernel_name" in kwargs:
            c.ExecutePreprocessor.kernel_name = kwargs["kernel_name"]
        # c.ExecutePreprocessor.kernel_name = 'py37'
        print(c)
        ep = ExecutePreprocessor(config=c)

        try:
            out = ep.preprocess(nb)
            print(f"notebook execution result: {out}")
        except Exception as e:
            print(f"Error executing the notebook: {str(e)}")
            raise
    except Exception as e:
        print(f"Error executing the notebook: {str(e)}")
        raise
    else:
        # delete environment variables
        if env_vars is not None:
            for key in env_vars.keys():
                del os.environ[key]

    print("Notebook executed successfully.")


def run_function(path, function_name, env_vars=None):
    # Set environment variables
    if env_vars is not None:
        os.environ.update(env_vars)

    # Load module
    spec = importlib.util.spec_from_file_location("module.name", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    # Get function
    func = getattr(module, function_name)

    # Execute function
    try:
        func()
    except Exception as e:
        print(f"Error executing the function: {str(e)}")
        raise

    print("Function executed successfully.")


def run_python_file(path, env_vars=None):
    # Set environment variables
    if env_vars is not None:
        os.environ.update(env_vars)

    # Read file
    with open(path, 'r') as file:
        python_code = file.read()

    # Execute Python code
    try:
        exec(python_code)
    except Exception as e:
        print(f"Error executing the Python file: {str(e)}")
        raise

    print("Python file executed successfully.")


if __name__ == "__main__":
    run_notebook("./demo/experiment_ntb.ipynb", env_vars={"stdflow_hello": "coucou"})
    run_function("./demo/experiment_fn.py", "export_env_var", env_vars={"stdflow_hello": "coucou"})
    run_python_file("./demo/python_script.py", env_vars={"stdflow_hello": "coucou"})


