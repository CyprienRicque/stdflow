from __future__ import annotations

import importlib.util
import logging
import os

import nbformat
import pandas as pd
from colorama import Fore, Style
from jupyter_client.kernelspec import NoSuchKernel
from nbclient.exceptions import CellExecutionError
from nbconvert.preprocessors import ExecutePreprocessor
from traitlets.config import Config
from stdflow.stdflow_utils.kernel import find_current_kernel_name, list_kernels

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# :current kernel
def get_current_kernel() -> str:
    return find_current_kernel_name()


# :target kernel
def get_target_kernel(nb) -> str:
    return nb.metadata.kernelspec.name


#: any_available kernel
def get_available_kernel() -> str:
    return list_kernels(verbose=False)[0]


def get_kernel(kernel: str, nb_target) -> str:
    if kernel == ":current":
        return get_current_kernel()
    elif kernel == ":target":
        return get_target_kernel(nb_target)
    elif kernel == ":any_available":
        return get_available_kernel()
    else:
        return kernel


class KernelManager:
    def __init__(self, kernel, kernels_on_fail, nb_target):
        self.kernel = kernel
        self.kernels_on_fail = kernels_on_fail or []
        self.nb_target = nb_target

    def __iter__(self):
        for k in [self.kernel] + self.kernels_on_fail:
            kernel = get_kernel(k, self.nb_target)
            if not kernel:  # do not return None kernels
                continue
            yield kernel


def run_notebook(
    path,
    env_vars,
    run_path: str,
    kernel: str,
    kernels_on_fail: list | str | None,
    save_notebook: bool = False,
    verbose=False,
    **kwargs,
):
    # Set environment variables
    # Load notebook
    # print("cwd", os.getcwd())
    # print("exists?", os.path.exists(path))

    with open(path) as f:
        nb = nbformat.read(f, as_version=4)

    # Create a new code cell with information about the notebook
    #         info_cell = nbformat.v4.new_code_cell(
    #             source=f"""
    #         nb.cells.insert(0, info_cell)  # Insert the cell at the beginning of the notebook

    # Configure and run the notebook
    # c = get_config()
    # c.IPKernelApp.extensions = [ext for ext in c.IPKernelApp.extensions if ext != "bq_stats"]

    c = Config()

    if "timeout" in kwargs:
        c.ExecutePreprocessor.timeout = kwargs["timeout"]
    # c.ExecutePreprocessor.timeout = 600   # Set execution timeout

    try:
        kernel_manager = KernelManager(kernel, kernels_on_fail, nb)
        for kernel in kernel_manager:
            try:
                if verbose is True:
                    print(f"Run with kernel: {kernel}")
                c.ExecutePreprocessor.kernel_name = kernel
                logger.debug(c)
                ep = ExecutePreprocessor(config=c)
                out = ep.preprocess(nb, resources={"metadata": {"path": run_path}})
                break  # break loop on kernel exec success
            except NoSuchKernel:
                print(Fore.RED + f"Kernel {kernel} not found" + Style.RESET_ALL)
                continue
        # executed cell has "ExecuteTime" metadata out[0]['cells'][-1]['metadata']['ExecuteTime']['end_time']

        execution_time, last_cell_executed = get_execution_time(out)
        try:
            print(f"\tPath: {path}")
            print(f"\tDuration: {execution_time}")
            print(f"\tEnv: {env_vars}")

            if "outputs" in last_cell_executed and kwargs.get("verbose", False):
                for output in last_cell_executed["outputs"]:
                    if "text" in output:
                        print(f"\tLast cell output: [[{output['text'].strip()}]]")

        except KeyError:
            # logger.warning("Internal error generating the execution report.")
            print(Fore.RED + "Error generating the execution report" + Style.RESET_ALL)

        finally:
            print(
                Style.BRIGHT
                + Fore.GREEN
                + "Notebook executed successfully."
                + Style.RESET_ALL
            )

    except CellExecutionError as e:
        # msg = 'Error executing the notebook "%s".\n\n' % notebook_filename
        # msg += 'See notebook "%s" for the traceback.' % notebook_filename_out
        # print(msg)
        print(
            Style.BRIGHT
            + Fore.RED
            + "Error executing the notebook: "
            + Style.RESET_ALL
            + path
        )
        raise e
    except Exception as e:
        print(
            Style.BRIGHT
            + Fore.RED
            + "Error executing the notebook: "
            + Style.RESET_ALL
            + path
        )
        # logger.error(f"Error executing the notebook: {path}")
        raise e
    finally:
        if save_notebook:
            with open(path, mode="w", encoding="utf-8") as f:
                nbformat.write(nb, f)
    return True


def get_execution_time(out):
    first_cell_executed = next(
        (
            c
            for c in out[0]["cells"]
            if "metadata" in c and "execution" in c["metadata"]
        ),
        None,
    )
    last_cell_executed = next(
        (
            c
            for c in out[0]["cells"][::-1]
            if "metadata" in c and "execution" in c["metadata"]
        ),
        None,
    )
    logger.debug(f"notebook execution result: {out}")
    execution_time = pd.to_datetime(
        last_cell_executed["metadata"]["execution"]["iopub.status.idle"]
    ) - pd.to_datetime(
        first_cell_executed["metadata"]["execution"]["iopub.status.busy"]
    )
    return execution_time, last_cell_executed


def run_function(path, function_name, env_vars=None, **kwargs):
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


def run_python_file(path, env_vars=None, **kwargs):
    # Set environment variables
    if env_vars is not None:
        os.environ.update(env_vars)

    # Read file
    with open(path, "r") as file:
        python_code = file.read()

    # Execute Python code
    try:
        exec(python_code)
    except Exception as e:
        print(f"Error executing the Python file: {str(e)}")
        raise

    print("Python file executed successfully.")


if __name__ == "__main__":
    env = {"stdflow__0__vars": "coucou"}
    # put in env
    for key, value in env.items():
        os.environ[key] = value

    run_notebook(
        "../../_experiments/experiment_ntb.ipynb",
        run_path="../../_experiments/",
        kernel=":any_available",
        kernels_on_fail=None,
        env_vars=env,
        verbose=True,
    )
    # run_function("./demo/experiment_fn.py", "export_env_var", env_vars={"stdflow_hello": "coucou"})
    # run_python_file("./demo/python_script.py", env_vars={"stdflow_hello": "coucou"})
