from __future__ import annotations

import os
import nbformat
from nbconvert.preprocessors import ExecutePreprocessor
from nbconvert import NotebookExporter
from jupyter_client.kernelspec import KernelSpecManager, NoSuchKernel
from ipykernel.kernelspec import RESOURCES

from typing import Optional
import logging
import json


def find_current_kernel_name(verbose=False) -> Optional[str]:
    import ipynbname
    import nbformat

    try:
        notebook_name = ipynbname.name()
    except FileNotFoundError:
        logging.warning("current notebook name not found")
        return None

    with open(f"./{notebook_name}.ipynb") as f:
        nb = nbformat.read(f, as_version=4)
    kernel_name = nb.metadata.kernelspec.name
    if verbose:
        print("current kernel:", kernel_name)

    # # Find resource directory (path to the kernel)
    # if verbose:
    #     print(f"resources: {RESOURCES}")
    # resource_dir = os.path.dirname(RESOURCES)
    #
    # if verbose:
    #     print(f"resource dir: {resource_dir}")
    # # Use the KernelSpecManager to find the kernels and their names
    # kernelspec_manager = KernelSpecManager()
    # try:
    #     kernel_name = kernelspec_manager.get_kernel_spec(resource_dir).name
    # except NoSuchKernel:
    #     logging.warning("no kernel found")
    #     return None
    return kernel_name


def list_kernels(verbose: bool = False) -> list[str]:
    # list ipykernels
    # jupyter kernelspec list
    # show current default ipykernel
    # jupyter kernelspec list --json
    # list all ipykernels
    # jupyter kernelspec list --json
    ksm = KernelSpecManager()
    kernels = ksm.get_all_specs()
    for kernel_name, kernel_info in kernels.items():
        if verbose:
            print(f"Kernel Name: {kernel_name}")
            print(json.dumps(kernel_info, indent=4))
            print("-" * 40)

    return [kernel_name for kernel_name, kernel_info in kernels.items()]


if __name__ == "__main__":
    print(find_current_kernel_name(verbose=True))
