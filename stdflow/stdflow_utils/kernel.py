import os
import nbformat
from nbconvert.preprocessors import ExecutePreprocessor
from nbconvert import NotebookExporter
from jupyter_client.kernelspec import KernelSpecManager, NoSuchKernel
from ipykernel.kernelspec import RESOURCES

from typing import Optional
import logging


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
        print("using kernel: ", kernel_name)

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


if __name__ == "__main__":
    print(find_current_kernel_name(verbose=True))
