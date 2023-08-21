def get_notebook_name():
    import ipynbname

    try:
        return ipynbname.name()
    except FileNotFoundError:
        return None


def get_notebook_path():
    import ipynbname

    try:
        return ipynbname.path()
    except FileNotFoundError:
        return None


def get_python_file_name():
    import os

    return os.path.basename(__file__)


def get_python_file_full_path():
    import inspect

    return inspect.getframeinfo(inspect.currentframe()).filename


def get_cwd():
    import pathlib

    return pathlib.Path().absolute()


def get_cwd2():
    import os

    cwd = os.getcwd()
    return cwd


def get_python_file_full_path_2(f=__file__):
    import os

    return os.path.realpath(f)


def get_call_python_file_name():
    import inspect

    stack = inspect.stack()
    try:
        calling_context = next(context for context in stack if context.filename != __file__)
        return calling_context.filename
    except StopIteration:
        return None


def get_call_python_function_name():
    import inspect

    stack = inspect.stack()
    print([context.function for context in stack])
    # print([context. for context in stack])
    calling_context = next(context for context in stack if context.filename != __file__)
    return calling_context.function


if __name__ == "__main__":
    print(get_notebook_name())
    print(get_notebook_path())

    print(get_cwd())
    print(get_cwd2())

    print(get_python_file_name())

    print(get_python_file_full_path())
    print(get_python_file_full_path_2())
    print(get_call_python_file_name())
