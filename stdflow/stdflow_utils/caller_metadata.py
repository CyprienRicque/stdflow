import logging
import os

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def get_caller_metadata():
    import inspect
    stack = inspect.stack()

    # find function called right before stdflow_functions
    stdflow_detected = False
    user_context = None

    for context in stack:
        # logger.debug(f"context: {context.filename, context.function, context.frame.f_globals['__package__'], context.frame.f_globals}\n\n")
        if context.frame.f_globals['__package__'] == "stdflow":  # FIXME if someone has a package named stdflow, this will break
            # logger.debug(f"caller context: {context.filename, context.function, context.frame.f_globals['__package__'], context.frame.f_globals}\n\n")
            stdflow_detected = True
        elif stdflow_detected:
            user_context = context
            break

    return user_context.filename, user_context.function, user_context.frame.f_globals['__package__']# , user_context.frame.f_globals


def get_notebook_name():
    import ipynbname
    try:
        return ipynbname.name()
    except FileNotFoundError:
        return None


def get_notebook_path():
    import ipynbname
    try:
        full_path = ipynbname.path()
        name = os.path.basename(full_path)
        path = os.path.dirname(full_path)
        if path is None:
            try:
                return os.getcwd(), name
            except:
                return None, None
        return path, name
    except:
        return None, None


def get_calling_package__():
    import inspect
    import sys
    stack = inspect.stack()
    # get the frame of the caller
    frame = stack[1][0]
    # get the module name of the caller
    logger.info(f"frame.f_globals: {frame.f_globals}")
    module_name = inspect.getmodulename(frame.f_globals['__file__'])

    # If the module name is '__main__', it means the script is being run directly,
    # so it won't be part of any package
    if module_name == '__main__':
        return None

    # Now we find the package name. We'll do this by finding the top-most
    # package that this module is a part of.
    module = sys.modules[module_name]
    while '.' in module.__name__:
        module = sys.modules[module.__name__.rpartition('.')[0]]

    return module.__name__
