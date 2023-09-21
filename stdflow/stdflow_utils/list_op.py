from stdflow.filemetadata import FileMetaData


def alias_from_file_metadata(file_metadata: FileMetaData):
    return file_metadata.uuid


def filter_list(lst, starts_with):
    """Remove all strings in a list that do not start with"""
    new_lst = []
    for item in lst:
        if isinstance(item, list):  # If the item is a list, call the function recursively
            new_lst.append(filter_list(item, starts_with))
        elif isinstance(item, str) and item.startswith(starts_with):
            new_lst.append(item)
    return new_lst


def flatten(lst):
    """Flatten a nested list."""
    flat_list = []
    for item in lst:
        if isinstance(item, list):
            flat_list.extend(flatten(item))
        else:
            flat_list.append(item)
    return flat_list


def nested_replace(lst, old, new):
    """
    Replace occurrences of 'old' with 'new' in nested lists.
    """
    for i, item in enumerate(lst):
        if isinstance(item, list):
            lst[i] = nested_replace(item, old, new)
        elif isinstance(item, str):
            lst[i] = item.replace(old, new)
    return lst
