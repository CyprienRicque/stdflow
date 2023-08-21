import json
import logging

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)


def display_tree(uuid, data, prefix="", last=True, depth=0, max_depth=None):
    """
    Recursively display the hierarchy in a tree format.

    Args:
    - uuid (int): UUID of the current node to display.
    - data_map (dict): Mapping from UUIDs to their respective input_files.
    - prefix (str): Prefix to display based on tree depth and position.
    - last (bool): Is the current node the last child in its level.
    - depth (int): Current depth of the node.
    - max_depth (int, optional): Maximum depth to display. If not provided, all depths are displayed.
    """
    # Prefixes for tree structure
    branch = ("└── " if last else "├── ") if depth != 0 else ""
    tree_structure = (
        prefix
        + branch
        + f"[{str(uuid)[:3]}] {data.get(uuid, {})['step']['step_name']}:{data.get(uuid, {})['step']['path']}"
    )

    # Print the current node with prefix
    print(tree_structure)

    # Get children of the current node
    children = data.get(uuid, {}).get("input_files", [])

    # Adjust prefix for children
    if depth > 0:
        prefix += "    " if last else "│   "

    # Check if we've reached the maximum depth, if specified
    if max_depth is not None and depth >= max_depth:
        return

    # Recursively display children, incrementing depth
    for index, child in enumerate(children):
        is_last_child = index == len(children) - 1
        child_uuid = child["uuid"]
        display_tree(child_uuid, data, prefix, is_last_child, depth + 1, max_depth)


def display_full_tree(data_list, max_depth=None):
    """
    Display the full tree structure with an optional max depth.

    Args:
    - data_list (list): List of nodes in the tree.
    - max_depth (int, optional): Maximum depth to display. If not provided, all depths are displayed.
    """
    # Create a map for easy lookup based on UUID
    data_map = {item["uuid"]: item for item in data_list}

    # Display the root nodes (nodes that are not children of any other node)
    child_uuids = {child["uuid"] for parent in data_list for child in parent["input_files"]}
    root_uuids = [item["uuid"] for item in data_list if item["uuid"] not in child_uuids]

    for root in root_uuids:
        display_tree(root, data_map, max_depth=max_depth)


if __name__ == "__main__":
    data_list = [
        {
            "uuid": 101,
            "input_files": [
                {"uuid": 201},
                {"uuid": 202},
                {"uuid": 203},
            ],
        },
        {
            "uuid": 201,
            "input_files": [{"uuid": 301}, {"uuid": 302}],
        },
        {
            "uuid": 202,
            "input_files": [
                {"uuid": 303},
                {"uuid": 304},
                {"uuid": 305},
            ],
        },
        {
            "uuid": 203,
            "input_files": [{"uuid": 306}, {"uuid": 307}],
        },
        {"uuid": 301, "input_files": []},
        {"uuid": 302, "input_files": []},
        {"uuid": 303, "input_files": []},
        {"uuid": 304, "input_files": []},
        {"uuid": 305, "input_files": []},
        {"uuid": 306, "input_files": []},
        {"uuid": 307, "input_files": []},
    ]
    data_list = json.load(open("stdflow/stdflow_viz/metadata.json"))

    display_full_tree(data_list["files"], max_depth=3)
