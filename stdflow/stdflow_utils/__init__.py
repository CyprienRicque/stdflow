from __future__ import annotations

import datetime
import glob
import os
import re
import shutil
import uuid
from typing import List, Union

from stdflow.config import STEP_PREFIX, VERSION_PREFIX
from stdflow.stdflow_html.template import template

# from graphviz import Digraph


def get_creation_time(file_path) -> None | datetime.datetime:
    # Check if file exists
    if os.path.isfile(file_path):
        # Get the timestamp when the file was created
        timestamp = os.path.getctime(file_path)

        # Convert timestamp to datetime
        creation_time = datetime.datetime.fromtimestamp(timestamp)

        # Return the creation time
        return creation_time
    else:
        return None


def get_arg_value(arg, default):
    if arg == ":default":
        return default
    return arg


def detect_folders(path: str, prefix: str) -> list:
    full_path_versions = sorted(glob.glob(os.path.join(path, f"{prefix}*")))
    suffixes = [os.path.basename(v)[len(prefix) :] for v in full_path_versions if os.path.isdir(v)]
    return suffixes


def retrieve_from_path(path: str, prefix: str) -> str:
    version = [part[len(prefix) :] for part in path.split(os.sep) if part.startswith(prefix)]
    return version[-1] if version else None


def fv(version):
    """fv: format version"""
    return f"{VERSION_PREFIX}{version}" if version else None


def fstep(step):
    """fstep: format step"""
    return f"{STEP_PREFIX}{step}" if step else None


def remove_dir(path, dir_to_remove):
    """
    From a given path remove the first dir found matching the given dir name starting from the end of the path
    :param path: Initial path
    :param dir_to_remove: Directory to be removed
    :return: Corrected path
    """
    path_parts = []

    # Keep splitting the path until you reach the top
    while path != os.path.dirname(path):  # os.path.dirname(path) returns the directory part of path
        path, tail = os.path.split(path)
        if tail == dir_to_remove:
            # Skip the directory to be removed
            break
        path_parts.insert(0, tail)

    return os.path.join(path, *path_parts)


# def get_pipeline(metadata, dest):
#     dot = Digraph("Pipeline")
#
#     # For each file, create a node, and for each input_file, draw an edge
#     for file in metadata["files"]:
#         dot.node(file["file_name"])
#         for input_file in file.get("input_files", []):
#             # find the file in the list of files using uuid
#             print(f"dest={dest}")
#             print(f"input_file['uuid']={input_file['uuid']}")
#             print([f["uuid"] for f in metadata["files"]])
#
#             input_file = [f for f in metadata["files"] if f["uuid"] == input_file["uuid"]][0]
#             dot.edge(input_file["file_name"], file["file_name"])
#
#     # Save the graph in DOT format
#     dot.save(os.path.join(dest, "pipeline.dot"))
#     os.system("dot -Tpng pipeline.dot -o pipeline.png")
#     dot.format = "svg"
#     dot.render("pipeline")


def to_html(metadata_file, dest):
    import json

    from jinja2 import Environment, FileSystemLoader

    # Load metadata
    with open(metadata_file) as f:
        metadata = json.load(f)

    # get_pipeline(metadata, dest)

    # Set up Jinja2 environment
    # env = Environment(loader=FileSystemLoader("."))
    # template = env.get_template("stdflow/html/template.html")

    # Convert to JavaScript
    js_data = json.dumps(metadata)

    # Write JS data into HTML file
    with open(os.path.join(dest, "data.js"), "w") as js_file:
        js_file.write(f"var data = {js_data};")

    # dump the content of template variable in dest/template.html
    with open(os.path.join(dest, "template.html"), "w") as html_file:
        html_file.write(template)


def string_to_uuid(input_string):
    return uuid.uuid5(uuid.NAMESPACE_DNS, input_string)


#     # Write output to HTML file
#     with open(os.path.join(dest, "pipeline.html"), "w") as html_file:
#         # html_file.write(output)
#         html_file.write(
#             """
# <html>
# <body>
# <h1>Pipeline</h1>
# <img src="pipeline.png" alt="Pipeline">
# </body>
# </html>
# """
#         )


if __name__ == "__main__":
    to_html(
        dest="./",
        metadata={
            "files": [
                {
                    "file_name": "df_features",
                    "uuid": "uuid2",
                    "file_type": "csv",
                    "step": {
                        "path": "twitter/global",
                        "version": "3",
                        "step_name": "features",
                    },
                    "columns": [
                        {
                            "name": "datetime",
                            "type": "datetime",
                            "description": "date and time",
                        },
                        {"name": "text", "type": "string", "description": "tweet text"},
                        {
                            "name": "sentiment",
                            "type": "string",
                            "description": "sentiment",
                        },
                        {
                            "name": "sentiment_score",
                            "type": "float",
                            "description": "sentiment score",
                        },
                    ],
                    "input_files": [
                        {"uuid": "uuidfr"},
                        {"uuid": "uuides"},
                        {"uuid": "uuidraw_preped"},
                    ],
                },
                {
                    "name": "df_pp",
                    "uuid": "uuidfr",
                    "type": "csv",
                    "step": {
                        "path": "twitter/fr",
                        "version": "5",
                        "step_name": "preprocessing",
                    },
                    "columns": [
                        {
                            "name": "datetime",
                            "type": "datetime",
                            "description": "date and time",
                        },
                        {"name": "text", "type": "string", "description": "tweet text"},
                    ],
                    "input_files": [
                        {"uuid": "uuidbasenorth"},
                        {"uuid": "uuidbasesouth"},
                        {"uuid": "uuidraw_preped"},
                    ],
                },
                {
                    "name": "df_pp",
                    "uuid": "uuides",
                    "type": "csv",
                    "step": {
                        "path": "twitter/es",
                        "version": "1",
                        "step_name": "preprocessing",
                    },
                    "columns": [
                        {
                            "name": "datetime",
                            "type": "datetime",
                            "description": "date and time",
                        },
                        {"name": "text", "type": "string", "description": "tweet text"},
                    ],
                    "input_files": [
                        {"uuid": "uuidbasenorth"},
                        {"uuid": "uuidbasesouth"},
                    ],
                },
                {
                    "name": "df_raw",
                    "uuid": "uuidbasenorth",
                    "type": "csv",
                    "step": {},
                    "columns": [],
                    "input_files": [],
                },
                {
                    "name": "df_raw",
                    "uuid": "uuidbasesouth",
                    "type": "csv",
                    "step": {},
                    "columns": [],
                    "input_files": [],
                },
                {
                    "name": "df_already_prep",
                    "uuid": "uuidraw_preped",
                    "type": "csv",
                    "step": {},
                    "columns": [],
                    "input_files": [],
                },
            ]
        },
    )
