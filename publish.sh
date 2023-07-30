#!/bin/zsh

#!/bin/bash

# Check if the env file is provided
if [ $# -ne 1 ]; then
    echo "Usage: $0 <env_file>"
    exit 1
fi

# Check if the env file exists and is readable
if [ -r "$1" ]; then
    source "$1"
else
    echo "Env file not found or not readable."
    exit 1
fi

# Your script logic goes here, using the environment variables from the file
# For example:
echo "token: $PYPI_TOKEN"


# 1. Clean the project.
rm -rf dist

# 2. Build the project.
python -m build

# 2. Upload it to PyPI.
twine upload -u __token__ -p "$PYPI_TOKEN" dist/* --verbose


