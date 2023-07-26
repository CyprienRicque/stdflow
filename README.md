# stdflow

Data flow tool that transform your notebooks and python files into pipeline steps by standardizing the data input /
output. [for Data science projects]

# Data Organization

## Format

Data folder organization is systematic and used by the function to load and save.
If follows this format:
root_data_folder/attrs_1/attrs_2/.../attrs_n/step_name/version/file_name

where:

- root_data_folder: is the path to the root of your data folder, and is not exported in the metadata
- attrs: information to classify your dataset (e.g. country, client, ...)
- step_name: name of the step. always starts with `step_`
- version: version of the step. always starts with `v_`
- file_name: name of the file. can be anything

Each folder is the output of a step. It contains a metadata.json file with information about all files in the folder
and how it was generated.
It can also contain a html page (if you set `html_export=True` in `save()`) that lets you visualize the pipeline and your metadata

## Pipeline

A pipeline is composed of steps
each step should export the data by using export_tabular_data function which does the export in a standard way
a step can be

- a file: jupyter notebook/ python file
- a python function

## How to use

Load from raw data source

```python
import stdflow as sf
sf.root = '../demo_data'  # set it as default for both load and save

df = sf.load(attrs=['raw'], file_name='countries of the world.csv')


```

### Recommended steps

You can set up any step you want. However, just like any tools there are good/bad and common ways to use it.

The recommended way to use it is:

1. Load
    - Use a custom load function to load you raw datasets if needed
    - Fix column names
    - Fix values
        - Except those for which you would like to test multiple methods that impacts ml models.
    - Fix column types
2. Merge
    - Merge data from multiple sources
3. Transform
    - Pre-processing step along with most plots and analysis
4. Feature engineering (step that is likely to see many iterations)
   > *The output of this step goes into the model*
    - Create features
    - Fill missing values
5. Model
    - This step likely contains gridsearch and therefore output multiple resulting datasets
    - Train model
    - Evaluate model (or moved to a separate step)
    - Save model

**Best Practices**:
- Do not use ```sf.reset``` as part of your final code
- Do not export to multiple path (path + attr_1/attr_2/.../attr_n + step_name) in the same step: only multiple versions
- Do not set sub-dirs within the export (i.e. version folder is the last depth). if you need similar operation 
  for different datasets, create pipelines



TODO: add pipelines
TODO: add excalidraw schema
TODO: add import export of other data types: [structured, unstructured, semi-structured]
TODO: add test loop
TODO: architecture with
- data
- pipelines
- models
- tests
- notebooks
- src
- config
- logs
- reports
- requirements.txt
- README.md
- .gitignore
TODO: setup pipelines_root, models_root, tests_root, notebooks_root, src_root, config_root, logs_root, reports_root
TODO: setup the situation in which you chain small function in a directory and it deletes the previous file 
  before creating a new one with another name. in the chain it will appear with different names showing the process
TODO: a processing step can delete the loaded files.
TODO: common steps of moving a file / deleting a file
TODO: setting export=False ? delete_after_n_usage=4 ? 


