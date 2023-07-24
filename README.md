# stdflow

Data flow tool that transform your notebooks and python files into pipeline steps by standardizing the data input /
output. [for Data science project]

# Data Organization

## Format

Data folder organization is systematic and used by the function to load and export.
If follows this format:
data_name/attrs_1/attrs_2/.../attrs_n/step_name/{data_name}_{country_code}_{step_name}_{version}_{attrs}.csv"

where:

- data_name: name of the dataset
- step_name: name of the step
- attrs: additional attributes of the dataset (such as the country)

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

# basic use-case
dfs = sf.load(
   path='./twitter/france/')  # recommended is: ./twitter/france/step_raw/v_202108021223  or (v_1 / v_demo / ...)
# or
dfs = sf.load(path='./', attrs=['twitter', 'france'], step=False, version=False)
# or
dfs = sf.load(path='./twitter', attrs=['france'], step=False)

sf.export(dfs, step="loaded")  # export in ./twitter/france/step_loaded/v_202108021223
```

Load from processed data source

```python
import pandas as pd
import stdflow as sf

dfs = sf.load(
   path='./twitter/france/step_processed/v_2_client_intern/data.csv'
)  # automatically use appropriate function if meta-data is available. otherwise, use default with detected extension
# or
dfs = sf.load(
   path='./twitter/france/step_processed/',
   step=True,  # default is True: meaning it detects it from the path
   version="2_client_intern"  # default is last version
)

sf.load(path='./twitter/france/', file='data.csv', step="processed", version="last")

sf.load(pd.read_csv, path='./twitter/france/', file='data.csv', step="processed", version="last", header=None)
sf.load(pd.read_csv, path='./twitter/france/step_processed/v_12/data.csv', header=None)

# or 
dfs = sf.load(path='./twitter/france/step_processed/', step=True, version="last")  # last version is taken
# version keywords: last, first

```

Multiple data sources

```python

dfs = sf.load(srcs=['./digimind/india/step_processed', './digimind/indonesia/step_processed'])
```


or the elements one by one

```python
sf.step_in = 'clean'
sf.version_in = 1
# ...

sf.step_name = 'preprocess'
sf.version = 1  # default to datetime
sf.attrs = ['india']  # default to []
# ...
```

attrs adds the attributes to the file name
it is also possible to use out_path. the final out_path is composed of
in_path[0] (or out_path if any) + attrs + step_name + version

```python
sf.export_tabular_data(dfs, data_path='./digimind/india/processed', step_name='clean', attrs=['india'], version=1)
```

### Data Loader

- Auto: automatically select one of the existing loader based on meta-data
- CSVLoader: loads all csv files in a folder
- ExcelLoader: loads all excel files in a folder

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

## How the package works

a step is composed of in and out data sources
data sources are just folders. The format is 
path + attr_1/attr_2/.../attr_n + step_name + version

where:
   attrs_1: usually the name of the dataset
   attrs_2...n: additional attributes of the dataset (such as the country)
   step_name: name of the step (optional but recommended so that the usage of the package makes sense)
   version: version of the data (optional but recommended) default to datetime

each time you load data, the input data sources are saved. This is useful to keep track of the data used in a step.
You can reset the loaded data by using ```sf.reset()```

At export time a file with all details about the input and output data is generated and saved in the output folder.


### Metadata 

Each folder contains one metadata file with the list of *all* files details.
Note that even if with this architecture it is technically possible to generate files in the same folder from different
steps (future-proof concerns), it is not recommended and you will get warnings.


```json
{
   "files": [
      {
         "name": "file_name",
         "type": "file_type",
         "step": {
            "attrs": [
               "attr_1",
               "attr_2",
               "...",
               "attr_n"
            ],
            "version": "version",
            "step": "step_name"
         },
         "columns": [
            {
               "name": "column_name",
               "type": "column_type",
               "description": "column_description"
            }
         ],
         "input_files": [
               ...
         ]
      },
      {
         ...
      }
   ]
}
```