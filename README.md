# stdflow

Data flow tool that transform your notebooks and python files into pipeline steps by standardizing the data input /
output. (for data science projects)

Create clean data flow pipelines just by replacing you `pd.read_csv()` and `df.to_csv()` by `sf.load()` and `sf.save()`.

![viz_tool.png](res%2Fviz_tool.png)

## Pipelines

```python
import stdflow as sf
from stdflow import Step
from stdflow.pipeline import Pipeline

# set a stdflow variable to be used by a pipeline calling this pipeline notebook
root = sf.var("preprocessing_path", "./")  

def path(ntb):
    os.path.join(root, ntb)

files = [
    "1. formatting.ipynb",
    "2. remove_outliers.ipynb",
    "3. missing_values_imputation.ipynb",
    "4. scaling.ipynb",
]

# create a pipeline with 4 steps
ppl = Pipeline([Step(exec_file_path=ntb) for ntb in files])

# add step 5 twice with different parameters
ppl.add_step(
    Step(
        exec_file_path=path("5. merge.ipynb"),
        exec_variables={
            "country": "france",  # stdflow variable in the notebook "5. merge.ipynb" is configurable
        },
    )
)
ppl.add_step(
    Step(
        exec_file_path=path("5. merge.ipynb"),
        exec_variables={
            "country": "spain",
        },
    )
)

# run the pipeline
ppl.run()
```

## Load and save data

**Specify everything**

```python
import stdflow as sf
import pandas as pd

# load data from ./data/raw/france/step_raw/v_1/countries of the world.csv
df = sf.load(
   root="./data", 
   attrs=['twitter', 'france'], # or attrs='twitter/france'
   step='raw', 
   version='1', 
   file_name='countries of the world.csv',
   method=pd.read_csv  # or method='csv'
)

# export data to ./data/raw/france/step_processed/v_1/countries.csv
sf.save(
   df, 
   root="./data", 
   attrs=['twitter', 'france'], 
   step='processed', 
   version='1', 
   file_name='countries.csv', 
   method=pd.to_csv  # or method='csv'  or any function that takes the object to export as first input 
)
```

Each time you perform a save, a metadata.json file is created in the folder.
This keeps track of how your data was created and other information.

**Specify almost nothing**

```python
import stdflow as sf

# use package level default values
sf.root = "./data"
sf.attrs = ['twitter', 'france']  # if needed use attrs_in and attrs_out
sf.step_in = 'raw'
sf.step_out = 'processed'

df = sf.load()  
# ! root / attrs / step : used from default values set above
# ! version : the last version was automatically used. default: ":last"
# ! file_name : the file, alone in the folder, was automatically found
# ! method : was automatically used from the file extension

sf.save(df)
# ! root / attrs / step : used from default values set above
# ! version: used default %Y%m%d%H%M format
# ! file_name: used from the input (because only one file)
# ! method : inferred from file name

```

Note that everything we did at package level can be done with the Step class
```python
from stdflow import Step

step = Step(root="./data", attrs=['twitter', 'france'], step_in='raw', step_out='processed')
# or set after
step.root = "./data"
# ...

df = step.load(version=':last', file_name=":auto", verbose=True)

step.save(df, verbose=True)
```


## Data visualization

```python
import stdflow as sf
sf.save({'what?': "very cool data"}, export_viz_tool=True) # exports viz folder
```

The viz folder contains a html page that is going to load the metadata.json file in 
the parent dir (where you exported) and display the data pipeline.

# Under the hood

## Data Organization

### Format

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


### Pipeline

A pipeline is composed of steps
each step should export the data by using export_tabular_data function which does the export in a standard way
a step can be

- a file: jupyter notebook/ python file
- a python function


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
- In one step, export only to one path (except the version). meaning for one step only one combination of attrs and step_name
- Do not set sub-dirs within the export (i.e. version folder is the last depth). if you need similar operation 
  for different datasets, create pipelines






---

TODO: add pipelines
TODO: add excalidraw schema
TODO: add import export of other data types: [structured, unstructured, semi-structured]
TODO: add test loop
TODO: example architecture with
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
TODO: common steps of moving a file / deleting a file (requires pipeline)
TODO: version :last should use the metadata (datetime in file and of the file to know which one is the last)
TODO: option to delete previous version when saving


TODO: setup the situation in which you chain small function in a directory and it deletes the previous file 
  before creating a new one with another name. in the chain it will appear with different names showing the process
TODO: a processing step can delete the loaded files.
TODO: setting export=False ? delete_after_n_usage=4 ? 



