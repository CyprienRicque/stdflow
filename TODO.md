Comment like nbdev to ignore some cells when running the notebook

json type export

integration with pandas

auto delete previous version

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



