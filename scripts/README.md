# Scripts
This folder contains the Python scripts that are used to generate the Jupyter Notebooks (using VSCodium/VSCode) and other function definitions.

## Users

If you just want to use the Jupyter Notebooks, the only interesting thing  for you in this folder currently is the `fg_wiag_ids_functions.py` file. Some functions that were making the notebook `fg_wiag_ids` more difficult to read/use were moved to that file. Unless you want to know exactly what is happening and why, you can just ignore the file.

## Developers
Combining Jupyter Notebooks and git does not work well. It's often very difficult to tell what exactly changed in a notebook using git. On the other hand, if you use Python scripts, it's much easier to tell what changed and when. This is why the sync_notebooks are developed by using Python scripts as the source of truth and the notebooks are generated whenever something has changed.