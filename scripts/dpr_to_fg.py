#%% [markdown]
## 6. Digitales Personenregister (DPr) to FactGrid
### Update GSNs in FactGrid
#This notebook streamlines the synchronization of personal data between two databases, DPr and FactGrid. After loading DPr-data from a CSV-file and retrieving FactGrid-data via SPARQL queries, discrepancies in the GSN (Germania Sacra Number) are identified. Afterwards the script generates QuickStatements for FactGrid and saves them in a CSV-file.
#%% [markdown]
### 1. Export data from DPr
#
#For this step you have to manually export the datasets by following the steps. In case the text description is not enough, a description with screenshots can be found [here](https://github.com/WIAG-ADW-GOE/sync_notebooks/blob/main/docs/Run_SQL_Query_and_Export_CSV.md).
#
#1. open [phpMyAdmin DPr](https://personendatenbank.germania-sacra.de/phpmyadmin/)
#2. log in 
#3. select the 'gso' database
#4. switch to the 'SQL' tab
#5. copy [this query](queries/select_dpr_with_deleted.sql), paste it in the text field and click 'Go'
#6. export the result to a csv file
#%% [markdown]
### 2. Import the file
#Please **move the downloaded file** to the `input_path` directory defined below or **change the `input_path`** to where the file is located.
#
#In case you renamed the file (e.g. to include the date on which it was created) you also need to **change the `filename`** below.
#%%
import requests
import csv
import os
import pandas as pd
import json
import re
import time
from datetime import datetime, timedelta
import math
import traceback

input_path = r"C:\Users\Public\sync_notebooks\input_files"
filename = 'persons.csv'
#%%
pr_df = pd.read_csv(os.path.join(input_path, filename), header = 0, names=["fg_id", "id", "gsn", 'is_deleted'])
#%% [markdown]
### 3. Import data from FactGrid
#Data is downloaded and and cleaned for further processing automatically.
#%%
url = 'https://database.factgrid.de/sparql'
query = (
"""SELECT ?item ?gsn WHERE {
  ?item wdt:P472 ?gsn.
}""")

r = requests.get(url, params={'query': query}, headers={"Accept": "application/json"})
data = r.json()
factgrid_df = pd.json_normalize(data['results']['bindings'])

len(factgrid_df)
#%%
#extract out q id
def extract_qid(df, column):
    df[column] = df[column].map(lambda x: x.strip('https://database.factgrid.de/entity/'))

#drop irrelevant columns
def drop_type_columns(df):
    df.drop(columns=[column for column in df.columns if column.endswith('type')], inplace=True)
    df.drop(columns=[column for column in df.columns if column.endswith('xml:lang')], inplace=True)

#%%
drop_type_columns(factgrid_df)
extract_qid(factgrid_df, 'item.value')
factgrid_df.columns = ['FactGrid_ID', 'gsn']
#%% [markdown]
### 4. Compare data from DPr and FG
#Joining the data and showing a sample to give an idea of what the data looks like.
#%%
joined_df = pr_df.merge(factgrid_df, left_on='fg_id', right_on='FactGrid_ID', suffixes=('_dpr', '_fg'))
joined_df
#%% [markdown]
#Only considering entries which were not deleted (in DPr) and where the FactGrid-entry points to a different DPr-entry. It's important that before running this notebook, the in the notebook before (step 7) the 
#have a different GSN in 
#%%
unequal_df = joined_df[(joined_df['is_deleted'] == 0) & (joined_df['gsn_dpr'] != joined_df['gsn_fg'])]
unequal_df
#%%
export_csv = unequal_df[['fg_id', 'gsn_dpr', 'gsn_fg']]
export_csv = export_csv.rename(columns={'fg_id': 'qid', 'gsn_dpr': 'P472', 'gsn_fg': '-P472'})
export_csv
#%% [markdown]
### 5. Update FactGrid
#### Generate QuickStatements to update FG
#Please **change the `output_path`** to where you want the CSV-file to be saved to.
#%%
output_path = r"C:\Users\Public\sync_notebooks\output_files"
#%%
today_string = datetime.now().strftime('%Y-%m-%d')
output_path = r"C:\Users\Public\sync_notebooks\output_files"

export_csv["-P472"] = export_csv["-P472"].apply(lambda x: f'"{x}"')
export_csv["P472"] = export_csv["P472"].apply(lambda x: f'"{x}"')
export_csv.to_csv(
    os.path.join(
        output_path,
        f'factgrid_dpr_id_update_{today_string}.csv'
    ),
    index=False
)
#%% [markdown]
#### Upload the file
#Once the file has been generated, please open [QuickStatements](https://database.factgrid.de/quickstatements/#/batch) and **run the CSV-commands**. More details to perform this can be found [here](https://github.com/WIAG-ADW-GOE/sync_notebooks/blob/main/docs/Run_factgrid_csv.md).
#### Next notebook
#There is no next notebook. The workflow is complete.