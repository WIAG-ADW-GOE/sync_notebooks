#%% [markdown]
## 4. Add WIAG roles, institutions, institution roles and offices to FactGrid
# 
#This notebook takes data from WIAG as the primary source and joins it with data from FactGrid to find out which institutions, roles or institution roles need to be added to FactGrid. After generating files for adding these things to FactGrid (institutions are now instead added using the workflow developed for [DomVoc](https://github.com/Germania-Sacra/DomVoc/tree/main)), at the end a file for adding all offices of persons to FactGrid is generated.
# 
#Whenever a file is generated, all entries with institutions/roles/inst roles, that still need to be added to FG, are from then on ignored so the other steps can be performed for the rest of the entries. This enables you to go through the notebook linearly from start to finish and add all four things for the majority of entries right away. This means though, that you will need to go through the notebook multiple (up to four) times to make sure FactGrid is all up-to-date.
#
#Another possible workflow for the notebook is to use the generated files right away whenever one is generated to add things to FG and after that is done, start from the beginning of the notebook again. This way you also need to go through the notebook multiple times, but only need to upload one file per thing (institutions, roles, inst roles, offices).

#%% [markdown]
### 1. Setup
#

#%%
import requests
import csv
import os
import json
import re
import time
from datetime import datetime, timezone, date
import math
import traceback
import polars as pl
import polars.selectors as cs
from enum import Enum

today_string = datetime.now().strftime('%Y-%m-%d')

#%% [markdown]
#For the automatic translation, AI models hosted by the GWDG are used. For this a [SAIA](https://docs.hpc.gwdg.de/services/saia/index.html) API key is needed. You can either uncomment the line in the cell below and replace the placeholder with your key or (safer option) create a text-file (called `.env`) in the project directory containing `API_KEY="PLACEHOLDER"` (with your key inserted) before running the cell below.

#%%
#API_KEY="PLACEHOLDER"

import scripts.translate

#%% [markdown]
#The cell below defines where input files can be found and where the generated files will be saved to. 
#%%
input_path = r"C:\Users\Public\sync_notebooks\input_files"

output_path = r"C:\Users\Public\sync_notebooks\output_files"

#%% [markdown]
### 2. Download data from WIAG
#
#### Export data via phpMyAdmin
#
#For this step you have to manually export the datasets by following the steps. In case the text description is not enough, a description with screenshots can be found [here](https://github.com/WIAG-ADW-GOE/sync_notebooks/blob/main/docs/Run_SQL_Query_and_Export_CSV.md).
#
#1. open [phpMyAdmin WIAG](https://vwebfile.gwdg.de/phpmyadmin/)
#2. log in
#3. select the 'wiagvokabulare' database
#4. switch to the 'SQL' tab
#5. copy [this query](queries/get_wiag_roles.sql), paste it in the text field and click 'Go'
#6. export the result to a csv file

#### Import the files
#
#Please move the downloaded file to the `input_path` directory defined above or change the `input_path` to where the file is located.

#%%
input_file = f'role.csv'
input_path_file = os.path.join(input_path, input_file)
wiag_roles_df = pl.read_csv(input_path_file)
len(wiag_roles_df)

#%% [markdown]
#### Export data via the website
#
#It's recommended to limit the export to one Domstift by first searching for that Domstift before exporting the 'CSV Amtsdaten' to make sure that the amount of roles to be added is manageable.
#
#1. go to https://wiag-vocab.adw-goe.de/query/can
#2. filter by cathedral chapter (Domstift)
#3. click Export->Amtsdaten
# 
#If you filtered by Domstift (cathedral chapter), **change the variable below** to the domstift you used and **change the name of the exported file** to include the name of the cathedral chapter.
#
#If you did not filter, you need to change the line to `domstift = ""`.

#%%
domstift = "Bamberg" # with domstift = "Mainz" the name of the file should be "WIAG-Domherren-DB-Ämter-Mainz.csv"
#domstift = "" # in case you did not filter by Domstift, use this instead

#%%
if domstift == "":
    input_file = f'WIAG-Domherren-DB-Ämter.csv'
else:
    input_file = f'WIAG-Domherren-DB-Ämter-' + domstift + '.csv'

input_path_file = os.path.join(input_path, input_file)
wiag_offices_df = pl.read_csv(input_path_file, separator=';', infer_schema_length = None)
len(wiag_offices_df)

#%%
last_modified = datetime.fromtimestamp(os.path.getmtime(input_path_file))
now = datetime.now()
assert last_modified.day == now.day and last_modified.month == now.month, f'The file was last updated on {last_modified.strftime('%d.%m')}'

#%% [markdown]
##### Troubleshooting: Old file used
#
#You get an error when you run the line above if the file was not updated today.
#Suggested solutions: 
#* update the file again by downloading it again
#* if you downloaded the data today, check the file name in input_file. It's pointing to a file that has old data.
#* (not recommended) continue if you are sure that you need to use old data. This is something that the developer might want to do.

#%% [markdown]
### 3. Download data from FactGrid
#
#
#
#Troubleshooting: If the following cell throws an error, try rerunning the cell. Its probably just a connection problem.

#%%
from scripts.wiag_to_factgrid_functions import load_fg_data

(factgrid_institution_df, factgrid_diocese_df, factgrid_inst_roles_df) = load_fg_data()

#%% [markdown]
#### Check for possible institution duplicates
#
#If any two (or more) institutions on FactGrid link to the same GSN, they will be listed below. These entries need to be **fixed manually**. Use the `fg_institution_id` to find and fix the entries on FG.
#
#You can either fix the duplicates now or just continue on and fix them later, because they will be ignored for the rest of the notebook.
#Not fixing them now means though, that you will need to run the notebook again at a later point.

#%%
duplicate_fg_entries = factgrid_institution_df.group_by('fg_gsn_id').len().filter(pl.col('len') > 1)
if not duplicate_fg_entries.is_empty():
    display(factgrid_institution_df.filter(pl.col('fg_gsn_id').is_in(duplicate_fg_entries.get_column('fg_gsn_id').implode())))

factgrid_institution_df = factgrid_institution_df.filter(pl.col('fg_gsn_id').is_in(duplicate_fg_entries.get_column('fg_gsn_id').implode()).not_())

#%% [markdown]
### 4. Join the data

#%% [markdown]
#First the WIAG "Amtsdaten" for Domherren export is joined with institution data from FactGrid

#%%
wiag_offices_df = wiag_offices_df.join(factgrid_institution_df, how='left', left_on='institution_id', right_on='fg_gsn_id')

#%% [markdown]
#Next the diocese data is added.
#
#For each entry in the input dataframe, the associated diocese is searched in the factgrid_diocese_df dataframe. The diocese is found by first searching for the WIAG-ID. Only if no entry was found, the search continues with the diocese's name, first in the diocese label and lastly, if the search was unsuccessfull again, in the diocese alt label.

#%%
#join with fg dioceses
rows = []
query = pl.DataFrame() # empty initialisation to enable the call of the clear function below

for row in wiag_offices_df.iter_rows(named = True):
    query = query.clear()

    if row['diocese_id'] != None:
        query = factgrid_diocese_df.filter(pl.col('dioc_wiag_id') == row['diocese_id'])
        
    if query.is_empty() and row['diocese'] != None:
        query = factgrid_diocese_df.filter(pl.col('dioc_label') == row['diocese'])
        
        if query.is_empty():
            query = factgrid_diocese_df.filter(pl.col('dioc_alt') == row['diocese'])

    if not query.is_empty():
        rows.append({'role_all-id': row['id'], 'fg_diocese_id': query.row(0)[0]})
    # #TODO should cases where no result was found be noted/handled?

wiag_offices_df = wiag_offices_df.join(pl.DataFrame(rows), how = 'left', left_on = 'id', right_on = 'role_all-id')

#%% [markdown]
### 5. Missing institutions

#%% [markdown]
#### Check for special cases
#
#These lists below allow the code below to identify if the role is missing an institution or if the role doesn't require one at all.
#* The `unbound_role_groups` list contains the role_groups that are not bound to a place at all.
#* The `diocese_role_groups` list contains the role_groups that are bound to a diocese but not an institution.
#  * `diocese_role_group_exception_roles` contains roles that belong to this group but are still bound to an institution.
#Please add more role_groups or roles to the lists if necessary.

#%%
unbound_role_groups = [
    'Kurienamt',
    'Papst',
    'Kardinal',
]
diocese_role_groups = [
    'Oberstes Leitungsamt Diözese',
    'Leitungsamt Diözese',
    'Bischöfliches Hilfspersonal',
]
diocese_role_group_exception_roles = [ 
    'Erzbischöflicher Prokurator',
]

#%%
#select all entries that should contain an institution on FactGrid but don't have it after the join operation
missing_inst_df = wiag_offices_df.filter(
    pl.col('fg_institution_id').is_null() &
    pl.col('role_group').is_in(unbound_role_groups).not_() &
    pl.col('role_group').is_in(diocese_role_groups).not_()
)
print(str(missing_inst_df.height) + " entries with missing institution id in FG")

#select all entries that should contain a diocese on FactGrid but don't have it after the join operation
missing_dioc_df = wiag_offices_df.filter(
    pl.col('fg_diocese_id').is_null() & 
    pl.col('role_group').is_in(unbound_role_groups).not_() & 
    pl.col('role_group').is_in(diocese_role_groups) &
    pl.col('name').is_in(diocese_role_group_exception_roles).not_()
)
print(str(missing_dioc_df.height) + " entries with missing diocese id in FG")

#%% [markdown]
#### Check for new roles (roles that so far have not been handled by this notebook)
#
#
#
#Any roles showing up here need to be added to the `diocese_role_group_exception_roles` list if they don't need a diocese entry in FactGrid. If you added a name to the `diocese_role_group_exception_roles` list, rerun the cells from the start of step 5 to make sure the change is propagated.
#
#
#
# - diocese is missing WIAG
# - diocese is missing in FG
# - diocese in FG does not have the Q164535 property
# - diocese in FG has a different German label and no WIAG ID -> list label from WIAG as an alternative label
# - diocese in FG is missing German label and WIAG ID -> add German label
# - role that should not be uploaded to FactGrid
# - incorrect office assignment in WIAG
# - a role that should be added to unbound_roles
# - role with incorrect role group assignment

#%%
#roles_that_need_a_diocese = ['Bischof','Koadjutor','Erzbischof']
roles_that_need_a_diocese = ['Archipresbyter','Propst und Archidiakon']
missing_dioc_df.filter(pl.col('name').is_in(roles_that_need_a_diocese).not_())
#%% [markdown]
##### Check entries that have no role group in wiag

#%%
missing_inst_df.filter(pl.col('role_group').is_null())

#%% [markdown]
##### Check for entries that are missing an id **in WIAG** required for the join
#Please **manually inspect all the entries** that are shown by the code cells below

#%% [markdown]
#Entries that have a missing institution id **in WIAG**

#%%
missing_inst_df.filter(pl.col('institution_id').is_null())

#%% [markdown]
#Entries that have a missing diocese id **in WIAG**

#%%
missing_dioc_df.filter(pl.col('diocese_id').is_null())

#%% [markdown]
#### Missing institutions
#
#If there are any institutions listed here, they should be added using the workflow that was designed for adding monasteries as part of the DomVoc project. The code can be found on [GitHub](https://github.com/Germania-Sacra/DomVoc/tree/main).

#%%
create_institution_factgrid_df = missing_inst_df.filter(pl.col('institution_id').is_not_null()).rename({'institution' : 'Lde', 'institution_id' : 'P471'}).unique(subset = pl.col('P471')).with_columns(
    qid = None,
    Len = None,
    Dde = None,
    Den = None,
    P131 = pl.lit('Q153178')
).select(['qid', 'Lde', 'Len',	'P471',	'Dde',	'Den',	'P131'])

create_institution_factgrid_df

#%% [markdown]
### 6. Missing roles
#
#These roles do not include the institution information. In other words, this step adds roles to FactGrid like 'archbishop' and not 'archbishop of trier'
#%% [markdown]
#### Remove all missing (institution and diocese) entries **

#%%
all_missing_entries = pl.concat([missing_inst_df, missing_dioc_df], how = "diagonal")

dioc_joined_df = wiag_offices_df.remove(pl.col("id").is_in(all_missing_entries.get_column("id").implode()))

print("From originally " + str(wiag_offices_df.height) + " rows, " + str(dioc_joined_df.height) + " rows, that are not missing an institution or diocese, are left.")

#%% [markdown]
#### Check for special cases
#
##### Check for roles with multiple entries in FactGrid
#
#Should the cell below print anything, these entries need to be **handled manually**, because they contain more than one entry on FactGrid. You can continue with the rest of the notebook even without taking care of these, because these entries will simply be ignored.

#%%
wiag_roles_df.filter(pl.col("name").is_duplicated())

#%% [markdown]
##### Check for missing roles in WIAG role table

#%%
missing_roles_wiag = dioc_joined_df.filter(pl.col("name").is_in(wiag_roles_df.get_column("name").implode()).not_()).unique()
print(missing_roles_wiag.height)
missing_roles_wiag.head()

#%% [markdown]
##### Join role_fg_id attribute from WIAG

#%%
wiag_roles_df = wiag_roles_df.remove(pl.col("name").is_duplicated())

joined_df = dioc_joined_df.join(wiag_roles_df.rename({'id' : 'role_id', 'factgrid_id': 'role_fg_id'}), on = "name", how = "left")


#%% [markdown]
##### Ignore all Kanonikatsbewerber and Vikariatsbewerber roles/offices
#
#The 'bewerber' suffix means, that this person was applying for this office, so these are not proper offices and don't need to be / shouldn't be added to FactGrid.

#%%
joined_df = joined_df.remove(pl.col('name').is_in(['Vikariatsbewerber', 'Kanonikatsbewerber']))

#%% [markdown]
##### Entries with missing FactGrid-entries for the roles in wiag

#%%
missing_roles_df = joined_df.filter(pl.col('role_fg_id').is_null())
print(str(missing_roles_df.height) + " entries are missing a role in FactGrid.\n")

print("Roles that are not yet in FactGrid:")
missing_roles = missing_roles_df.select(pl.col('name'), pl.col('role_id'), pl.col('role_group_fq_id')).unique().drop_nulls() # TODO report null values, instead of just dropping them
missing_roles

#%% [markdown]
#### Generate missing roles file
#
#In this step a file is prepared for the roles missing in FactGrid that can later be uploaded to add the roles to FG. Important things to note:
#
#1. The English labels for the roles are translations of the German labels. To facilitate the process of translating, a first draft of translations is generated using AI. However, the quality varies widly and it is **absolutely necessary** to check and correct the translations, since some will likely be incorrect.
#2. The generated file also contains columns for descriptions. Either fill in the descriptions or if you do not intend to add any, you should remove these columns (with e.g. Excel or LibreOffice Calc). Should you want to add descriptions only for a few roles, it might still be easier to remove the columns and add the descriptions separately after the upload, depending on the number of roles to be uploaded (FactGrid does not allow empty cells for the csv format, so you need to add descriptions for every row or none at all)
#
#After checking/correcting the translations and adding descriptions or removing the columns, you can copy the content of the generated file (name: `create-missing-roles_<date>.csv`) and paste it into the textfield on quickstatements. As mentioned above, you can either do this right away or at the end.

#%% [markdown]
#This cell generates the translations of the labels. This can take a few minutes.

#%%
system_prompt = """**Role:** You are a professional translator specializing in historical and religious terminology, with expertise in German–English translation.
    **Task:** You will receive a German name for a role or occupation. Your task is to return the most accurate and context-appropriate English translation.
    **Format:** Only return the translation. Do not add any remarks or formatting. Always start the translation with a capital letter."""
    
create_missing_roles_df = scripts.translate.translate(missing_roles.rename({"name" : "Lde"}), system_prompt)

#%% [markdown]
#this cell generates the file and show a sample of the content

#%%
create_missing_roles_df = create_missing_roles_df.with_columns(
    qid = None,
    Dde = None,
    Den = None,
    P2 = pl.lit("Q37073"),
    P131 = pl.lit("Q153178")
).rename({
    "role_id" : "item_id",
    "role_group_fq_id" : "P3"}
).select(
    ["qid",	"Lde",	"Len",	"Dde",	"Den",	"P2",	"P131",	"item_id",	"P3"]
)

create_missing_roles_df.write_csv(os.path.join(output_path, f"create-missing-roles_{today_string}.csv"))
print(f'{create_missing_roles_df.height} rows were written. Here is a sample of them:')
if create_missing_roles_df.height >= 3:
    display(create_missing_roles_df.sample(n=3))
else:
    display(create_missing_roles_df)

#%% [markdown]
### 7. Missing institution roles
#
#### Remove all missing (role) entries now **
#
#The code below removes all the entries that failed the join with the WIAG role join above.

#%%
with_roles_in_fg_df = joined_df.remove(pl.col('role_fg_id').is_null())

#%% [markdown]
#### Check for people with missing FactGrid-entries or missing FactGrid-IDs in wiag
#
#There generally shouldn't be any such persons, since notebook 3 takes care of this.
#%%
missing_people_list = joined_df.filter(pl.col('FactGrid').is_null()).unique('person_id')
print(missing_people_list.height)
missing_people_list.sample(n = 3)
#%% [markdown]
#To generate quickstatements for creating the persons, go back to [notebook 3](Csv2FactGrid-create.ipynb) (Csv2FactGrid-create).
#
#The code below removes all the entries for persons that don't exist on FactGrid

#%%
print(len(joined_df))
joined_df = joined_df.filter(pl.col('FactGrid').is_not_null())
print(len(joined_df))

#%% [markdown]
#### Find out which institution roles are missing on FactGrid
#
#these roles have information of the institution as well

#%%
#in addition to the parameters, this uses the dataframe factgrid_inst_roles_df directly

def find_fg_inst_role(name, inst, dioc):
    search_result = pl.DataFrame()
    if inst == None:
        if dioc != None: # TODO handle cases where inst and dioc are None? - should only be true for [35, 48, 49] Kardinal, Papst, Kurienamt (except maybe special role_groups)
            if name not in ["Archidiakon", "Koadjutor"]:
                dioc = dioc.lstrip('Bistum').lstrip('Erzbistum').lstrip('Patriarchat').lstrip()
            if name == "Fürstbischof" and dioc in ["Passau", "Straßburg"]:
                name = "Bischof"    
            search_result = factgrid_inst_roles_df.filter(pl.col('inst_role').str.contains(f"^{name}.*{dioc}"))
            if name == "Erzbischof" and dioc == "Salzburg":
                # will be merged in later # TODO what does this mean and why?
                search_result = factgrid_inst_roles_df.filter(pl.col('fg_inst_role_id') == 'Q172567')
    else:
        name = name.replace('Domkanoniker', 'Domherr')
        search_result = factgrid_inst_roles_df.filter(pl.col('inst_role') == f"{name} {inst}")
    
    return search_result

#%%
data_dict = [] # joined to the main df as fg_inst_role_id (used in the last part) - in other words, these are the institution roles that are assigned on FactGrid
not_found = [] # used for creating institution roles (e.g. bishop of ...) in the next cell
dupl = {} # these entries are ignored, because they need to be fixed manually

i = 0
for (id, name, inst, inst_id, dioc) in joined_df.select('id', 'name', 'institution', 'institution_id', 'diocese').iter_rows():
    # Kardinal receives insitution role Q254893 manually -- probably simply handling a simple special case first
    if name == "Kardinal":
        data_dict.append((id,"Q254893"))
        continue
    
    search_result = find_fg_inst_role(name, inst, dioc)

    if search_result.is_empty() or len(search_result) == 0:
        # TODO entries without institution entry in WIAG are simply ignored - makes sense if dioc is set?? (diocese level roles)
        not_found.append((name, inst, inst_id))
    elif len(search_result) == 1:
        data_dict.append((id, search_result['fg_inst_role_id'][0]))
    elif len(search_result) >= 2:
        dupl[i] = (name, inst, dioc, search_result)
    
    i += 1

print("Roles found:", len(data_dict), "duplicates:", len(dupl), "not found:", len(not_found))

#%% [markdown]
#### Generate missing institution roles file
#
#This step is mostly the same (some additional preprocessing steps) as for the roles (without institution). It is no less important though, to check and **correct the translations** of the labels.
#
#Once again you also need to either add descriptions (for all the rows) or remove the description columns. Afterwards you can copy the content of the generated file (name: `create-missing-inst-roles_<date>.csv`) and paste it into the textfield on quickstatements.

#%%
not_found_df = pl.DataFrame(not_found, orient = 'row', schema = ['role', 'institution', 'institution_id'])
not_found_df = not_found_df.drop_nulls() # remove entries for diocese level roles 

#not_found contains an entry per row where a combination was not found - here we want just one row per unique combination
#these combinations could be found much more efficiently, but as it's a byproduct of finding the fg_inst_role_id for all the other rows, this is fine
not_found_df = not_found_df.unique()
#since the institution names are quite specific, it's not realistic that two roles with the same label but different institution_id could exist

#add role details
not_found_df = not_found_df.join(
    wiag_roles_df.rename({'id' : 'role_id', 'factgrid_id': 'role_fg_id'}), how='left', left_on='role', right_on='name'
)
#add instution details
not_found_df = not_found_df.join(factgrid_institution_df, how='left', left_on='institution_id', right_on='fg_gsn_id')

#create label
not_found_df = not_found_df.with_columns(Lde = pl.col('role') + ' ' + pl.col('institution'))

#%% [markdown]
#This cell generates the translations of the labels. This can take a few minutes.

#%%
system_prompt = """**Role:** You are a professional translator specializing in historical and religious terminology, with expertise in German–English translation.
    **Task:** You will receive a German name for a role or occupation including a place that this role is associated with. Your task is to return the most accurate and context-appropriate English translation.
    **Format:** Only return the translation. Do not add any remarks or formatting. Always start the translation with a capital letter."""

create_miss_inst_roles = scripts.translate.translate(not_found_df.sample(n=3), system_prompt)

#%% [markdown]
#this cell generates the file and show a sample of the content

#%%
#add other columns
create_miss_inst_roles = create_miss_inst_roles.with_columns(
    qid = None,
    Dde = None,
    Den = None,
    P2 = pl.lit('Q257052'),
    P131 = pl.lit('Q153178'),
    P3 = pl.col('role_fg_id'),
    P267 = pl.col('fg_institution_id'),
    # id is the number of the role in the role table in WIAG -- institution_id is the klosterdatenbank id of the institution
    P1100 = pl.when(pl.col('role_id').is_null()).then(pl.lit(None)).otherwise('off' + pl.col('role_id').cast(str) + '_gsn' + pl.col('institution_id').cast(str))
).select(['qid', 'Lde', 'Len', 'Dde', 'Den', 'P2', 'P131', 'P3', 'P267', 'P1100']) # selecting only relevant columns

#export to csv file
create_miss_inst_roles.write_csv(os.path.join(output_path, f"create-missing-inst-roles_{today_string}.csv"))
print(f'{create_miss_inst_roles.height} rows were written. Here is a sample of them:')
if create_miss_inst_roles.height >= 3:
    display(create_miss_inst_roles.sample(n = 3))
else:
    display(create_miss_inst_roles)

#%% [markdown]
### 8. Missing offices
#
#### Ignore all missing (inst role) entries now **
#
#The code below ignores entries that are generated above and does a join without them.

#%%
final_joined_df = joined_df.join(pl.DataFrame(data_dict, schema = ['id', 'fg_inst_role_id'], orient = 'row'), on = 'id')
print(len(final_joined_df))
final_joined_df.sample(n = 3)

#%% [markdown]
#### Parse dates
#
#The following code parses the date information present in the date_begin or date_end string and converts it to the correct property in FactGrid and it's corresponding value.
#There are also testcases which are run in case you want to modify it.
#Here is an overview of relevant FactGrid properties: [link](https://database.factgrid.de/query/embed.html#SELECT%20%3FPropertyLabel%20%3FProperty%20%3FPropertyDescription%20%3Freciprocal%20%3FreciprocalLabel%20%3Fexample%20%3Fuseful_statements%20%3Fwd%20WHERE%20%7B%0A%20%20SERVICE%20wikibase%3Alabel%20%7B%20bd%3AserviceParam%20wikibase%3Alanguage%20%22en%22.%20%7D%0A%20%20%3FProperty%20wdt%3AP8%20wd%3AQ77483.%0A%20%20OPTIONAL%20%7B%20%3FProperty%20wdt%3AP364%20%3Fexample.%20%7D%0A%20%20OPTIONAL%20%7B%20%3FProperty%20wdt%3AP86%20%3Freciprocal.%20%7D%0A%20%20OPTIONAL%20%7B%20%3FProperty%20wdt%3AP343%20%3Fwd.%20%7D%0A%20%20OPTIONAL%20%7B%20%3FProperty%20wdt%3AP310%20%3Fuseful_statements.%20%7D%0A%7D%0AORDER%20BY%20%3FPropertyLabel)

#%%
#defining an enum to more clearly define what type of date is being passed 
class DateType(Enum):
    ONLY_DATE = 0
    BEGIN_DATE = 1
    END_DATE = 2

#date precision and calendar declaration (see Time at https://www.wikidata.org/wiki/Help:QuickStatements#Add_simple_statement)
PRECISION_CENTURY = 7
PRECISION_DECADE = 8
PRECISION_YEAR = 9
PRECISION_MONTH = 10
PRECISION_DAY = 11
JULIAN_ENDING = '/J'

#defining some constants for better readability of the code:
#self defined:
JHS_GROUP = r'(Jhs\.|Jahrhunderts?)'
JH_GROUP = r'(Jh\.|Jahrhundert)'
EIGTH_OF_A_CENTURY = 13
QUARTER_OF_A_CENTURY = 25
TENTH_OF_A_CENTURY = 10

ANTE_GROUP = "bis|vor|spätestens"
POST_GROUP = "nach|frühestens|ab|zwischen" # NOTE: 'zwischen' does not actually fit into this group, but because the current strategy for 'zwischen 1087 und 1093' is to just take the first date with post quem, it makes sense to have it here
CIRCA_GROUP = r"etwa|ca\.|um"
#pre-compiling the most complex pattern to increase efficiency
MOST_COMPLEX_PATTERN = re.compile(r'(wohl )?((kurz )?(' + ANTE_GROUP + '|' + POST_GROUP + r') )?((' + CIRCA_GROUP +r') )?(\d{3,4})(\?)?')

#FactGrid properties:
#simple date properties:
DATE = 'P106' 
BEGIN_DATE = 'P49'
END_DATE = 'P50'
#when there is uncertainty / when all we know is the latest/earliest possible date:
DATE_AFTER = 'P41' # the earliest possible date for something
DATE_BEFORE = 'P43' # the latest possible date for something
END_TERMINUS_ANTE_QUEM = 'P1123' # latest possible date of the end of a period
BEGIN_TERMINUS_ANTE_QUEM  = 'P1124' # latest possible date of the begin of a period
END_TERMINUS_POST_QUEM = 'P1125' # earliest possible date of the end of a period
BEGIN_TERMINUS_POST_QUEM = 'P1126' # earliest possible date of the beginning of a period

NOTE = 'P73' # Field for free notes
PRECISION_DATE = 'P467' # FactGrid qualifier for the specific determination of the exactness of a date
PRECISION_BEGIN_DATE = 'P785'   # qualifier to specify a begin date
PRECISION_END_DATE = 'P786'
STRING_PRECISION_BEGIN_DATE = 'P787' # qualifier to specify a begin date; string alternate to P785
STRING_PRECISION_END_DATE = 'P788'

#qualifiers/options
SHORTLY_BEFORE = 'Q255211'
SHORTLY_AFTER = 'Q266009'
LIKELY = 'Q23356'
CIRCA = 'Q10'
OR_FOLLOWING_YEAR = 'Q912616'

def format_datetime(entry: datetime, precision: int):
    ret_val =  f"+{entry.isoformat()}Z/{precision}"

    if entry.year < 1582: # declaring that the julian calendar is being used by adding '/J' to the end
        ret_val +=  JULIAN_ENDING
    
    #on FactGrid, if the date is at most accurate to a year, the day and month are set to 0. The datetime type in Python does not allow you to set the day or month to 0 so we need to replace it manually
    if precision <= PRECISION_YEAR:
        ret_val = ret_val.replace(f"{entry.year}-01-01", f"{entry.year}-00-00", 1)
    elif precision == PRECISION_MONTH:
        ret_val = ret_val.replace(f"{entry.year}-{entry.month}-01", f"{entry.year}-{entry.month}-00", 1)

    return ret_val

#only_date=True means there is only one date, not a 'begin date' and an 'end date'
def date_parsing(date_string: str, date_type: DateType):
    qualifier = ""
    precision = PRECISION_CENTURY

    ante_property = (match := re.search(ANTE_GROUP, date_string))
    post_property = (match := re.search(POST_GROUP, date_string))
    assert(not ante_property or not post_property)
    
    match date_type:
        case DateType.ONLY_DATE:
            string_precision_qualifier_clause = NOTE
            exact_precision_qualifier = PRECISION_DATE
            if ante_property:
                return_property = DATE_BEFORE
            elif post_property:
                return_property = DATE_AFTER
            else:
                return_property = DATE
        case DateType.BEGIN_DATE:
            string_precision_qualifier_clause = STRING_PRECISION_BEGIN_DATE
            exact_precision_qualifier = PRECISION_BEGIN_DATE
            if ante_property:
                return_property = BEGIN_TERMINUS_ANTE_QUEM
            elif post_property:
                return_property = BEGIN_TERMINUS_POST_QUEM
            else:
                return_property = BEGIN_DATE
        case DateType.END_DATE:
            string_precision_qualifier_clause = STRING_PRECISION_END_DATE
            exact_precision_qualifier = PRECISION_END_DATE
            if ante_property:
                return_property = END_TERMINUS_ANTE_QUEM
            elif post_property:
                return_property = END_TERMINUS_POST_QUEM
            else:
                return_property = END_DATE
        case _:
            assert False, "Unexpected DateType!"
        
    string_precision_qualifier_clause += f'\t"{date_string}"'

    if date_string == '?':
        return tuple()
    
    # something like: 12. Jahrhundert
    if matches := re.match(r'(\d{1,2})\. ' + JH_GROUP, date_string):
        year = 100 * int(matches.group(1))
    
    # something like: 2. Hälfte des 12. Jahrhunderts
    elif matches := re.match(r'(\d)\. Hälfte (des )?(\d{1,2})\. ' + JHS_GROUP, date_string):
        half = int(matches.group(1)) - 1
        centuries = int(matches.group(3)) - 1
        year   = centuries * 100 + (half * 50) + QUARTER_OF_A_CENTURY
        qualifier = string_precision_qualifier_clause
    
    elif matches := re.match(r'(\w+) Viertel des (\d{1,2})\. ' + JHS_GROUP, date_string):
        number_map = {
            "erstes":  0,
            "zweites": 1,
            "drittes": 2,
            "viertes": 3,
        }
        quarter = matches.group(1)
        centuries = int(matches.group(2))
        year = (centuries - 1) * 100 + (number_map[quarter] * 25) + EIGTH_OF_A_CENTURY
        qualifier = string_precision_qualifier_clause

    elif matches := re.match(r'frühes (\d{1,2})\. ' + JH_GROUP, date_string):
        centuries = int(matches.group(1)) - 1
        year = centuries * 100 + TENTH_OF_A_CENTURY
        qualifier = string_precision_qualifier_clause

    elif matches := re.match(r'spätes (\d{1,2})\. ' + JH_GROUP, date_string):
        centuries = int(matches.group(1))
        year = centuries * 100 - TENTH_OF_A_CENTURY
        qualifier = string_precision_qualifier_clause

    elif matches := re.match(r'(Anfang|Mitte|Ende) (\d{1,2})\. ' + JH_GROUP, date_string):
        number_map = {
            "Anfang":  0,
            "Mitte": 1,
            "Ende": 2,
        }
        third = number_map[matches.group(1)]
        centuries = int(matches.group(2)) - 1
        year = centuries * 100 + (third * 33) + 17
        qualifier = string_precision_qualifier_clause

    elif matches := re.match(r'(\d{3,4})er Jahre', date_string):
        year = int(matches.group(1))
        precision = PRECISION_DECADE
    
    elif matches := re.match(r'Wende zum (\d{1,2})\. ' + JH_GROUP, date_string):
        centuries = int(matches.group(1)) - 1
        year = centuries * 100 - 10
        qualifier = string_precision_qualifier_clause

    elif matches := re.match(r'Anfang der (\d{3,4})er Jahre', date_string):
        year = int(matches.group(1))
        qualifier = string_precision_qualifier_clause
        precision = PRECISION_DECADE

    # something like: (1140) 1145
    elif matches := re.match(r'\((\d{3,4})\s?\?\) (\d{3,4})', date_string):
        year = int(matches.group(2)) # ignoring the year in parantheses
        precision = PRECISION_YEAR
        qualifier = string_precision_qualifier_clause
    
    # something like: zwischen 1087 und 1093
    elif matches := re.match(r'zwischen (\d{3,4}) und (\d{3,4})', date_string):
        year = int(matches.group(1)) # ignoring the second year
        precision = PRECISION_YEAR
        qualifier = string_precision_qualifier_clause

    # something like: 1140/1141
    # or like: 1140/1152
    elif matches := re.match(r'(\d{3,4})/(\d{3,4})', date_string):
        year1 = int(matches.group(1))
        year2 = int(matches.group(2))

        if year2 - year1 == 1:
            # check for consecutive years
            qualifier = exact_precision_qualifier + '\t' + OR_FOLLOWING_YEAR
        else:
            qualifier = string_precision_qualifier_clause
            
        year = year1
        precision = PRECISION_YEAR

    # this pattern is pre-compiled above, because it's rather complex and it's much more efficient to compile it just once, instead of on every function call
    elif matches := MOST_COMPLEX_PATTERN.match(date_string):
        if matches.group(1): # if 'wohl' was found
            qualifier = exact_precision_qualifier + '\t' + LIKELY
        if matches.group(5): # if 'etwa' , 'ca.' or 'um' were found
            if len(qualifier) != 0:
                qualifier += '\t'
            qualifier += exact_precision_qualifier + '\t' + CIRCA
                
        if matches.group(3): # if 'kurz' was found -- because of how the regex is defined, this can only happen when combined with 'nach', 'bis', etc.
            if len(qualifier) != 0:
                qualifier += '\t'

            if ante_property: # already checked above whether it's before or after
                qualifier += exact_precision_qualifier + '\t' + SHORTLY_BEFORE
            else: # post_property
                qualifier += exact_precision_qualifier + '\t' + SHORTLY_AFTER

        if matches.group(8): # if a question mark at the end were found
            # TODO is it correct, that on ? the other matches ('ca.' etc.) are ignored, because it's not exact enough?
            qualifier = string_precision_qualifier_clause
        
        year = int(matches.group(7))
        precision = PRECISION_YEAR

    else:
        raise Exception(f"Couldn't parse date '{date_string}'")

    entry = datetime(year, 1, 1)
    return (return_property, format_datetime(entry, precision), qualifier, date(year, 1, 1).isoformat())
    #return (return_property, format_datetime(entry, precision), qualifier)

#%% [markdown]
##### Test cases
#
#Because there are so many special cases, testing is a must to more clearly show what is expected for each case and make sure no incorrect changes are made.

#%%
#TODO still to be handled:
    # nach 1177/vor 1305 -- maybe correct to "zwischen 1177 und 1305"?
    # "(996)" -- mistake or what does this mean?
    # "12. oder 13. Jahrhundert"
    # "Ende 11. Jahrhundert/1. Viertel 12. Jahrhundert"
    # "(vor 1254) 1256"

begin_date_tests = {
    "1605": (BEGIN_DATE, "+1605-00-00T00:00:00Z/9"),
    "1205": (BEGIN_DATE, "+1205-00-00T00:00:00Z/9/J"),
    "1205?": (BEGIN_DATE, "+1205-00-00T00:00:00Z/9/J", STRING_PRECISION_BEGIN_DATE + '\t"1205?"'),
    "12. Jahrhundert": (BEGIN_DATE, "+1200-00-00T00:00:00Z/7/J"),
    "1. Hälfte des 12. Jhs.": (BEGIN_DATE, "+1125-00-00T00:00:00Z/7/J", STRING_PRECISION_BEGIN_DATE + '\t"1. Hälfte des 12. Jhs."'),
    "1. Hälfte des 12. Jahrhunderts": (BEGIN_DATE, "+1125-00-00T00:00:00Z/7/J", STRING_PRECISION_BEGIN_DATE + '\t"1. Hälfte des 12. Jahrhunderts"'),
    "2. Hälfte des 12. Jhs.": (BEGIN_DATE, "+1175-00-00T00:00:00Z/7/J", STRING_PRECISION_BEGIN_DATE + '\t"2. Hälfte des 12. Jhs."'),
    "erstes Viertel des 12. Jhs.": (BEGIN_DATE, "+1113-00-00T00:00:00Z/7/J", STRING_PRECISION_BEGIN_DATE + '\t"erstes Viertel des 12. Jhs."'),
    "zweites Viertel des 12. Jhs.": (BEGIN_DATE, "+1138-00-00T00:00:00Z/7/J", STRING_PRECISION_BEGIN_DATE + '\t"zweites Viertel des 12. Jhs."'),
    "drittes Viertel des 12. Jhs.": (BEGIN_DATE, "+1163-00-00T00:00:00Z/7/J", STRING_PRECISION_BEGIN_DATE + '\t"drittes Viertel des 12. Jhs."'),
    "viertes Viertel des 12. Jhs.": (BEGIN_DATE, "+1188-00-00T00:00:00Z/7/J", STRING_PRECISION_BEGIN_DATE + '\t"viertes Viertel des 12. Jhs."'),
    "frühes 12. Jh.": (BEGIN_DATE, "+1110-00-00T00:00:00Z/7/J", STRING_PRECISION_BEGIN_DATE + '\t"frühes 12. Jh."'),
    "spätes 12. Jh.": (BEGIN_DATE, "+1190-00-00T00:00:00Z/7/J", STRING_PRECISION_BEGIN_DATE + '\t"spätes 12. Jh."'),
    "Anfang 12. Jh.": (BEGIN_DATE, "+1117-00-00T00:00:00Z/7/J", STRING_PRECISION_BEGIN_DATE + '\t"Anfang 12. Jh."'),
    "Anfang 15. Jahrhundert": (BEGIN_DATE, "+1417-00-00T00:00:00Z/7/J", STRING_PRECISION_BEGIN_DATE + '\t"Anfang 15. Jahrhundert"'),
    "Mitte 12. Jh.": (BEGIN_DATE, "+1150-00-00T00:00:00Z/7/J", STRING_PRECISION_BEGIN_DATE + '\t"Mitte 12. Jh."'),
    "Mitte 14. Jahrhundert?": (BEGIN_DATE, "+1350-00-00T00:00:00Z/7/J", STRING_PRECISION_BEGIN_DATE + '\t"Mitte 14. Jahrhundert?"'),
    "Ende 12. Jh.": (BEGIN_DATE, "+1183-00-00T00:00:00Z/7/J", STRING_PRECISION_BEGIN_DATE + '\t"Ende 12. Jh."'),
    "Ende 12. Jahrhundert": (BEGIN_DATE, "+1183-00-00T00:00:00Z/7/J", STRING_PRECISION_BEGIN_DATE + '\t"Ende 12. Jahrhundert"'),
    "bis etwa 1147": (BEGIN_TERMINUS_ANTE_QUEM, '+1147-00-00T00:00:00Z/9/J', PRECISION_BEGIN_DATE + '\t' + CIRCA),
    "etwa 1147": (BEGIN_DATE, '+1147-00-00T00:00:00Z/9/J', PRECISION_BEGIN_DATE + '\t' + CIRCA),
    "ca. 1050": (BEGIN_DATE, "+1050-00-00T00:00:00Z/9/J", PRECISION_BEGIN_DATE + '\t' + CIRCA),
    "um 1050": (BEGIN_DATE, "+1050-00-00T00:00:00Z/9/J", PRECISION_BEGIN_DATE + '\t' + CIRCA),
    "1230er Jahre": (BEGIN_DATE, "+1230-00-00T00:00:00Z/8/J"),
    "Wende zum 12. Jh.": (BEGIN_DATE, '+1090-00-00T00:00:00Z/7/J', STRING_PRECISION_BEGIN_DATE + '\t"Wende zum 12. Jh."'),
    "Anfang der 1480er Jahre": (BEGIN_DATE, '+1480-00-00T00:00:00Z/8/J', STRING_PRECISION_BEGIN_DATE + '\t"Anfang der 1480er Jahre"'),
    "1164/1165": (BEGIN_DATE, '+1164-00-00T00:00:00Z/9/J', PRECISION_BEGIN_DATE + '\t' + OR_FOLLOWING_YEAR),
    "1164/1177": (BEGIN_DATE, '+1164-00-00T00:00:00Z/9/J', STRING_PRECISION_BEGIN_DATE + '\t"1164/1177"'),
    "(1014?) 1015": (BEGIN_DATE,"+1015-00-00T00:00:00Z/9/J", STRING_PRECISION_BEGIN_DATE + '\t"(1014?) 1015"'),
    "ab 1534": (BEGIN_TERMINUS_POST_QUEM, '+1534-00-00T00:00:00Z/9/J'),
    "nach 1230": (BEGIN_TERMINUS_POST_QUEM, '+1230-00-00T00:00:00Z/9/J'),
    "kurz nach 1200": (BEGIN_TERMINUS_POST_QUEM, '+1200-00-00T00:00:00Z/9/J', PRECISION_BEGIN_DATE + '\t' + SHORTLY_AFTER),
    "frühestens 1342": (BEGIN_TERMINUS_POST_QUEM, '+1342-00-00T00:00:00Z/9/J'),
    "vor 1230": (BEGIN_TERMINUS_ANTE_QUEM, '+1230-00-00T00:00:00Z/9/J'),
    "wohl vor 1249": (BEGIN_TERMINUS_ANTE_QUEM, '+1249-00-00T00:00:00Z/9/J', PRECISION_BEGIN_DATE + '\t' + LIKELY),
    "kurz vor 1200": (BEGIN_TERMINUS_ANTE_QUEM, '+1200-00-00T00:00:00Z/9/J', PRECISION_BEGIN_DATE + '\t' + SHORTLY_BEFORE), 
    "wohl etwa 1249": (BEGIN_DATE, '+1249-00-00T00:00:00Z/9/J', PRECISION_BEGIN_DATE + '\t' + LIKELY + '\t' + PRECISION_BEGIN_DATE + '\t' + CIRCA),
    "spätestens 1277": (BEGIN_TERMINUS_ANTE_QUEM, '+1277-00-00T00:00:00Z/9/J'),
    "zwischen 1087 und 1093": (BEGIN_TERMINUS_POST_QUEM,"+1087-00-00T00:00:00Z/9/J", STRING_PRECISION_BEGIN_DATE + '\t"zwischen 1087 und 1093"'),
}

for key, value in begin_date_tests.items():
    retval = date_parsing(key, DateType.BEGIN_DATE)
    if len(retval[2]) == 0:
        retval = retval[0:2]
    else:
        retval = retval[0:3] # ignore the datetime object
    assert retval == value, f"{key}: Returned {retval} instead of {value}"

end_date_tests = {
    "1205?": (END_DATE, "+1205-00-00T00:00:00Z/9/J", STRING_PRECISION_END_DATE + '\t"1205?"'),
    "12. Jahrhundert": (END_DATE, "+1200-00-00T00:00:00Z/7/J"),
    "drittes Viertel des 12. Jhs.": (END_DATE, "+1163-00-00T00:00:00Z/7/J", STRING_PRECISION_END_DATE + '\t"drittes Viertel des 12. Jhs."'),
    "bis etwa 1147": (END_TERMINUS_ANTE_QUEM, '+1147-00-00T00:00:00Z/9/J', PRECISION_END_DATE + '\t' + CIRCA),
    "um 1050": (END_DATE, "+1050-00-00T00:00:00Z/9/J", PRECISION_END_DATE + '\t' + CIRCA),
    "Anfang der 1480er Jahre": (END_DATE, '+1480-00-00T00:00:00Z/8/J', STRING_PRECISION_END_DATE + '\t"Anfang der 1480er Jahre"'),
    "1164/1165": (END_DATE, '+1164-00-00T00:00:00Z/9/J', PRECISION_END_DATE + '\t' + OR_FOLLOWING_YEAR),
    "1164/1177": (END_DATE, '+1164-00-00T00:00:00Z/9/J', STRING_PRECISION_END_DATE + '\t"1164/1177"'),
    "(1014?) 1015": (END_DATE,"+1015-00-00T00:00:00Z/9/J", STRING_PRECISION_END_DATE + '\t"(1014?) 1015"'),
    "ab 1534": (END_TERMINUS_POST_QUEM, '+1534-00-00T00:00:00Z/9/J'),
    "nach 1230": (END_TERMINUS_POST_QUEM, '+1230-00-00T00:00:00Z/9/J'),
    "frühestens 1342": (END_TERMINUS_POST_QUEM, '+1342-00-00T00:00:00Z/9/J'),
    "vor 1230": (END_TERMINUS_ANTE_QUEM, '+1230-00-00T00:00:00Z/9/J'),
    "wohl vor 1249": (END_TERMINUS_ANTE_QUEM, '+1249-00-00T00:00:00Z/9/J', PRECISION_END_DATE + '\t' + LIKELY),
    "zwischen 1087 und 1093": (END_TERMINUS_POST_QUEM,"+1087-00-00T00:00:00Z/9/J", STRING_PRECISION_END_DATE + '\t"zwischen 1087 und 1093"'),
}

for key, value in end_date_tests.items():
    retval = date_parsing(key, DateType.END_DATE)
    if len(retval[2]) == 0:
        retval = retval[0:2]
    else:
        retval = retval[0:3] # ignore the datetime object
    assert retval == value, f"{key}: Returned {retval} instead of {value}"

only_date_tests = {
    "1205?": (DATE, "+1205-00-00T00:00:00Z/9/J", NOTE + '\t"1205?"'),
    "12. Jahrhundert": (DATE, "+1200-00-00T00:00:00Z/7/J"),
    "drittes Viertel des 12. Jhs.": (DATE, "+1163-00-00T00:00:00Z/7/J", NOTE + '\t"drittes Viertel des 12. Jhs."'),
    "bis etwa 1147": (DATE_BEFORE, '+1147-00-00T00:00:00Z/9/J', PRECISION_DATE + '\t' + CIRCA),
    "um 1050": (DATE, "+1050-00-00T00:00:00Z/9/J", PRECISION_DATE + '\t' + CIRCA),
    "Anfang der 1480er Jahre": (DATE, '+1480-00-00T00:00:00Z/8/J', NOTE + '\t"Anfang der 1480er Jahre"'),
    "1164/1165": (DATE, '+1164-00-00T00:00:00Z/9/J', PRECISION_DATE + '\t' + OR_FOLLOWING_YEAR),
    "1164/1177": (DATE, '+1164-00-00T00:00:00Z/9/J', NOTE + '\t"1164/1177"'),
    "(1014?) 1015": (DATE,"+1015-00-00T00:00:00Z/9/J", NOTE + '\t"(1014?) 1015"'),
    "ab 1534": (DATE_AFTER, '+1534-00-00T00:00:00Z/9/J'),
    "nach 1230": (DATE_AFTER, '+1230-00-00T00:00:00Z/9/J'),
    "frühestens 1342": (DATE_AFTER, '+1342-00-00T00:00:00Z/9/J'),
    "vor 1230": (DATE_BEFORE, '+1230-00-00T00:00:00Z/9/J'),
    "wohl vor 1249": (DATE_BEFORE, '+1249-00-00T00:00:00Z/9/J', PRECISION_DATE + '\t' + LIKELY),
    "zwischen 1087 und 1093": (DATE_AFTER,"+1087-00-00T00:00:00Z/9/J", NOTE + '\t"zwischen 1087 und 1093"'),
}

for key, value in only_date_tests.items():
    retval = date_parsing(key, DateType.ONLY_DATE)
    if len(retval[2]) == 0:
        retval = retval[0:2]
    else:
        retval = retval[0:3] # ignore the datetime object
    assert retval == value, f"{key}: Returned {retval} instead of {value}"

#%% [markdown]
#How a date is parsed depends on whether it's the only date or not (begin and end date), so the below function handles this.

#%%
def parse_both_dates(d: dict):
    try:
        date_begin = d["date_begin"]
        date_end = d["date_end"]

        if date_begin != None:
            if date_end != None:
                begin = date_parsing(date_begin, DateType.BEGIN_DATE)
                end = date_parsing(date_end, DateType.END_DATE)
            
                date_clauses = {"begin": begin, "end" : end}
            else:
                date_clauses = {"begin": date_parsing(date_begin, DateType.ONLY_DATE), "end" : None}
        else:
            if date_end != None:
                date_clauses = {"begin": None, "end" : date_parsing(date_end, DateType.ONLY_DATE)}
            else:
                # do nothing, since nothing needs to be parsed
                date_clauses = {"begin": None, "end" : None}

        return date_clauses
    except Exception as e:
        print(traceback.format_exc())
        print(row)
        print('\n')
        return {"begin": None, "end" : None}

#%% [markdown]
#### Generate missing offices file
#
#The code below creates the office entries to be uploaded on factgrid.
#
#If the date parsing function can't handle a date (either because that format hasn't been encountered yet or because the entry is nonsense), it prints the problematic date and the corresponding entry from the dataframe. If the relevant rows contain some nonsense data, use this output to find and fix it. If the data is not nonsense, most likely the date_parsing function above needs to be extended. For this, you probably want to contact whoever is responsible for maintaining the sync_notebooks.

#%%
filepath = os.path.join(output_path, f'quickstatements-offices_{today_string}.qs')

with open(filepath, 'w') as file:
    for row in final_joined_df.iter_rows(named = True):
        try:
            date_clauses = ()

            if row['date_begin'] != None:
                if row['date_end'] != None:
                    date_clauses = (*date_parsing(row['date_begin'], DateType.BEGIN_DATE), *date_parsing(row['date_end'], DateType.END_DATE))
                else:
                    date_clauses = date_parsing(row['date_begin'], DateType.ONLY_DATE)
            else:
                if row['date_end'] != None:
                    date_clauses = date_parsing(row['date_end'], DateType.ONLY_DATE)
                    
            file.write('\t'.join([
                row['FactGrid'], 
                'P165', 
                row['fg_inst_role_id'],
                'S601', 
                '"' + row['person_id'] + '"',
                *date_clauses,
            ]) + '\n')
        except Exception as e:
            print(traceback.format_exc())
            print(row)
            print('\n')

#%% [markdown]
### 9. Updating FactGrid
#Once the files have been generated, please open [QuickStatements](https://database.factgrid.de/quickstatements/#/batch) and **run the CSV-commands/V1-commands** (the qs-file contains V1 commands, the csv-files CSV-commands). More details to perform this can be found [here](https://github.com/WIAG-ADW-GOE/sync_notebooks/blob/main/docs/Run_factgrid_csv.md).
#### Next notebook
#Once the update is done, you can continue with [notebook 5](fg_to_dpr.ipynb) (fg_to_dpr).