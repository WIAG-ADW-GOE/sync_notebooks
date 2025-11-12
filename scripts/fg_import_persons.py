# %% [markdown]
# # 3. Import persons from WIAG into FactGrid (FG)
# 
# This notebook creates new records in FactGrid by generating V1 statements compatible with QuickStatements, a tool for batch editing FactGrid entries. It identifies new entries (persons that are in WIAG but not on FG), assigns appropriate role descriptions, and formats multilingual labels and descriptions for each entry. It ensures consistency with FactGrid conventions, including handling roles, group assignments, and standardized property mappings.

# %% [markdown]
# ## Import data

# %% [markdown]
# ### Download data from WIAG
# 
# It's recommended to limit the export to one Domstift by first searching for that Domstift before exporting the 'CSV Personendaten' and 'CSV Amtsdaten' to make sure that the amount of objects to be added is manageable.
# 
# 1. go to https://wiag-vokabulare.uni-goettingen.de/query/can
# 2. filter by cathedral chapter (Domstift)
# 3. click Export->Personendaten
# 4. click Export->Amtsdaten
# 
# If you filtered by Domstift (cathedral chapter), **change the variable below** to the domstift you used and **change the name of the exported file** to include the name of the cathedral chapter.
# 
# If you did not filter, you need to change the line to `domstift = ""`.
# 
# You can also rename the file (e.g. to include the date on which it was created). In that case you also need to **change the `filename`** below.

# %%
import polars as pl
import os
from datetime import datetime

domstift = "Osnabrück" # with domstift = "Mainz" the name of the file should be "WIAG-Domherren-DB-Ämter-Mainz.csv"
input_file_l = f"WIAG-Domherren-DB-Lebensdaten-{domstift}.csv"
input_file_a = f"WIAG-Domherren-DB-Ämter-{domstift}.csv"

#domstift = "" # in case you did not filter by Domstift, use this instead
#input_file_l = "WIAG-Domherren-DB-Lebensdaten.csv"
#input_file_a = "WIAG-Domherren-DB-Ämter.csv"

# %% [markdown]
# ### Import the files

# %% [markdown]
# Please **move the downloaded file** to the `input_path` directory defined below or **change the `input_path`** to where the file is located.

# %%
input_path = r"C:\Users\Public\sync_notebooks\input_files"

# %% [markdown]
# load person data

# %%
df_person_all = pl.read_csv(os.path.join(input_path, input_file_l), separator=';', infer_schema_length=None)
print(f"{len(df_person_all)} persons loaded in total.")
# filter for persons that are not yet in FG
df_person_in = df_person_all.filter(pl.col("FactGrid_ID").is_null()).rename({'id':'person_id'})
print(f"Out of these, {len(df_person_in)} persons are not yet in FactGrid")

# %% [markdown]
# load offices

# %%
df_offices = pl.read_csv(os.path.join(input_path, input_file_a), separator=';', infer_schema_length=None)
print(len(df_offices))

# %% [markdown]
# ## Create descriptions for each new person entry
# 
# this function defines how to piece together the description of an office

# %%
def describe_office(role_row):
    inst_or_dioc = role_row.get('institution') if role_row.get('institution') is not None else role_row.get('diocese')

    date_begin = role_row.get('date_begin')
    date_end = role_row.get('date_end')

    date_info = ""
    if date_begin is not None and date_end is not None:
        date_info = f"{date_begin}-{date_end}"
    elif date_begin is not None:
        date_info = str(date_begin)
    elif date_end is not None:
        date_info = f"bis {date_end}"
    
    description = role_row.get('name', '')
    if inst_or_dioc is not None:
        description += f" {inst_or_dioc}"
    if date_info:
        description += f" {date_info}"
    
    return description

# %% [markdown]
# this cell creates a ranking of the most important role (groups) to be added to the description (only up to two offices are added to the description) and then prints how many offices fall into each category 

# %%
role_group_rank_map = {
    "Q648236" : 0, # Leiter (Erz-)diözese (Altes Reich)
    "Q648232" : 1, # Domdignitär Altes Reich            
    "Q648226" : 2, # Domkleriker Altes Reich
    "Q648233" : 3, # Klosterangehöriger mit Leitungsamt
}

for rg in role_group_rank_map:
    count = df_offices.filter(pl.col('role_group_fq_id') == rg).height
    print(f"{rg}: {count}")

# %% [markdown]
# This cell actually creates the descriptions. For this the two most important offices are chosen, first by ranking by role group and then choosing the most recent office.

# %%
grp_descriptions = []
grp_descriptions_en = []

for pid, grp in df_offices.with_columns(
        pl.col('role_group_fq_id').replace_strict(role_group_rank_map, default=len(role_group_rank_map)).alias('rank')
    ).group_by('person_id'):
    N_ROLE_4_DESCRIPTION = 2

    df_sorted = grp.sort(['rank', 'date_sort_key'], descending=[False, True])

    # generating German descriptions
    description_list = []
    for row in df_sorted.iter_rows(named=True):
        desc = describe_office(row)
        if desc not in description_list:
            description_list.append(desc)

    # choosing the two descriptions with the highest rank
    descriptions = ", ".join(description_list[:N_ROLE_4_DESCRIPTION])    
    grp_descriptions.append({"person_id": pid[0], "summary_roles": descriptions})

    # getting the English title of the group with highest rank to use as the description
    if df_sorted.height > 0:
        grp_descriptions_en.append({"person_id": pid[0], "best_role_group_en": df_sorted.item(0, 'role_group_en')})
    else:
        grp_descriptions_en.append({"person_id": pid[0], "best_role_group_en": "missing"})

grp_descriptions_df = pl.DataFrame(grp_descriptions)
df_person = df_person_in.join(grp_descriptions_df, on="person_id")
df_person = df_person.join(pl.DataFrame(grp_descriptions_en), on="person_id")

print("Here is a sample of the descriptions created:")
grp_descriptions_df.sample(n=3)

# %% [markdown]
# ## Add role groups
# More detailed office information is added in the next notebook.

# %% [markdown]
# select relevant groups:
# 
# since 2023-12-19 and for now, all groups are relevant

# %%
relevant_role_group_fq_id = [
    "Q254893",
    "Q385344",
    "Q648226",
    "Q648227",
    "Q648228",
    "Q648229",
    "Q648230",
    "Q648232",
    "Q648233",
    "Q648234",
    "Q648235",
    "Q648236",
    "Q648239",
]

# %%
df_offices = df_offices.filter(pl.col('role_group_fq_id').is_in(relevant_role_group_fq_id))
print(len(df_offices))

# %% [markdown]
# Das deutsche Beschreibungsfeld soll Lebensdaten mit der Zusammenfassung der Amtsdaten enthalten.
# 
# Das englische Beschreibungsfeld soll Lebensdaten mit der am höchsten priorisierten Ämtergruppe enthalten. Falls der Name der Gruppe nicht übersetzt ist oder falls es keine Ämter gibt, wird für die Beschreibung 'missing' ausgegeben.

# %%
df_person = df_person.with_columns(
    description_de = pl.concat_str([pl.col('biographical_dates'), pl.col('summary_roles')], separator=', '),
    description_en = pl.concat_str([pl.col('biographical_dates'), pl.col('best_role_group_en')], separator=', ')
)

# %% [markdown]
# Benenne die Spalten um entsprechend den Konventionen des FactGrid.

# %%
df_person = df_person.rename({
    'displayname': 'Lde',
    'description_de': 'Dde',
    'description_en': 'Den',
    'date_of_birth': 'P77',
    'date_of_death': 'P38',
    'GND_ID': 'P76',
    'GSN': 'P472',
    'Wikidata_ID': 'Swikidatawiki',
    'Wikipedia': 'Sdewiki'
}).with_columns(P601=pl.col("person_id")) # this rename is done separately to have the unmodified id later

# %% [markdown]
# Kopiere das Label in Deutsch für die anderen Sprachen und füge Daten ein, die für alle Personen gleich sind: Mensch, Teil der Germania Sacra Forschungsdaten, männlich

# %%
df_person = df_person.with_columns(
    Len = pl.col('Lde'),
    Lfr = pl.col('Lde'),
    Les = pl.col('Lde'),
    P2 = pl.lit("Q7"),
    P131 = pl.lit("Q153178"),
    P154 = pl.lit("Q18")
)

# %% [markdown]
# surround some properties with quotation marks for FactGrid

# %%
df_person = df_person.with_columns(
    pl.format('"{}"', pl.col("P601")).alias("P601"),
    pl.format('"{}"', pl.col("P76")).alias("P76"),
    pl.format('"{}"', pl.col("P472")).alias("P472"),
    pl.format('"{}"', pl.col("Swikidatawiki")).alias("Swikidatawiki"),
    pl.format('"{}"', pl.col("Sdewiki")).alias("Sdewiki"),
    pl.format('"{}"', pl.col("Lde")).alias("Lde"),
    pl.format('"{}"', pl.col("Len")).alias("Len"),
    pl.format('"{}"', pl.col("Lfr")).alias("Lfr"),
    pl.format('"{}"', pl.col("Les")).alias("Les"),
    pl.format('"{}"', pl.col("Dde")).alias("Dde"),
    pl.format('"{}"', pl.col("Den")).alias("Den"),   
)

# %% [markdown]
# ## Update FactGrid
# ### Generate file with V1-instructions
# 
# Gib ausgewählte Elemente aus `df_person` aus. Falls es schon eine Datei mit gleichem Namen im angegebenen Verzeichnis gibt, wird die Datei überschrieben.

# %%
output_path = r"C:\Users\Public\sync_notebooks\output_files"

# %%
today_string = datetime.now().strftime('%Y-%m-%d')

if domstift == "":
    output_file = f"create_persons_FG_{today_string}.v1"
else:
    output_file = f"create_persons_FG_{today_string}-{domstift}.v1"
output_path_file = os.path.join(output_path, output_file)

# %%
with open(output_path_file, "w", encoding='utf-8') as out_stream:
    for row in df_person.iter_rows(named=True):
        out_stream.write("CREATE\n")
        for col in ['Lde', 'Len', 'Lfr', 'Les', 'Dde', 'Den']:
            if row.get(col) is not None:
                out_stream.write(f"LAST\t{col}\t{row[col]}\n")
        
        for col in ['P2', 'P131', 'P154', 'P601', 'P76', 'P472', 'Swikidatawiki', 'Sdewiki']:
            if row.get(col) is not None:
                out_stream.write(f"LAST\t{col}\t{row[col]}\n")
        
        fq_id_list = df_offices.filter(pl.col('person_id') == row['person_id']).get_column('role_group_fq_id').unique()
        for fq_id in fq_id_list:
            out_stream.write(f"LAST\t{'P165'}\t{fq_id}\n")

# %% [markdown]
# ### Upload to FactGrid
# Once the file has been generated, please open [QuickStatements](https://database.factgrid.de/quickstatements/#/batch) and **run the V1-commands**. More details to perform this can be found [here](https://github.com/WIAG-ADW-GOE/sync_notebooks/blob/main/docs/Run_factgrid_csv.md).
# ### Next notebook
# Once the update is done, you can continue with [notebook 4](wiag_to_factgrid.ipynb) (wiag_to_factgrid).