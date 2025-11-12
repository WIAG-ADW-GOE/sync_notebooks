# Synchronizing Data between Digitales Personenregister (DPr), WIAG, and FactGrid using Jupyter Notebooks

The Jupyter Notebooks contained in this repository are designed to synchronize data between three databases:

- **[Digitales Personenregister (DPr)](https://personendatenbank.germania-sacra.de/)**: The primary source of data containing persons from books produced by Germania Sacra.
- **[WIAG](https://wiag-vocab.adw-goe.de/)**: A local MySQL database with an API.
- **[FactGrid (FG)](https://database.factgrid.de/wiki/Main_Page)**: A Wikidata-based site for historians.

This guide is intended for team members at Germania Sacra who are domain experts in historical texts but may be missing technical expertise.

## Requirements

To use the notebooks, Python and some Python packages are required. Refer to the [installation guide](https://github.com/WIAG-ADW-GOE/sync_notebooks/blob/main/docs/Installation.md) for further details. Also required to run the notebooks is **database access** to all three databases. To obtain the necessary credentials please contact Barbara Kroeger at Germania Sacra.

## General notes
Ignoring instructions could lead to data inconsistencies, which might be difficult to fix. Make sure to follow instructions in the notebooks (like checking outputs manually) to avoid this. Also for the same reason don't rush through notebooks. Unless you know the notebook extremely well, always execute cells one at a time to monitor progress and catch any errors early.

Also important is to always use up-to-date data. Don't use old exports, because this might lead to data inconsistencies (e.g. if a person was deleted but the export you use still contains that entry).

## Workflow Overview

The synchronization process involves several steps, each performed using a specific Jupyter notebook or action. The steps are designed to be executed in a specific sequence to maintain data consistency. The diagram below displays which steps interact with which systems. Below the diagram is also a list of the steps with some more notes.

![A diagram showing the workflow.](docs/images/sync_notebooks.svg)

### Order of steps

0. **Import DPr-entries into WIAG** (non-notebook action explained [below](#import-dpr-entries-into-wiag-non-notebook-action))
1. **Update WIAG-IDss in Digitales Personenregister (DPr)** (`dpr_recon.ipynb`)
2. **Update WIAG-IDs in FactGrid and then add FG-IDs in WIAG** (`fg_wiag_ids.ipynb`)
3. **Create New Entries on FactGrid from WIAG** (`Csv2FactGrid-create.ipynb`)
4. **Add Roles, Institutions, Institution Roles and Offices on FactGrid** (`wiag_to_factgrid.ipynb`)
5. **Add/Update FG-IDs in DPr** (`fg_to_dpr.ipynb`)
6. **Add/Update GSNs in FactGrid** (`dpr_to_fg.ipynb`)
[link](https://github.com/WIAG-ADW-GOE/sync_notebooks/blob/main/wiag_to_factgrid.ipynb#parse-begin-and-end-date-from-the-wiag-data)

Note:
- The notebooks contain all necessary information for executing them (step 0 is explained belowStep 0 is a non-notebook action and is explained [below](#import-dpr-entries-into-wiag-non-notebook-action). The notebooks themselves contain all necessary information for executing them.).
- Notebook 4 can be skipped. It's very long and you might not be interested in creating the offices for all persons along with all the other FactGrid-entries. Also, the results of this step do not affect any other steps down the line.

### Import DPr-entries into WIAG (non-notebook action)

One step of the workflow that is not part of the notebooks, because it was developed as part of WIAG, is the import of DPr-entries into WIAG. This needs to be taken care of **before getting started on the notebooks**.

1. Log in to WIAG using your credentials.
2. Navigate to **Edit > Domherren aus dem Digitalen Personenregister**.
3. Scroll to the bottom of the page and click the **Start** button to import entries from DPr.
4. Newly imported entries will receive a higher WIAG ID, distinguishing them from native WIAG entries.

5. These entries require manual review. After verification, they will be assigned a lower WIAG ID.
