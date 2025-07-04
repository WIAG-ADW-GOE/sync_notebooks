# Synchronizing Data Between Digitales Personenregister (DPr), WIAG, and FactGrid Using Jupyter Notebooks

The Jupyter Notebooks contained in this repository are designed to synchronize data between three databases:

- **Digitales Personenregister (DPr)**: The primary source of data containing persons from books produced by Germania Sacra.
- **WIAG**: A local MySQL database with an API.
- **FactGrid (FG)**: A Wikidata-based site for historians.

This guide is intended for team members at Germania Sacra who are domain experts in historical texts but may be missing technical expertise.

## Requirements

To use the notebooks, first some software (Python and Julia) needs to be installed. For this, refer to the [Software Installation](https://github.com/WIAG-ADW-GOE/sync_notebooks/blob/main/docs/Installation.md) guide. Also required is **database access** to all three databases. To obtain the necessary credentials please contact Barbara Kroeger at Germania Sacra.

## General notes
Ignoring instructions could lead to data inconsistencies, which might be difficult to fix. Make sure to follow instructions in the notebooks (like checking outputs manually) to avoid this. Also for the same reason don't rush through notebooks. Unless you know the notebook extremely well, always execute cells one at a time to monitor progress and catch any errors early.

## Workflow Overview

The synchronization process involves several steps, each performed using a specific Jupyter notebook or action. The steps are designed to be executed in a specific sequence to maintain data consistency.

### Order of steps

0. **Import DPr-entries into WIAG** (non-notebook action explained [below](#import-dpr-entries-into-wiag-non-notebook-action))
1. **Update WIAG IDs in Digitales Personenregister (DPr)** (`dpr_recon.ipynb`)
2. **Update WIAG-IDs in FactGrid and then add FG-IDs in WIAG** (`fg_wiag_ids.ipynb`)
3. **Create New Entries on FactGrid from WIAG** (`Csv2FactGrid-create.ipynb`)
4. **Add Offices to Persons on FactGrid** (`wiag_to_factgrid.ipynb`)
5. **Update DPr Entries with FactGrid Links** (`fg_to_dpr.ipynb`)
6. **Update FactGrid Entries with DPr Links** (`dpr_to_fg.ipynb`)
[link](https://github.com/WIAG-ADW-GOE/sync_notebooks/blob/main/wiag_to_factgrid.ipynb#parse-begin-and-end-date-from-the-wiag-data)

Note:
- The notebooks contain all necessary information for executing them (step 0 is explained belowStep 0 is a non-notebook action and is explained [below](#import-dpr-entries-into-wiag-non-notebook-action). The notebooks themselves contain all necessary information for executing them.).
- Step 6 can be skipped. This step is very long and you might not be interested in creating the offices for all persons along with all the other FactGrid-entries. Also, the results of this step do not affect any other steps down the line.

### Import DPr-entries into WIAG (non-notebook action)

One step of the workflow that is not part of the notebooks, because it was developed as part of WIAG, is the import of DPr-entries into WIAG. This needs to be taken care of **before getting started on the notebooks**.

1. Log in to WIAG using your credentials.
2. Navigate to **Edit > Domherren aus dem Digitalen Personenregister**.
3. Scroll to the bottom of the page and click the **Start** button to import entries from DPr.
4. Newly imported entries will receive a higher WIAG ID, distinguishing them from native WIAG entries.
5. These entries require manual review. After verification, they will be assigned a lower WIAG ID.