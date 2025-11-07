import requests
import polars as pl
import polars.selectors as cs

FG_SPARQL_URL = 'https://database.factgrid.de/sparql'

#extract out factgrid id
def extract_id(df, column):
    return df.with_columns(pl.col(column).str.strip_chars('https://database.factgrid.de/entity/'))

#drop irrelevant columns
def drop_type_columns(df):
    df = df.drop(
        cs.ends_with("type"),
        cs.ends_with("xml:lang")
        )
    return df


def load_fg_data():
    # load institutions (items with a Klosterdatenbank-ID)
    query = (
        """SELECT ?item ?gsn WHERE {
    ?item wdt:P471 ?gsn
    }
    """
    )

    r = requests.get(FG_SPARQL_URL, params={'query': query}, headers={"Accept": "application/json"})
    data = r.json()
    institution_df = pl.json_normalize(data['results']['bindings'])
    institution_df = institution_df.cast({'gsn.value':pl.UInt32})

    # load dioceses (items that are an instance or subclass of a diocese)
    query = (
    """
    SELECT DISTINCT ?item ?wiagid ?label ?alternative WHERE {
    ?item wdt:P2/wdt:P3* wd:Q164535.
    #?item schema:description ?itemDesc.
    ?item rdfs:label ?label.
    OPTIONAL {?item schema:description ?itemDesc.}
    OPTIONAL {?item skos:altLabel ?alternative. }
    OPTIONAL {?item wdt:P601 ?wiagid.}
    FILTER(LANG(?label) in ("en", "de"))
    }
    """
    )

    r = requests.get(FG_SPARQL_URL, params={'query': query}, headers={"Accept": "application/json"})
    data = r.json()
    diocese_df = pl.json_normalize(data['results']['bindings'])

    # load institution roles (any item that is a "Career statement that captures a sequence of incumbents")
    query = (
    """
    SELECT ?item ?label WHERE {
    ?item wdt:P2 wd:Q257052.
    ?item rdfs:label ?label.
    FILTER(LANG(?label) in ("de"))
    }
    """
    )

    r = requests.get(FG_SPARQL_URL, params={'query': query}, headers={"Accept": "application/json"})
    data = r.json()
    inst_role_df = pl.json_normalize(data['results']['bindings'])

    # clean data
    institution_df = extract_id(institution_df, 'item.value')
    diocese_df = extract_id(diocese_df, 'item.value')
    inst_role_df = extract_id(inst_role_df, 'item.value')

    institution_df = drop_type_columns(institution_df)
    diocese_df = drop_type_columns(diocese_df)
    inst_role_df = drop_type_columns(inst_role_df)

    #rename columns
    institution_df.columns = ['fg_institution_id', 'fg_gsn_id']
    diocese_df.columns = ["fg_diocese_id", "dioc_label", "dioc_alt", "dioc_wiag_id"]
    inst_role_df.columns = ["fg_inst_role_id", "inst_role"]

    #clean the diocese alts by removing BITECA and BETA entries 
    diocese_df = diocese_df.with_columns(pl.col('dioc_alt').str.replace('^(BITECA|BETA).*', ''))

    print(f"{institution_df.height} institutions, {diocese_df.height} dioceses and {inst_role_df.height} institution roles were loaded from FactGrid.")

    return (institution_df, diocese_df, inst_role_df)