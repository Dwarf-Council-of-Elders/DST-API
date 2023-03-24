import json
import pprint
from io import StringIO

import numpy as np
import pandas as pd
import requests


def _get_metadata():
    """
    Henter metadata for tabellerne, som udbydes via Danmarks Statistiks 
    API. Funktionen henter den rå JSON-data og transformerer data til
    3 DataFrames, der gør metadata let at analysere. 

    De 3 DataFrames der returneres er: 

    1.  "metadata" som indeholder de 3 niveauer, som tabeller 
        kategoriseres efter.
    2.  "tables" som indeholder alle tables, som kan hentes
    3.  "variables" som indeholder informationer om hvilke variable, som
        de enkelte tables indeholder. 

    Funktionen anvendes af klassen "Metadata", som fungerer som wrapper.
    """
    url = "https://api.statbank.dk/v1/subjects"
    payload = {
       "recursive": True,
       "includeTables": True,
       "format": "JSON"
    }

    subs = requests.post(url, payload).json()

    def transformer(s):
        """
        Funktionen tager en Pandas-serie, der indeholder JSON.
        Funktionen transformerer JSON til en DataFrame og sikrer sig
        derefter, at alle kolloner er strings.

        Er nødvendig fordi JSON-strukturen, som vi transformerer til 
        en DataFrame indeholder et "0", som er i typen int.
        """
        s = s.subjects.apply(pd.Series)
        s.columns = [str(x) for x in s.columns]
        return s


    df = (
        pd.DataFrame(subs)
        .drop(
            columns=[
                "tables",
                "hasSubjects"
            ]
        )
        .explode("subjects")
        .rename(
            columns={
                "id": "lvl1_id",
                "description": "lvl1_desc",
                "active": "lvl1_active"
            }
        )
        .pipe(
            lambda df:
                df.assign(**df.subjects.apply(pd.Series))
        )
        .drop(
            columns=[ 
                "hasSubjects",
                "tables"
            ]
        )
        .rename(
            columns={
                "id": "lvl2_id",
                "description": "lvl2_desc",
                "active": "lvl2_active"
            }
        )
        .explode("subjects")
        .pipe(
            lambda df:
                df.assign(**df.pipe(transformer))
        )
        .drop(
            columns=[ 
                "hasSubjects",
                "0",
                "subjects"
            ]
        )
        .explode("tables")
        .query("tables.notna()")
        .rename(
            columns={
                "id": "lvl3_id",
                "description": "lvl3_desc",
                "active": "lvl3_active"
            }
        )
        .pipe(
            lambda df:
                df.assign(**df.tables.apply(pd.Series))
        )
        .drop(
            columns=[
                "tables"
            ]
        )
        .rename(
            columns={
                "id": "tableid",
                "text": "tablename"
            }
        )
    )

    metadata_cols = [
        'lvl1_id',      'lvl1_desc',    'lvl1_active', 
        'lvl2_id',      'lvl2_desc',    'lvl2_active',  
        'lvl3_id',      'lvl3_desc',    'lvl3_active', 
        'tableid'
    ] 

    tables_cols = [
        'tableid',	    'lvl3_desc',    'firstPeriod',  
        'latestPeriod',	'updated',	    'active',	  
        'variables'
    ]

    variables_cols = [
        "tableid",      "lvl3_desc",    "firstPeriod", 
        "latestPeriod", "updated",      "active", 
        "variables"
    ]

    metadata = df[metadata_cols]
    tables = df[tables_cols]
    variables = df[variables_cols].explode("variables")

    tables.set_index("tableid", inplace=True)
    
    return metadata, tables, variables

class Metadata:
    """
    Klassen fungerer som wrapper for funktionen "_get_metadata".
    Brug en af 3 følgende kommandoer:

    1.  self.metadata (returnerer udvidede metadata for tables)
    2.  self.tables (returnerer tables, som kan kaldes)
    3.  self.variables (returnerer de variable, som tables indeholder)
    """
    def __init__(self):
        a, b, c = _get_metadata()
        self.kategorier = a
        self.tables = b
        self.variables = c
    
    def __repr__(self): return self.__doc__

class Variable:
    def __init__(self, id_, vals, eliminate):
        self.id_ = id_
        self.rå_vals = vals
        self.eliminate = eliminate
        self.create_vals()
        self.chosen = []
        self.repr_dict = {}
        self._antal_var = len(self.rå_vals)
        self.update_repr()

    def update_repr(self):
        self._valgte = len(self.chosen)

    def create_vals(self):
        df = (
            pd.DataFrame(self.rå_vals)
            .rename(columns={"id": "id_var"})
        )
        df = df.infer_objects()
        self.vals = df

    def set_vals(self, df):
        self.chosen = df.id_var.to_list()
        self.update_repr()

    def create_query(self):
        return {
            "code": self.id_,
            "values": self.chosen
        }

    def __repr__(self):
        eliminate = " PERMANENT" if self.eliminate == False else ""
        return f"<Var: {self.id_}, valgt: {self._valgte}, i alt: {self._antal_var}{eliminate}>"

class DataSelector:
    def __init__(self, tablename, get_metadata=False):
        url = "https://api.statbank.dk/v1/tableinfo"
        self.tablename = tablename
        payload = {
           "table": self.tablename,
           "format": "JSON"
        }
        self.rå_metadata = requests.post(url, payload).json()
        self.make_tables()
        if get_metadata:
            self.setup_metadata()

    def get_col_number(self):
        array1 = np.array(
            [len(x.chosen) for x in self.vars.values()]
        )
        array2 = np.array(
            [(x.eliminate == False) for x in self.vars.values()]
        )
        return sum(array1 + array2 > 0) + 1

    def get_number_of_combinations(self):
        """
        OBS: np.prod([]) = 1 (hvilket er den ønskede funktionalitet)
        """
        chosen = [len(x.chosen) for x in self.vars.values()]
        chosen_clean = [x for x in chosen if x != 0]
        return np.prod(chosen_clean)

    def estimated_data_amount(self):
        return self.get_number_of_combinations() * self.get_col_number()

    
    def make_tables(self):
        res = {}
        for var in self.rå_metadata["variables"]:
            id_  = var["id"]
            vals = var["values"]
            eliminate = var["elimination"]
            res[id_] = Variable(id_, vals, eliminate)
        self.vars = res
        
    def create_query(self):
        vars = [x.create_query() for x in self.vars.values()]
        vars = [x for x in vars if len(x["values"]) != 0]
        payload = {
           "table": self.tablename,
           "format": "CSV",
            "variables": vars
        }
        return json.dumps(payload)
    
    def get_data(self):
        url = "https://api.statbank.dk/v1/data"
        headers = {
            'Content-Type': 'application/json',
            'Accept-Encoding': 'gzip, deflate, br'
        }
        resp = requests.post(url, self.create_query(), headers=headers)
        df = pd.read_csv(
            StringIO(
                resp.text
            ),
            sep=";"
        )
        return df
    
    def __repr__(self):
        pprint.pprint(self.vars)
        return ""
    
    def __getitem__(self, key):
        return self.vars[key]
    
    def __setitem__(self, key, new_val):
        if new_val is True:
            self.vars[key].set_vals(self.vars[key].vals)
        else:
            self.vars[key].set_vals(new_val)

if __name__ == '__main__':
    pass
