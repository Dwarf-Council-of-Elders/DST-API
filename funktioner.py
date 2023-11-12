
import json
import pprint
from io import StringIO

import numpy as np
import pandas as pd
import requests
from typing import Union

import time

def _get_metadata():
    """
    Henter metadata for tabellerne, som udbydes via Danmarks Statistiks 
    API. Funktionen henter den rå JSON-data og transformerer data til
    3 DataFrames, der gør metadata let at analysere. 

    De 3 DataFrames der returneres er: 

    1.  "metadata" som indeholder de 3 niveauer, som tabeller kategoriseres efter.
    2.  "tables" som indeholder alle tables, som kan hentes
    3.  "variables" som indeholder informationer om hvilke variable, som de enkelte tables indeholder. 

    Funktionen anvendes af klassen "Metadata", som fungerer som wrapper.
    """
    url = "https://api.statbank.dk/v1/subjects"
    payload = {
       "recursive": True,
       "includeTables": True,
       "format": "JSON"
    }

    subs = requests.post(url, payload).json()

    def transformer(s: pd.Series) -> pd.Series:
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

    1.  self.kategorier (returnerer udvidede metadata for tables)
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
    """
    Klassen er overordnet set bare en repr-klasse, der tillader printning af variablene, så de er lette at arbejde med.
    Klassen kan overskrives ved at bruge `self.set_vals()`, som hovesageligt bør anvendes fra `DataSelector`-klassen.
    """
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

    def set_vals(self, chosen: Union[pd.DataFrame, list, str, int, bool]):
        if isinstance(chosen, (int, str)):
            self.chosen = [str(chosen)]
        elif isinstance(chosen, list):
            self.chosen = chosen
        elif isinstance(chosen, bool):
            if chosen == True:
                self.chosen = self.vals.id_var.to_list()
            else:
                self.chosen = []
        else:
            self.chosen = chosen.id_var.to_list()
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
    """
    Henter variable for en tabel og tilader, at variablene analyseres og udvælges.
    Hver variabel konstrueres med Variable-klassen. 
    Variablenes indhold kan ses med `ds["{VAR_NAVN}"].vals`.
    For at udvælge mindre dele af variablen, så kan vi overskrive ds["{VAR_NAVN}"] med en DataFrame, der indeholder de ønskede variable i kolonnen `id_vars`.
    For at udvælge alle variable, kan vi sætte ds["{VAR_NAVN}"] lig med `True`.
    For at hente data, så bruger vi `ds.get_data()`
    """

    def __init__(self, tablename, accept=False):
        url = "https://api.statbank.dk/v1/tableinfo"
        self.tablename = tablename
        self.accept = accept
        self.link = f"www.statistikbanken.dk/{tablename}"
        payload = {
           "table": self.tablename,
           "format": "JSON"
        }
        self.rå_metadata = requests.post(url, payload).json()
        self.make_tables()

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
        Estimerer antallet af rækker i den returnerede tabel
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
    
    def create_sub_query(self):
        res = []
        for var_key, var_value in self.vars.items():
            if isinstance(var_value, Variable):
                res.append(var_value.create_query())
            if isinstance(var_value, list):
                var_value = [str(x) for x in var_value]
                res.append({"code": var_key, "values": var_value})
            if isinstance(var_value, (str, int)):
                res.append({"code": var_key, "values": str(var_value)})
        return res

    def create_query(self):
        vars = self.create_sub_query()
        vars = [x for x in vars if len(x["values"]) != 0]
        if self.get_number_of_combinations() > 2*10**6: # Just for security
            format = "BULK"
            if not self.accept:
                self.are_you_sure()
        else:
            format = "CSV" 
        payload = {
           "table": self.tablename,
           "format": format,
            "variables": vars
        }
        return json.dumps(payload)
    
    def are_you_sure(self):
        print("You are trying to fetch a very large dataset. \n"
              "You can use `accept=True` when creating DataSelector to prevent this prompt. \n"
              "The total number of combinations are: \n"
              "{:_}".format(self.get_number_of_combinations()))
        answer = input("Do you wish to continue? (y/N) ")
        if answer.lower() != "y":
            raise InterruptedError("Process interrupted due to user input")

    def get_data(self):
        url = "https://api.statbank.dk/v1/data"
        headers = {
            'Content-Type': 'application/json',
            'Accept-Encoding': 'gzip, deflate, br'
        }
        resp = requests.post(url, self.create_query(), headers=headers)
        if resp.status_code != 200:
            error_msg = resp.text
            raise requests.RequestException(f"Error fetching data: {error_msg}")
        df = pd.read_csv(
            StringIO(
                resp.text
            ),
            sep=";"
        )
        return df

    def select_all(self):
        for key in self.vars:
            self[key] = True

    def __repr__(self):
        pprint.pprint(self.vars)
        return ""
    
    def __getitem__(self, key):
        return self.vars[key]
    
    def __setitem__(self, key, new_val):
        self.vars[key].set_vals(new_val)

if __name__ == '__main__':
    print("PERFOMING TEST")
    md = Metadata()
    print("Metadata fetched")
    ds = DataSelector("REGK100")
    ds.select_all()
    # print(ds["EJER"].vals)
    ds["EJER"] = "1"
    ds["Tid"] = ["2022", "2021"]
    with open("TEST_QUERY.json", "w") as f:
        json.dump(ds.create_query(), f)
    print("Test query saved in `TEST_QUERY.json`")
    print("GETTING DATA")
    df = ds.get_data()
    print("Data fetched after")
    df.to_feather("TEST.feather")