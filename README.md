# DST-API

Denne pakke giver et let tilgøængeligt interface til at arbejde med [Danmarks Statistiks API](https://www.dst.dk/da/Statistik/brug-statistikken/muligheder-i-statistikbanken/api#testkonsol).

Overordnet er der to hovedklasser, som anvendes, når man arbejder med pakken:

1. `Metadata`
2. `DataSelector`

Begge af ovenstående gennemgås nedenfor. 

# Metadata

Man kan udtrække metadata fra Danmarks Statistik.

```python
>>> from funktioner import Metadata
>>> md = Metadata()
>>> md # Hvis objektet kaldes rent, så fås en guide
 
    Klassen fungerer som wrapper for funktionen "_get_metadata".
    Brug en af 3 følgende kommandoer:

    1.  self.kategorier (returnerer udvidede metadata for tables)
    2.  self.tables (returnerer tables, som kan kaldes)
    3.  self.variables (returnerer de variable, som tables indeholder)
    
```
Når først Metadata-objektet er initieret, så kan man trække 3 forskellige Pandas DataFrames ud af det, hhv.: 

1. `md.kategorier`, som giver et kategorioverblik for alle 3 levels i hierarkistrukturen.
2. `md.tables`, som returnerer de tables, som man kan bruge `DataSelector` til at arbejde med.
3. `md.variables`, som returnerer de variable, som hver table indeholder.

Herfra kan man analysere sig frem til hvilke tables, man har behov for. 

# DataSelector

Når vi har fundet et interessant table, så kan vi begynde at arbejde med det via `DataSelector`:

```python
>>> from funktioner import DataSelector
>>> ds = DataSelector("REGK100")
>>> ds # Hvis objektet kaldes rent, så fås overblik over table-variable
{'ART': <Var: ART, valgt: 0, i alt: 51>,
 'DRANST': <Var: DRANST, valgt: 0, i alt: 7>,
 'EJER': <Var: EJER, valgt: 0, i alt: 5>,
 'FUNKTION': <Var: FUNKTION, valgt: 0, i alt: 358>,
 'GRUPPERING': <Var: GRUPPERING, valgt: 0, i alt: 83>,
 'OMRÅDE': <Var: OMRÅDE, valgt: 0, i alt: 109>,
 'PRISENHED': <Var: PRISENHED, valgt: 0, i alt: 2 PERMANENT>,
 'Tid': <Var: Tid, valgt: 0, i alt: 16 PERMANENT>}
```

I ovenstående kodeeksempel får vi en dictionary med en række `Variable`-objekter. `Variable`-objektets REPR-funktion viser os navnet på variablen, antallet af mulige variable, vi kan medtage, samt hvor mange vi har udvalgt til vores datatræk.

Hvis vi gerne vil se mulighederne, som vi kan vælge imellem under en variabel, så kan vi slå values for `Variable`-objektet op via `ds["Tid"].vals`. 

```python
>>> ds["Tid"].vals
   id_var  text
0    2007  2007
1    2008  2008
2    2009  2009
3    2010  2010
4    2011  2011
5    2012  2012
6    2013  2013
7    2014  2014
8    2015  2015
9    2016  2016
10   2017  2017
11   2018  2018
12   2019  2019
13   2020  2020
14   2021  2021
15   2022  2022
```

I ovenstående DataFrame skal man særligt bide mærke i kollonen `id_vars`, som indeholder den data, som vi skal bruge, når vi i det nedenstående skal udvælge vores data forud for hentning.

## Udvælgelse af variable
For at udvælge variable til vores datatræk, så kan vi anvende flere forskellige metoder, som gennemgås nedenfor. 

### Input af True og False
Hvis vi gerne vil vælge alle mulige kategorier af en variabel, så bruer vi `True`, som i det følgende eksempel, hvor vi gerne vil vælge alle variable under variablen `Tid`:

```python
>>> ds["Tid"] = True
>>> ds["Tid"]
<Var: Tid, valgt: 16, i alt: 16 PERMANENT>
```

Som det ses, så er samtlige kategorier nu blevet valgt. 
For at afvælge kategorier, så kan vi gøre det samme - bare med `False`:

```python
>>> ds["Tid"] = False
>>> ds["Tid"]
<Var: Tid, valgt: 0, i alt: 16 PERMANENT>
```

Som det ses er der nu 0 valgte kategorier under tid.

### Anvendelse af `str`, `int` eller `list`

Vi kan også udvælge kategorier med objekter af typerne `str`, `int` eller `list`.
I denne forbindelse er det vigtigt, at vi anvender værdierne fra kolonnen `id_var`, som vi kan se, når vi kalder `ds["{VARIABEL_NAVN}"].vals`. 

```python
>>> ds["Tid"] = ["2022", 2021]
>>> ds["Tid"]
<Var: Tid, valgt: 2, i alt: 16 PERMANENT>
```

Som det ses, så kan vi både benytte integers og strings i vores liste.
Det er blot vigtigt at huske, at alle værdier i listen konverteres til strings.

Ligeledes kan i gøre det samme for strings, hvor vi dog kun har mulighed for at vælge en enkel værdi:

```python
>>> ds["Tid"] = 2022
>>> ds["Tid"]
<Var: Tid, valgt: 1, i alt: 16 PERMANENT>
``` 

### Anveldelse af DataFrames

Vi kan også anvende en DataFrame til at sætte vores query.
Det eneste krav, der er til dette DataFrame er, at den indeholder kolonnen `id_var`. Lad os eksempelvis sige, at vi er interesserede i at hente de seneste 3 år:

```python
>>> udvalgte = ds["Tid"].vals.sort_values("text", ascending=False).head(3)
>>> udvalgte
   id_var  text
15   2022  2022
14   2021  2021
13   2020  2020
>>> ds["Tid"] = udvalgte
>>> ds["Tid"]
<Var: Tid, valgt: 3, i alt: 16 PERMANENT>
```

### `select_all()`

Hvis vi vil vælge alle variable, så kan vi bruge `ds.select_all()`.

```python
>>> ds.select_all()
>>> ds
{'ART': <Var: ART, valgt: 51, i alt: 51>,
 'DRANST': <Var: DRANST, valgt: 7, i alt: 7>,
 'EJER': <Var: EJER, valgt: 5, i alt: 5>,
 'FUNKTION': <Var: FUNKTION, valgt: 358, i alt: 358>,
 'GRUPPERING': <Var: GRUPPERING, valgt: 83, i alt: 83>,
 'OMRÅDE': <Var: OMRÅDE, valgt: 109, i alt: 109>,
 'PRISENHED': <Var: PRISENHED, valgt: 2, i alt: 2 PERMANENT>,
 'Tid': <Var: Tid, valgt: 16, i alt: 16 PERMANENT>}
```

# Datahentning

Når vi har udvalgt de variable, vi er interesseret i, så kan vi hente data. 
Vi laver først vores udvælgelse, hvorefter vi kalder `ds.get_data()`, som returnerer en DataFrame.

```python
>>> ds = DataSelector("REGK100")
>>> ds["Tid"] = True
>>> ds["PRISENHED"] = "LOBM"
>>> ds["FUNKTION"] = True
>>> df = ds.get_data()
>>> df.head()
                                FUNKTION                   PRISENHED   TID  INDHOLD
0  0.22.01 Fælles formål (jordforsyning)  Løbende priser (1.000 kr.)  2007     9421
1  0.22.01 Fælles formål (jordforsyning)  Løbende priser (1.000 kr.)  2008     1231
2  0.22.01 Fælles formål (jordforsyning)  Løbende priser (1.000 kr.)  2009      290
3  0.22.01 Fælles formål (jordforsyning)  Løbende priser (1.000 kr.)  2010     6676
4  0.22.01 Fælles formål (jordforsyning)  Løbende priser (1.000 kr.)  2011    -4821
```

## Sikkerhedsprompt
Hvis man udvælger rigtig meget data, så vil man blive promptet for, om man er sikker:

```python
>>> ds.select_all()
>>> ds
{'ART': <Var: ART, valgt: 51, i alt: 51>,
 'DRANST': <Var: DRANST, valgt: 7, i alt: 7>,
 'EJER': <Var: EJER, valgt: 5, i alt: 5>,
 'FUNKTION': <Var: FUNKTION, valgt: 358, i alt: 358>,
 'GRUPPERING': <Var: GRUPPERING, valgt: 83, i alt: 83>,
 'OMRÅDE': <Var: OMRÅDE, valgt: 109, i alt: 109>,
 'PRISENHED': <Var: PRISENHED, valgt: 2, i alt: 2 PERMANENT>,
 'Tid': <Var: Tid, valgt: 16, i alt: 16 PERMANENT>}
>>> df = ds.get_data()
You are trying to fetch a very large dataset. 
You can use `accept=True` when creating DataSelector to prevent this prompt. 
The total number of combinations are: 
185_001_741_120
Do you wish to continue? (y/N) 
```

Som det ses, så får vi af vide, at vi er i gang med at hente 185 milliarder linjer.
Dette er en enorm mængde data, som vi aldrig vil kunne slippe afsted med at hente.
Hvis det er en mindre andel af data, så kan vi også risikere at blive promptet, men her kan vi trykke os videre ved at inputte "y". 

I følgende eksempel bliver vi også promptet, men her er vi i stand til at hente vores data ad en omgang.

```python
>>> ds.select_all()
>>> ds["ART"] = "TOT"
>>> df["Tid"] = 2022
>>> ds
{'ART': <Var: ART, valgt: 1, i alt: 51>,
 'DRANST': <Var: DRANST, valgt: 7, i alt: 7>,
 'EJER': <Var: EJER, valgt: 5, i alt: 5>,
 'FUNKTION': <Var: FUNKTION, valgt: 358, i alt: 358>,
 'GRUPPERING': <Var: GRUPPERING, valgt: 83, i alt: 83>,
 'OMRÅDE': <Var: OMRÅDE, valgt: 109, i alt: 109>,
 'PRISENHED': <Var: PRISENHED, valgt: 2, i alt: 2 PERMANENT>,
 'Tid': <Var: Tid, valgt: 1, i alt: 16 PERMANENT>}
>>> df = ds.get_data()
You are trying to fetch a very large dataset. 
You can use `accept=True` when creating DataSelector to prevent this prompt. 
The total number of combinations are: 
226_717_820
Do you wish to continue? (y/N) y
>>> df.shape
(203048, 9)
```

Ovenstående eksempel er illustrativt, fordi vi bliver advaret om, at vi potentielt kan komme til at hente 226 millioner rækker. Imidlertid indeholder det returnerede datasæt kun 203.048 rækker. Dette er en quirk, der er med DST's API. Det returnerer ikke nødvendigvis tom data. 

Vi kan dermed se, at det kan lykkes at hente data, og vi kan derfor undgå prompten fremadrettet, hvis vi anvender `accept=True`, som vi gør i følgende eksempel:

```python
>>> ds = DataSelector("REGK100", accept=True)
>>> ds.select_all()
>>> ds["ART"] = "TOT"
>>> df["Tid"] = 2022
>>> df = ds.get_data()
>>> df.shape
(203048, 9)
```

## Strategier ved store datasæt

En alternativ strategi, som kan give en radikalt lavere performance er, hvis store datasæt hentes ad flere omgange. Vi kan konkret anvende `pandas` til at samle de forskellige datasæt til sidst.

**Det bør understreges, at man skal være meget kritisk med, hvornår man bruger `accept=True`**.
Test størrelsen på dine queries, inden du anvender nedenstående hentningsstrategi.

**Nedenstående strategi bør som udgangspunkt kun anvendes i særlige tilfælde, hvor det kan forsvares**.

```python 
>>> import pandas as pd
>>> from funktioner import DataSelector
>>> ds = DataSelector("REGK100")
>>> ds.select_all()
>>> ds["PRISENHED"] = "LOBM"
>>> ds["ART"] = "TOT"
>>> ds["EJER"] = 1
>>> dataframes = []
>>> for x in ds["Tid"].vals.id_var:
...     ds["Tid"] = x
...     df_temp = ds.get_data()
...     dataframes.append(df_temp)
>>> df = pd.concat(dataframes)
>>> df.shape
(542425, 9)
```


