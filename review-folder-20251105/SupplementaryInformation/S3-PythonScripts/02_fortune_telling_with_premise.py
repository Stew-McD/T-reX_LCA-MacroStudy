"""
02_fortune_telling_with_premise
===========================

This script will copy the default project to another, 
then use premise[bw2] to produce future scenario based databases

conda env created for this with:
conda create -n premise-bw2
conda install -c conda-forge -c cmutel -c romainsacchi premise-bw2 premise_gwp

see:
https://premise.readthedocs.io/
https://deepwiki.com/polca/premise/
"""

import bw2data as bd
import bw2io as bi
from premise import NewDatabase
from premise_gwp import add_premise_gwp

## Set parameters
ECOINVENT_VERSION = "3.9.1"
ECOINVENT_MODEL = "cutoff"
PROJECT_SOURCE = f"default-{ECOINVENT_VERSION}"
PROJECT_PREMISE = f"{PROJECT_SOURCE}-premise-sep"

SOURCE_DATABASE = f"ecoinvent-{ECOINVENT_VERSION}-{ECOINVENT_MODEL}"
PREMISE_DATABASE = f"ecoinvent-{ECOINVENT_VERSION}-premise-SSP15"

DELETE_PREMISE_PROJECT = False
BACKUP_PREMISE_PROJECT = True
GENERATE_SEPARATE_DATABASES = True
GENERATE_SUPERSTRUCTURE_DATABASE = False

## clear_cache() # clears both ecoinvent and additional inventories cache
if DELETE_PREMISE_PROJECT and PROJECT_PREMISE in bd.projects:
    print(f"Deleting existing project {PROJECT_PREMISE} ...")
    bd.projects.delete_project(PROJECT_PREMISE, delete_dir=True)

## Copy the project
bd.projects.set_current(PROJECT_SOURCE)
print(f"** Using project: {PROJECT_SOURCE} **")
print("** Existing databases **")
for db in bd.databases:
    print(f" - {db}")

PROJECT_LIST = [x.name for x in list(bd.projects)]
if PROJECT_PREMISE not in PROJECT_LIST:
    print(f"Copying project {PROJECT_SOURCE} to {PROJECT_PREMISE} ...")
    bd.projects.copy_project(PROJECT_PREMISE)
else:
    bd.projects.set_current(PROJECT_PREMISE)

print(f"** Using project: {bd.projects.current} **")

## Make a list to run premise in a loop
SCENARIOS = [
    {"model": "remind", "pathway": "SSP1-PkBudg500", "year": 2020},
    {"model": "remind", "pathway": "SSP1-PkBudg500", "year": 2030},
    {"model": "remind", "pathway": "SSP1-PkBudg500", "year": 2040},
    {"model": "remind", "pathway": "SSP1-PkBudg500", "year": 2050},
    {"model": "remind", "pathway": "SSP5-PkBudg500", "year": 2020},
    {"model": "remind", "pathway": "SSP5-PkBudg500", "year": 2030},
    {"model": "remind", "pathway": "SSP5-PkBudg500", "year": 2040},
    {"model": "remind", "pathway": "SSP5-PkBudg500", "year": 2050},
]


## Create the new databases, first checking if they already exist
print("Creating new databases with premise...")
DB_LIST = list(bd.databases)
PREMISE_DATABASE_LIST = [f"{s['pathway']}-{s['year']}" for s in SCENARIOS]
NEW_SCENARIOS = [s for s in SCENARIOS if f"{s['pathway']}-{s['year']}" not in DB_LIST]
for SCENARIO in NEW_SCENARIOS:
    ndb = NewDatabase(
        scenarios=[SCENARIO],
        source_db=SOURCE_DATABASE,
        source_version=ECOINVENT_VERSION,
        system_model=ECOINVENT_MODEL,
        key="tUePmX_S5B8ieZkkM7WUU2CnO8SmShwmAeWK9x2rTFo=",  # Decryption key
        keep_source_db_uncertainty=False,
        keep_imports_uncertainty=False,
        use_absolute_efficiency=False,
        biosphere_name="biosphere3",
    )

    # Update all sectors
    print("Updating all sectors...")
    ndb.update()

    # Generate separate databases
    if GENERATE_SEPARATE_DATABASES:
        print("Generating separate databases...")
        PREMISE_DATABASE_NAMES = [f"{s['pathway']}-{s['year']}" for s in ndb.scenarios]
        ndb.write_db_to_brightway(name=PREMISE_DATABASE_NAMES)
        print(f"** Database {PREMISE_DATABASE} initialized successfully! **")
    else:
        print("Not generating separate databases")
    # Generate a super structure database
    if GENERATE_SUPERSTRUCTURE_DATABASE:
        print("Generating a superstructure database")
        ndb.write_superstructure_db_to_brightway(name=PREMISE_DATABASE)
        print(f"** Database {PREMISE_DATABASE} initialized successfully! **")

    # Generate scenario reports as excel file
    # print("Generating scenario reports as excel files")
    # ndb.generate_scenario_report()

    print("Checking databases...")
    for DB in bd.databases:
        print(f" - {DB}")
        db = bd.Database(DB)
        print(db.metadata)

## Add GWP indicators - once per project
print("Adding GWP indicators...")
add_premise_gwp()

## Backup project
if BACKUP_PREMISE_PROJECT:
    print(f"Backing up project {PROJECT_PREMISE} ...")
    bi.backup_project_directory(PROJECT_PREMISE)
print("***** Script 02_fortune_telling_with_premise.py finished *****")
