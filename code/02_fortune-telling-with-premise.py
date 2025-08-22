"""
02_fortune_telling_with_premise

This script will copy the default project to another, then use premise[bw2] to produce future scenario based databases

conda env created for this with:
conda create -n premise-bw2
conda install -c conda-forge -c cmutel -c romainsacchi premise-bw2

see:
https://premise.readthedocs.io/
https://deepwiki.com/polca/premise/
"""

import bw2data as bd
from premise import NewDatabase, clear_cache

# Set parameters
ECOINVENT_VERSION = "3.9.1"
ECOINVENT_MODEL = "cutoff"
PROJECT_SOURCE = f"default-{ECOINVENT_VERSION}"
PROJECT_PREMISE = f"{PROJECT_SOURCE}-premise"

SOURCE_DATABASE = f"ecoinvent-{ECOINVENT_VERSION}-{ECOINVENT_MODEL}"
PREMISE_DATABASE = f"ecoinvent-{ECOINVENT_VERSION}-premise-SSP15"

DELETE_PREMISE_PROJECT = True
#clear_cache() # clears both ecoinvent and additional inventories cache
if DELETE_PREMISE_PROJECT and PROJECT_PREMISE in bd.projects:
    bd.projects.remove_project(PROJECT_PREMISE, delete_data=True)

# Copy the project 
bd.projects.set_current(PROJECT_SOURCE)
print(f"** Using project: {PROJECT_SOURCE} **")
print(f"** Existing databases **")
for db in bd.databases:
    print(f" - {db}")


if PROJECT_PREMISE not in bd.projects:
    print(f"Copying project {PROJECT_SOURCE} to {PROJECT_PREMISE} ...")
    bd.projects.copy_project(PROJECT_PREMISE)
else:
    bd.projects.set_current(PROJECT_PREMISE)

print(f"** Using project: {bd.projects.current} **")

# Create the new databases
print("Creating new databases with premise...")
ndb = NewDatabase(
    scenarios=[
        {"model": "remind", "pathway": "SSP1-PkBudg500", "year": 2020},
        {"model": "remind", "pathway": "SSP1-PkBudg500", "year": 2030},
        {"model": "remind", "pathway": "SSP1-PkBudg500", "year": 2040},
        {"model": "remind", "pathway": "SSP1-PkBudg500", "year": 2050},
        {"model": "remind", "pathway": "SSP5-Base", "year": 2020},
        {"model": "remind", "pathway": "SSP5-Base", "year": 2030},
        {"model": "remind", "pathway": "SSP5-Base", "year": 2040},
        {"model": "remind", "pathway": "SSP5-Base", "year": 2050},
    ],
    source_db = SOURCE_DATABASE,
    source_version = ECOINVENT_VERSION,
    system_model = ECOINVENT_MODEL,
    key="tUePmX_S5B8ieZkkM7WUU2CnO8SmShwmAeWK9x2rTFo=",  # Decryption key
    keep_source_db_uncertainty=False,
    keep_imports_uncertainty=False,
    use_absolute_efficiency=False,
    biosphere_name="biosphere3"
)

# Update all sectors
print("Updating all sectors...")
ndb.update()

# Generate separate databases
print("Generating separate databases...")
PREMISE_DATABASE_NAMES = [f"{s['pathway']}-{s['year']}" for s in ndb.scenarios]
#ndb.write_db_to_brightway(name=PREMISE_DATABASE_NAMES)

# Generate a super structure database
print("Generating a superstructure database")
ndb.write_superstructure_db_to_brightway(name=PREMISE_DATABASE)

# Generate scenario reports as excel file
# print("Generating scenario reports as excel files")
# ndb.generate_scenario_report()


print("Checking databases...")
for DB in bd.databases:
    print(f" - {DB}")
    db = bd.Database(DB)
    print(db.metadata)


print(f"** Database {PREMISE_DATABASE} initialized successfully! **")
print("***** Script 02_fortune_telling_with_premise.py finished *****")