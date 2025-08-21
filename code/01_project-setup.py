"""
01_project_setup
================

This module initialises a brighway2 project and installs the ecoinvent database of choice
"""

import os
from py7zr import SevenZipFile

import bw2data as bd
import bw2io as bi

DELETE_PROJECT = False
PROJECT = "default-311"
VERSION = "3.11"
SYSTEM_MODEL = "cutoff"

db_dir = "../.secrets/"
db_file = "ecoinvent 3.11_cutoff_ecoSpold02.7z"
db_name = f"ecoinvent-{VERSION}-{SYSTEM_MODEL}"
db_name = db_file.replace(".7z", "").replace("_", "-").replace(" ", "-").replace("-ecoSpold02", "")


if DELETE_PROJECT and PROJECT in bd.projects and len(bd.projects) > 1:
    print(f"Deleting project {PROJECT}")
    bd.projects.delete_project(PROJECT, delete_dir=True)



bd.projects.set_current(PROJECT)
print(f"** Using project: {PROJECT} **")
print(f"\tbw2data version: {bd.__version__}")
print(f"\tbw2io version: {bi.__version__}")
# print(f"\tbw2calc version: {bc.__version__}")

print("Setting up biosphere database...")
bi.bw2setup()



print(f"** Existing databases **")
print(bd.databases)

## Extract database from 7z file
print(f"** Initializing new database: {db_name} **")
tmp = "../.secrets/tmp/"+db_name
if not os.path.isdir(tmp):
    print("\nExtracting database...")
    with SevenZipFile(db_dir+db_file, 'r') as archive:
        archive.extractall(path=tmp)
    print(f"Database extracted to {tmp}")


## Initialize database with bw2io
path = tmp + "/datasets/"
db = bi.SingleOutputEcospold2Importer(path, db_name)

bi.create_core_migrations()
db.apply_strategies()
db.statistics()

if db.statistics()[2] == 0:
    print("ok")
    db.write_database()
    db = bd.Database(db_name)
    db.metadata
else:
    print("There are unlinked exchanges. Quitting.")