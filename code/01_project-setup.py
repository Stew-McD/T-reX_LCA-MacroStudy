"""
01_project_setup
================

This module initialises a brighway2 project and installs the ecoinvent database of choice
"""

import os
from py7zr import SevenZipFile


import bw2data as bd
import bw2io as bi
import bw2calc as bc

DELETE_PROJECT = True
DELETE_DB = True
VERSION = "3.9.1"
PROJECT = f"default-{VERSION}"
SYSTEM_MODEL = "cutoff"
import bw2data as bd

db_dir = "../.secrets/"
db_file = f"ecoinvent {VERSION}_{SYSTEM_MODEL}_ecoSpold02.7z"
db_name = f"ecoinvent-{VERSION}-{SYSTEM_MODEL}"
db_name = db_file.replace(".7z", "").replace("_", "-").replace(" ", "-").replace("-ecoSpold02", "")


if DELETE_PROJECT and PROJECT in bd.projects and len(bd.projects) > 1:
    print(f"Deleting project {PROJECT}")
    bd.projects.delete_project(PROJECT, delete_dir=True)

bd.projects.set_current(PROJECT)


print(f"** Using project: {PROJECT} **")


## check for correct versions of brightway
bw2io_correct = "0.8.12"
bw2data_correct = "3.6.6"
bw2calc_correct = "1.8.2"


print(f"\tbw2data version: {bd.__version__} -- correct is {bw2data_correct}")
print(f"\tbw2io version: {bi.__version__} -- correct is {bw2io_correct}")
print(f"\tbw2calc version: {bc.__version__}-- correct is {bw2calc_correct}")

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
print(f"Importing database from {path} ...")
db = bi.SingleOutputEcospold2Importer(path, db_name, use_mp=False)

#bi.create_core_migrations()
db.apply_strategies()
db.statistics()

if db.statistics()[2] == 0:
    print(f"Database {db_name} seems ok, writing to disk...")
    db.write_database()
    db = bd.Database(db_name)
    db.metadata
else:
    print("There are unlinked exchanges.")

print("Checking database...")
db


print(f"** Database {db_name} initialized successfully! **")