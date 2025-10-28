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

DB_DIR = "../.secrets/"
DB_NAME = f"ecoinvent-{VERSION}-{SYSTEM_MODEL}"
DB_FILE = f"{DB_NAME}_ecoSpold02.7z"

if DELETE_PROJECT and PROJECT in bd.projects and len(bd.projects) > 1:
    print(f"Deleting project {PROJECT}")
    bd.projects.delete_project(PROJECT, delete_dir=True)

bd.projects.set_current(PROJECT)

print(f"** Using project: {PROJECT} **")

## check for correct versions of brightway
BW2IO_CORRECT = "0.8.12"
BW2DATA_CORRECT = "3.6.6"
BW2CALC_CORRECT = "1.8.2"

print(f"\tbw2data version: {bd.__version__} -- correct is {BW2DATA_CORRECT}")
print(f"\tbw2io version: {bi.__version__} -- correct is {BW2IO_CORRECT}")
print(f"\tbw2calc version: {bc.__version__}-- correct is {BW2CALC_CORRECT}")

print("Setting up biosphere database...")
bi.bw2setup()

print("** Existing databases **")
print(bd.databases)

## Extract database from 7z file
print(f"** Initializing new database: {DB_NAME} **")
TMP = "../.secrets/tmp/"+DB_NAME
if not os.path.isdir(TMP):
    print("\nExtracting database...")
    with SevenZipFile(DB_DIR+DB_FILE, 'r') as archive:
        archive.extractall(path=TMP)
    print(f"Database extracted to {TMP}")


## Initialize database with bw2io
PATH = TMP + "/datasets/"
print(f"Importing database from {PATH} ...")
db = bi.SingleOutputEcospold2Importer(PATH, DB_NAME, use_mp=False)

#bi.create_core_migrations()
db.apply_strategies()
db.statistics()

if db.statistics()[2] == 0:
    print(f"Database {DB_NAME} seems ok, writing to disk...")
    db.write_database()
    db = bd.Database(DB_NAME)
    print(db.metadata)
else:
    print("There are unlinked exchanges.")

print("Checking database...")
print(f"** Database {DB_NAME} initialized successfully! **")
