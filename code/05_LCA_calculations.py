"""Module for performing LCA calculations and merging results."""

import sys
import os
from datetime import datetime, timedelta
from multiprocessing import cpu_count, Lock
from pathlib import Path

import pandas as pd
import numpy as np
from pathos.multiprocessing import ProcessingPool as Pool
from tqdm import tqdm
from termcolor import colored

num_cpus = int(os.environ.get('SLURM_CPUS_PER_TASK', os.environ.get('SLURM_JOB_CPUS_PER_NODE', cpu_count())))
print_lock = Lock()

custom_bw2_dir = None
if custom_bw2_dir:
    os.environ["BRIGHTWAY2_DIR"] = custom_bw2_dir
import bw2data as bd
import bw2calc as bc

## Project settings
TITLE = "markets"
PROJECT_NAME = "T-reX_macro"
LIMIT = None # LIMIT the number of activities (for testing)
VERBOSE = True # best to be false if multiprocessing istrue
USE_MULTIPROCESSING = False


# Get the directory of the main script
cwd = Path.cwd()
# Get the path one level up
dir_root = cwd #cwd.parents[0]
dir_data = dir_root / "data/05_Calculations_output"
dir_tmp = dir_data / "tmp"
dir_logs = dir_data / "logs"

dirs_results = [dir_data, dir_tmp, dir_logs]
if not all(d.exists() for d in dirs_results):
    for d in dirs_results:
        d.mkdir(parents=True, exist_ok=True)

activities_list = dir_root / "data" / "04_Filter_output" / "activities_list_merged_T-reX_macro_markets.csv"

if PROJECT_NAME not in bd.projects:
    print(f'{"*"*80}\n')
    print(f"Project {PROJECT_NAME} not found, exiting...")
    print(f'{"*"*80}\n')
    # exit(0)
else:
    print(f'\n\n{"="*100}\n')
    print(f'\t\t *** Processing activity set "{TITLE}" in project {PROJECT_NAME} *** ')
    print(f'\n{"="*100}\n')
    bd.projects.set_current(PROJECT_NAME)

## Database settings
DATABASE_NAMES = []  # you could also specify a list of databases here

# extract all databases in the project except those in the exclude list
# (ie. the default biosphere and the T-reX biosphere)

if  len(DATABASE_NAMES) == 0:
    exclude = ["biosphere3", "biosphere_T-reX"]
    DATABASE_NAMES = sorted(
        [x for x in bd.databases if not any(e in x for e in exclude)]
    )

## Method settings

# Filter methods to select those of interest
methods_all = np.unique([x[0] for x in bd.methods.list])
# methods_waste = [x for x in bd.methods.list if "Waste Footprint" in x[0]]
# methods_material = [x for x in bd.methods.list if "Demand:" in x[1]]

methods_waste_total = [x for x in bd.methods.list if "Waste: Total combined" in x[1]]
methods_waste_hazardous = [x for x in bd.methods.list if "Waste: Hazardous" in x[1]]

methods_recipe_selected = [x for x in bd.methods.list if "ReCiPe 2016 v1.03, endpoint (H)" == x[0]]

methods_recipe_endpoint = [x for x in methods_recipe_selected if "total:" in x[1]]

# KEYWORDS_METHODS = [
#     "T-reX",
#     "ReCiPe 2016 v1.03, endpoint (H)",
#     # "EF v3.1 EN15804",
#     # "EDIP 2003 no LT",
#     # "Crustal Scarcity Indicator 2020",
#     # "WasteAndMaterialFootprint",
# ]

# methods_other = [x for x in bd.methods.list if any(e in x[0] for e in KEYWORDS_METHODS)]

# # exclude 'no LT'
# methods_other = [x for x in methods_other if "no LT" not in x.name]

#methods = methods_other  # methods_waste + methods_material +
# methods = methods_other

methods = methods_waste_total + methods_waste_hazardous + methods_recipe_endpoint

def LCIA():

    start = datetime.now()
# read list of activities to be calculated from csv produced by MergeActivities()
    print(f'\n {"-"*80}')
    print('   \t\t\t*** Starting LCA calculations ***')
    print(f'{"-"*80}\n')
    print(f"** Reading activities list from file \n   {activities_list}")
    data = pd.read_csv(activities_list, sep=";")
    data.drop(columns=['ISIC_num', 'CPC_num'], inplace=True)
    
    bd.projects.set_current(PROJECT_NAME)
    
    estimated_time_minutes = (LIMIT if LIMIT else len(data)) * len(methods) * 0.0083
    finish_time = start + timedelta(minutes=estimated_time_minutes)
    acts2calc = LIMIT*len(DATABASE_NAMES) if LIMIT else len(data)
    total_calcs = acts2calc * len(methods)

    if finish_time.date() == datetime.now().date():
        display_time = f"{finish_time.strftime('%H:%M')} today"
    elif finish_time.date() == (datetime.now() + timedelta(days=1)).date():
        display_time = f"{finish_time.strftime('%H:%M')} tomorrow"
    else:
        # For dates further in the future, you might want to display the actual date
        display_time = finish_time.strftime('%Y-%m-%d %H:%M')

    if LIMIT:
        print(f"** Limiting number of activities to: {LIMIT} per database")

    print(f"""
    * Running calculations in {len(DATABASE_NAMES)} databases in project: {PROJECT_NAME}
    
    * Using packages:
    - bw2data: {bd.__version__}
    - bw2calc: {bc.__version__}

    * Number of activities to be calculated: {acts2calc}
    * Number of methods to be used: {len(methods)}
    * Total number of calculations: {total_calcs}

    * Estimated calculation time: {round(estimated_time_minutes, 2)} minutes
    * Should be finished at {display_time}
    """)
    print('** Using methods:')
    
    for method in sorted(set([x[1] for x in methods])): 
        print("\t", method)
    
    if LIMIT:
        print(f"\n %%% Limiting number of activities calculated per database to: {LIMIT} %%%")
    print(f'\n{"="*80}\n')
    
    args_list = [(i+1, database_name, data, PROJECT_NAME, LIMIT) for i, database_name in enumerate(DATABASE_NAMES)]
    # Use multiprocessing to parallelize the work
    
    if USE_MULTIPROCESSING:
        with Pool(num_cpus) as pool:
            print("\n ****** Using multiprocessing, the output will be messy ****** \n ")
            pool.map(LCIA_singledatabase, args_list)
            
            # print info about the combined calculations for the db
            finish = datetime.now()
            duration = finish - start
            outcome = (f"\n\n** {finish.strftime('%Y-%m-%d %H:%M:%S')} -- * Completed {total_calcs} LCIA calculations: "
                f"{len(data)} activities and {len(methods)} methods in: {duration.total_seconds()} seconds")

            with print_lock:
                print(outcome, flush=True)

            # write to log file
            log_file_path = dir_logs / f"{TITLE}_log.txt"
            try:
                with open(log_file_path, "a") as f:
                    f.write(outcome)
            except Exception as e:
                print(f"Error writing to log file: {e}")

    else:
        for args in args_list:
            LCIA_singledatabase(args)
            
                        # print info about the combined calculations for the db
            finish = datetime.now()
            duration = finish - start
            outcome = (f"\n\n** {finish.strftime('%Y-%m-%d %H:%M:%S')} -- * Completed {total_calcs} LCIA calculations: "
                f"{len(data)} activities and {len(methods)} methods in: {duration.total_seconds()} seconds")

            with print_lock:
                print(outcome, flush=True)

            # write to log file
            log_file_path = dir_logs / f"{TITLE}_log.txt"
            try:
                with open(log_file_path, "a") as f:
                    f.write(outcome)
            except Exception as e:
                print(f"Error writing to log file: {e}")
            
            
    return

def LCIA_singledatabase(args):
    
    i, database_name, data, PROJECT_NAME, LIMIT  = args
    if VERBOSE:
        print(f'**********   database_name: {database_name} *********')
    start_single = datetime.now()
    
    db = bd.Database(database_name)
# Select activities from the correct database
    acts = data[data.database == db.name]
    acts = acts.reset_index(drop=True)
    
# For testing, if you want, you can set a LIMIT on the number of activities to be calculated
    if LIMIT:
        acts = acts.sample(n=LIMIT)
        acts = acts.reset_index(drop=True)
        if VERBOSE:
            print(f"Limiting number of activities calculated to: {LIMIT}")
    else:
        if VERBOSE:
            print(f"LIMIT not defined, all {LIMIT} activities will be calculated")

# This progress bar tracks the processing of each activity-method pair for this worker
    if not VERBOSE:
        desc = f"{i:02} - {database_name}"
        # Make the description a fixed width 
        formatted_desc = "{:<60.60}".format(desc)
        # Colorize the description
        colorized_desc = colored(formatted_desc, 'white')
        bar = ("{l_bar}\033[95m{bar:30}\033[0m| {n}/{total} - "
            "[{percentage:3.0f}% - {elapsed}<{remaining}, {rate}]")
        pbar = tqdm(total=len(acts), position=i, leave=True, mininterval=0.5, desc=colorized_desc, ncols=140, bar_format=bar, unit_scale=True, file=sys.stdout)

 
    # Run first calculation
    try:
        act = db.get(acts.code.sample().values[0])
        lca = bc.LCA({act : 1}, method=bd.methods.random())
        lca.lci()
        lca.lcia()
        
    except Exception as e:
        print(f"Error {e}: 'lca' or 'act' not defined")

    # Repeat calculations for the rest of the activities
    if VERBOSE:
        print("\nRunning 'lca.redo_lci & lca.redo_lcia'...\n")
        print(" (printing only scores above 1)")

    results_dic = {}
    for j, act in acts.iterrows():
        code = act.code
        # if VERBOSE: 
        #     print(database_name, code + " : " + act['name'])

    # repeat calculations for each activity
        lca.redo_lci()
        act = db.get(code)
        dic = act.as_dict()

    # remove unnecessary keys from dictionary
        cut = [
            'comment',
            'classifications',
            'activity',
            'filename',
            'synonyms',
            'parameters',
            'authors',
            'type',
            'flow',
            'production amount'
            ]
        
        for c in cut:
            try:
                dic.pop(c)
            except KeyError:
                pass
            
        # repeat calculations for each method
        for k, m in enumerate(methods):
            lca.switch_method(m)
            lca.redo_lcia({act : 1})

            score = lca.score
            #top_acts = lca.top_activities()

            dic.update({lca.method[2] : score})
            #dic.update({"top_activities_"+lca.method[2] : top_acts})

            # print info about calculations
            if VERBOSE:
                if abs(score) > 1:
                    print(f"{database_name} {i}/{len(DATABASE_NAMES)} "
                        f"Act.{j+1: >2}/{len(acts): <2} "
                        f"Met.{k+1: >2}/{len(methods): <2} |"
                        f" Score: {score:.1e}:"
                        f" {m[1].split('_')[-1]: <10}/ {dic['unit']: <10}\t |"
                        f" '{dic['name']: <10}' |"
                        f"  with method: {lca.method[2]: <5}"
                        )
                    
            results_dic.update({dic["code"]: dic}) # add results to dictionary
        
        if VERBOSE == False:
            pbar.update(1)
    
    # get information for logs
    finish = datetime.now()
    duration = finish - start_single
    outcome = (f"\n\n** {finish.strftime('%Y-%m-%d %H:%M:%S')} -- * Completed {len(acts)*len(methods)} LCIA calculations: " f"{len(acts)} activities and {len(methods)} methods from {database_name} in: {str(duration).split('.')[0]}")
    
    if VERBOSE:
        print(outcome)

    # write to log file
    log_file_path = dir_logs / f"{database_name}_{TITLE}_log.txt"
    with open(log_file_path, "a") as f:
        f.write(outcome)
            
    # convert results dictionary to dataframe and transpose
    results_df = pd.DataFrame.from_dict(results_dic).T

    # save individual db results as pickle
    pickle_path = dir_tmp / f"{database_name}_{TITLE}_rawresults_df.pickle"
    results_df.to_pickle(pickle_path)

    # save individual db results as csv
    csv_path = dir_tmp / f"{database_name}_{TITLE}_rawresults_df.csv"
    results_df.to_csv(csv_path, sep=";")
    
    if not VERBOSE:
        pbar.close() # close progress bar
       
    return

def MergeResults():
    print(f"\n** Merging results from :\n {PROJECT_NAME} : {TITLE}")
    
    # get paths to individual results
    files = [dir_tmp / f"{database_name}_{TITLE}_rawresults_df.csv" for database_name in DATABASE_NAMES]
    
    # merge results from multiple databases
    df_merged = pd.read_csv(files[0], sep=';')
    if len(files) > 1:
        for f in files[1:]:
            df = pd.read_csv(f, sep=';')
            df_merged = pd.concat([df_merged, df], axis=0, ignore_index=True)
    df_merged = df_merged.reset_index(drop=True)
    df_merged.drop("Unnamed: 0", axis=1, inplace=True)
    
    # save combined results as pickle and csv
    if LIMIT:
        combined_raw_csv = dir_data / f"{TITLE}_{LIMIT}_combined_rawresults_df.csv"
        combined_raw_pickle = dir_data / f"{TITLE}_{LIMIT}_combined_rawresults_df.pickle"
    else:
        combined_raw_csv = dir_tmp / f"{TITLE}_combined_rawresults_df.csv"
        combined_raw_pickle = dir_tmp / f"{TITLE}_combined_rawresults_df.pickle"
    
    df_merged.to_csv(combined_raw_csv, sep=';', index=False)
    df_merged.to_pickle(combined_raw_pickle)
    print(f"\nSaved combined activities list \n\tto csv: {combined_raw_csv}\n\tand pickle: {combined_raw_pickle}")

    
    return



## Run functions

# #%% RUN CALCULATIONS
LCIA()

# merge results from all databases
MergeResults()

#%% PROCESS RESULTS