import re
import os
import bw2data as bd
import pandas as pd
import numpy as np
from pathlib import Path

## Project settings
TITLE = "markets"
PROJECT_NAME = "T-reX_macro"
LIMIT = None  # limit the number of activities (for testing)
VERBOSE = True

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
DATABASE_NAMES = None  # you could also specify a list of databases here

# extract all databases in the project except those in the exclude list
# (ie. the default biosphere and the T-reX biosphere)

if not DATABASE_NAMES:
    exclude = ["biosphere3", "biosphere_T-reX"]
    DATABASE_NAMES = sorted(
        [x for x in bd.databases if not any(e in x for e in exclude)]
    )


## Filter settings

# Define filters for activities of interest
# Can leave as an empty list or include specific names to filter.

# Uncommenting a name (e.g. 'battery production') will include it in the filter.
names_filter = [
    "market for",
    # 'battery production',
]

# Specify CPC (Central Product Classification) numbers to include. (integers)
cpc_num_filter = [
    # 46420,
    # 46410,
    # -1 # if there is no CPC number
]

# Specify keywords to exclude from the activities.
exclude_filter = [
    "recovery",
    "treatment",
    "disposal",
    "waste",
    "services",
    "waste",
    "scrap" "site preparation",
    "construction",
    "maintainence",
]

locations_filter = [
    "GLO",
    "RoW",
    "World",
]

units_filter = [
    "kilogram",
    "cubic meter",
    "unit",
]

activitytype_filter = [
    "market activity",
]

# Make filter set in dictionary
filters = {
    "names": names_filter,
    "CPC_num": cpc_num_filter,
    "exclude": exclude_filter,
    "locations": locations_filter,
    "units": units_filter,
    "activity type": activitytype_filter,
}


## Directory paths
# Set the paths (to the data, logs, and the results

# Get the directory of the main script
cwd = Path.cwd()
# Get the path one level up
dir_root = cwd.parents[0]

# Set up the data directories
dir_data = dir_root / "data" / "04_Filter_output" / TITLE
dir_tmp = dir_data / "tmp" / PROJECT_NAME
dir_logs = dir_data / "logs" / PROJECT_NAME
dir_results = dir_data / "results" / PROJECT_NAME
dir_figures = dir_results / "figures"
dirs = [dir_data, dir_tmp, dir_logs, dir_results]

for DIR in dirs:
    if not os.path.isdir(DIR):
        os.makedirs(DIR)

activities_list = dir_data / f"activities_list_merged_{PROJECT_NAME}_{TITLE}.csv"

combined_raw_csv, combined_raw_pickle = (
    dir_tmp / f"{TITLE}_combined_rawresults_df.{x}" for x in ["csv", "pickle"]
)

combined_cooked_csv, combined_cooked_pickle = (
    dir_results / f"{TITLE}_combined_cookedresults_df.{x}" for x in ["csv", "pickle"]
)


## Functions to retrieve activities

def GetActivities(database_name, TITLE, VERBOSE):

    # set the project
    db = bd.Database(database_name)

    acts_all = pd.DataFrame([x.as_dict() for x in db])

    if "activity type" not in acts_all.columns:
        acts_all.rename(columns={"type": "activity type"}, inplace=True)

    columns = [
        "code",
        "name",
        "unit",
        "location",
        "activity type",
        "reference product",
        "classifications",
        "database",
        "production amount",
        "price",
    ]
    try:
        acts_all = acts_all[columns]
    except KeyError as e:
        missing_columns = re.findall(r"\'(.*?)\'", str(e))
        columns = list(set(columns) - set(missing_columns))
        if VERBOSE:
            print(
                f"\t** Warning: Column(s) '{missing_columns}' not found in DataFrame '{database_name}'. Removing from selection. **\n"
            )
        acts_all = acts_all[columns]

    # pull out and look at the categories
    if VERBOSE:
        print("\t* Extracting classification data")

    # Define the function to extract values
    def extract_values(row):

        isic_num, isic_name, cpc_num, cpc_name = -1, "missing", -1, "missing"
        classifications = row.get("classifications")

        if isinstance(classifications, list):
            # Initialize default values

            for classification in classifications:
                if "ISIC" in classification[0]:
                    split_values = classification[1].split(":")
                    isic_num = split_values[0].strip()
                    if len(isic_num) < 5:
                        isic_num += "0" * (5 - len(isic_num))
                    isic_num = int(isic_num)
                    isic_name = ":".join(split_values[1:]).strip()
                elif "CPC" in classification[0]:
                    split_values = classification[1].split(":")
                    cpc_num = split_values[0].strip()
                    if len(cpc_num) < 5:
                        cpc_num += "0" * (5 - len(cpc_num))
                    cpc_num = int(cpc_num)
                    cpc_name = ":".join(split_values[1:]).strip()

        return pd.Series(
            [isic_num, isic_name, cpc_num, cpc_name],
            index=["ISIC_num", "ISIC_name", "CPC_num", "CPC_name"],
        )

    def classify_from_ISICandCPC(acts_all):
        # Use the apply method to apply the function row-wise and join the results to the original DataFrame

        acts_all[["ISIC_num", "ISIC_name", "CPC_num", "CPC_name"]] = acts_all.apply(
            extract_values, axis=1
        )

        acts_all["ISIC_num"] = acts_all["ISIC_num"].astype("Int64")
        acts_all["CPC_num"] = acts_all["CPC_num"].astype("Int64")

        # acts_all.replace("", np.nan, inplace=True)
        # acts_all['CPC_num'] = acts_all['CPC_num'].replace('',-1).astype(int)
        # acts_all['ISIC_num'] = acts_all['ISIC_num'].replace('',-1).astype(int)
        # Drop the original "classifications" column
        acts_all = acts_all.drop("classifications", axis=1)

        # Filter activities in database to select those of interest
        acts = filter_dataframe(acts_all, filters)

        # assign product categories and sub-categories based on CPC and ISIC codes
        acts["prod_category"] = ""
        acts["prod_sub_category"] = ""
        for i, j in acts.iterrows():

            cpc = acts.at[i, "CPC_num"]
            isic = acts.at[i, "ISIC_num"]

            # cpc = str(acts.at[i, "CPC_num"])
            # if cpc == '<NA>':
            #     cpc = 'missing'
            #     pass

            # if len(cpc) < 5:
            #     cpc += "0"*(5-len(cpc))
            # cpc = int(cpc)

            if cpc in range(0, 2000) or cpc in range(3000, 4000):
                acts.at[i, "prod_category"] = "AgriForeAnim"
                acts.at[i, "prod_sub_category"] = "Agricultural & forestry products"
            if cpc in range(2000, 3000) or cpc in range(4000, 5000):
                acts.at[i, "prod_category"] = "AgriForeAnim"
                acts.at[i, "prod_sub_category"] = "Live animal, fish & their products"
            if cpc in range(11000, 18000):
                acts.at[i, "prod_category"] = "OreMinFuel"
                acts.at[i, "prod_sub_category"] = "Ores, minerals & fuels"
            if cpc in range(18000, 19000):
                acts.at[i, "prod_category"] = "Chemical"
                acts.at[i, "prod_sub_category"] = "Chemical products"
            if cpc in range(21000, 24000):
                acts.at[i, "prod_category"] = "ProcBio"
                acts.at[i, "prod_sub_category"] = "Food & beverages, animal feed"
            if cpc in range(26000, 28200):
                acts.at[i, "prod_category"] = "ProcBio"
                acts.at[i, "prod_sub_category"] = "Textile"
            if cpc in range(31000, 32000):
                acts.at[i, "prod_category"] = "ProcBio"
                acts.at[i, "prod_sub_category"] = "Wood, straw & cork"
            if cpc in range(32000, 33000):
                acts.at[i, "prod_category"] = "ProcBio"
                acts.at[i, "prod_sub_category"] = "Pulp & paper"
            if cpc in range(33000, 34000):
                acts.at[i, "prod_category"] = "OreMinFuel"
                acts.at[i, "prod_sub_category"] = "Ores, minerals & fuels"
            if cpc in range(34000, 36000):
                acts.at[i, "prod_category"] = "Chemical"
                acts.at[i, "prod_sub_category"] = "Chemical products"
            if cpc in range(34700, 34800):
                acts.at[i, "prod_category"] = "PlastRub"
                acts.at[i, "prod_sub_category"] = "Plastics & rubber products"
            if cpc in range(35500, 37000):
                acts.at[i, "prod_category"] = "PlastRub"
                acts.at[i, "prod_sub_category"] = "Plastics & rubber products"
            if cpc in range(37000, 38000):
                acts.at[i, "prod_category"] = "GlasNonMetal"
                acts.at[i, "prod_sub_category"] = "Glass & other non-metallic products"
            if cpc in range(39000, 40000):
                acts.at[i, "prod_category"] = "AgriForeAnim"
                acts.at[i, "prod_sub_category"] = "Agricultural & forestry products"
            if cpc in range(40000, 42000):
                acts.at[i, "prod_category"] = "MetalAlloy"
                acts.at[i, "prod_sub_category"] = (
                    "Basic metals & alloys, their semi-finished products"
                )
            if cpc in range(42000, 43000):
                acts.at[i, "prod_category"] = "ProcBio"
                acts.at[i, "prod_sub_category"] = "Food & beverages, animal feed"
            if cpc in range(43000, 49000):
                acts.at[i, "prod_category"] = "MachElecTrans"
                acts.at[i, "prod_sub_category"] = "Metal/electronic equipments & parts"
            if cpc in range(49000, 49400):
                acts.at[i, "prod_category"] = "MachElecTrans"
                acts.at[i, "prod_sub_category"] = "Transport vehicles"
            if cpc in range(49000, 49940):
                acts.at[i, "prod_category"] = "MachElecTrans"
                acts.at[i, "prod_sub_category"] = "Transport vehicles"
            if cpc in range(49941, 50000):
                acts.at[i, "prod_category"] = "MachElecTrans"
                acts.at[i, "prod_sub_category"] = "Metal/electronic equipments & parts"
            if cpc in range(53000, 58000):
                acts.at[i, "prod_category"] = "Construction"
                acts.at[i, "prod_sub_category"] = "Construction"
            if cpc in range(60000, 70000):
                acts.at[i, "prod_category"] = "OreMinFuel"
                acts.at[i, "prod_sub_category"] = "Ores, minerals & fuels"
            if cpc in range(80000, 10000):
                acts.at[i, "prod_category"] = "Services"
                acts.at[i, "prod_sub_category"] = "Services"
            if cpc in range(89000, 95000):
                acts.at[i, "prod_category"] = "Services"
                acts.at[i, "prod_sub_category"] = (
                    "Material recovery & waste management services"
                )
            if cpc == 38100:  # wooden furniture
                acts.at[i, "prod_category"] = "ProcBio"
                acts.at[i, "prod_sub_category"] = "Wood, straw & cork"
            if cpc == 38450:  # fishing stuff
                acts.at[i, "prod_category"] = "ProcBio"
                acts.at[i, "prod_sub_category"] = "Textile"
            if cpc == 38150:
                acts.at[i, "prod_category"] = "MachElecTrans"
                acts.at[i, "prod_sub_category"] = "Furniture"

        return acts

    # try to extract classification data from the activities
    try:
        acts = classify_from_ISICandCPC(acts_all)
        # look at the categories in the activities

        isic_names = acts_all["ISIC_name"].unique()
        isic_names = [name for name in isic_names if isinstance(name, str) and name]
        isic_names.sort()
        # Extracting and processing unique CPC names
        cpc_names = acts_all["CPC_name"].unique().tolist()
        cpc_names = [name for name in cpc_names if isinstance(name, str) and name]
        cpc_names.sort()
        isic_names_filtered = acts.ISIC_name.unique()
        isic_names_filtered.sort()
        cpc_names_filtered = acts.CPC_name.unique().tolist()
        cpc_names_filtered.sort()

        info = (
            f"** {database_name} **\n"
            f"\t{database_name}: # of activities before filtering: {len(acts_all)})\n"
            f"\t# of ISIC categories: {len(isic_names)}\n"
            f"\t# of CPC categories: {len(cpc_names)}\n"
            f"\t* Filtering activities\n"
            f"\t# of activities after filtering: {len(acts)}\n"
            f"\t# of ISIC categories after filtering: {len(isic_names_filtered)}\n"
            f"\t# of CPC categories after filtering: {len(cpc_names_filtered)}\n"
        )

        if VERBOSE:
            print(info)
        else:
            text = "{:<50.50}".format(database_name)
            print(f"  {text} : {len(acts_all)} --> {len(acts)} acts")

    except KeyError as e:
        print(
            f"\t** Warning: classification column(s) '{e}' not found in DataFrame '{database_name}' **\n"
        )
        acts = acts_all
        text = "{:<50.50}".format(database_name)
        print(f"  {text} : {len(acts_all)} --> {len(acts)} acts")

    # save to a file for each database
    f = dir_tmp / f"activities_list_from_{db.name}_{TITLE}.csv"
    acts.to_csv(f, sep=";", index=False)
    if VERBOSE:
        print(f"\tSaved activities list to csv:\n\t {f}\n")

    return acts


def MergeActivities(DATABASE_NAMES, PROJECT_NAME, TITLE):

    print("\n** Merging activities lists from all selected databases...\n")

    files = [f"activities_list_from_{x}_{TITLE}.csv" for x in DATABASE_NAMES]

    paths = [dir_tmp / f for f in files]

    df_merged = pd.read_csv(paths[0], sep=";")
    df_merged = df_merged.reset_index(drop=True)

    if len(paths) > 1:
        for f in paths[1:]:
            df = pd.read_csv(f, sep=";")
            df_merged = pd.concat([df_merged, df], axis=0, ignore_index=True)

    # Filling missing values based on some key (e.g., 'activity_name')
    # Adjust the 'activity_name' to the actual column you want to group by.
    columns_to_fill = [
        "ISIC_num",
        "ISIC_name",
        "CPC_num",
        "CPC_name",
        "prod_category",
        "prod_sub_category",
        "activity type",
    ]
    for col in columns_to_fill:
        df_merged[col] = df_merged.groupby("name")[col].transform(
            lambda x: x.ffill().bfill()
        )

    if filters["activity type"]:
        mask = (
            df_merged["activity type"].str.contains("|".join(filters["activity type"]))
            | df_merged["activity type"].isna()
        )
        df_merged = df_merged[mask]

    file_name = dir_data / f"activities_list_merged_{PROJECT_NAME}_{TITLE}.csv"
    df_merged.to_csv(file_name, sep=";", index=False)
    print("\tSaved combined activities list to csv:\n\t  ", file_name)

    return


def filter_dataframe(df, filters):
    conditions = []
    # Name condition
    if filters["names"]:
        conditions.append(
            df["name"].apply(
                lambda x: any(str(x).startswith(name) for name in filters["names"])
            )
        )

    if filters["exclude"]:
        conditions.append(
            df["name"].apply(
                lambda x: not any(
                    name.lower() in str(x).lower() for name in filters["exclude"]
                )
            )
        )

    # Location condition
    if filters["locations"]:
        conditions.append(df["location"].isin(filters["locations"]))

    # Unit condition
    if filters["units"]:
        conditions.append(df["unit"].isin(filters["units"]))

    try:
        # Exclude CPC condition
        if filters["exclude"]:
            conditions.append(
                ~df["CPC_name"].str.contains("|".join(filters["exclude"])).fillna(False)
            )

        # Exclude ISIC condition
        if filters["exclude"]:
            conditions.append(
                ~df["ISIC_name"]
                .str.contains("|".join(filters["exclude"]))
                .fillna(False)
            )

        # CPC number condition
        if "CPC_num" in filters and filters["CPC_num"]:
            if not all(isinstance(num, (int)) for num in filters["CPC_num"]):
                raise ValueError("All CPC numbers should be of type int")
            conditions.append(df["CPC_num"].isin(filters["CPC_num"]))

        # ISIC number condition
        if "ISIC_num" in filters and filters["ISIC_num"]:
            if not all(isinstance(num, (int)) for num in filters["ISIC_num"]):
                raise ValueError("All ISIC numbers should be of type int")
            conditions.append(df["ISIC_num"].isin(filters["ISIC_num"]))

    except KeyError as e:
        print(
            f"\t** Warning: column '{e}' not found in DataFrame. Skipping classification filter conditions. **\n"
        )

    # Combine conditions using logical AND
    combined_condition = conditions[0]
    for condition in conditions[1:]:
        combined_condition &= condition

    return df[combined_condition].reset_index(drop=True)


## Run functions

#%% FILTER ACTIVITIES

# filter activities from databases
for db_name in DATABASE_NAMES:
    print(f"\nProcessing database: {db_name}")
    acts = GetActivities(db_name, TITLE, VERBOSE)

# # merge activities from all databases
MergeActivities(DATABASE_NAMES, PROJECT_NAME, TITLE)