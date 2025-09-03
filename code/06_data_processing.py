
import os
import pandas as pd

FILE_RAW = "/home/stew/code/gh/T-reX_LCA-MacroStudy/data/05_Calculations_output/tmp/markets_combined_rawresults_df.pickle"

FILE_ACTIVITIES = "/home/stew/code/gh/T-reX_LCA-MacroStudy/data/04_Filter_output/activities_list_merged_T-reX_macro_markets.csv"

FILE_COOKED = "/home/stew/code/gh/T-reX_LCA-MacroStudy/data/06_Data_processing_output/markets_combined_cookedresults_df.csv"


## Load data from calculations
df_raw = pd.read_pickle(FILE_RAW)
df_raw["Total (kg)"] = 1
df_raw["Total (m3)"] = 1
df_raw_columns_drop = ['parameters full', 'emptied', 'log parameters', 'activity type']

df_raw = df_raw.drop(columns=df_raw_columns_drop)

df_raw["Total waste (kg)"] = df_raw["Total (kg)"] + 1000*df_raw["Total (m3)"]
df_raw = df_raw.drop(columns=["Total (kg)", "Total (m3)"])

df_raw["Hazardous waste (kg)"] = df_raw["Hazardous (kg)"] + 1000*df_raw["Hazardous (m3)"]
df_raw = df_raw.drop(columns=["Hazardous (kg)", "Hazardous (m3)", "location", "reference product", "unit"])


## Load data from activity filtering

df_acts = pd.read_csv(FILE_ACTIVITIES, sep=";")
df_acts = df_acts.drop(columns=["production amount", "activity type", "ISIC_num", "ISIC_name", "CPC_num", "CPC_name"])

## Merge the df_acts and df_raw based on the columns "code" and "database"

df_merged = pd.merge(df_acts, df_raw, left_on=["code", "database", "name"], right_on=["code", "database", "name"], how="inner")

## Process the dataframe for visualisation

# Limit to activities with a unit of kg or m3

df_merged = df_merged[df_merged["unit"].isin(["kilogram", "cubic meter"])]

# replace kilogram with kg and cubic meter with m3
df_merged["unit"] = df_merged["unit"].replace({"kilogram": "kg", "cubic meter": "m3"})

# for activities with unit m3, convert to kg
methods = ['ecosystem quality',
       'human health', 'natural resources', 'Total waste (kg)',
       'Hazardous waste (kg)']
df_merged[methods] = df_merged[methods].where(df_merged["unit"] == "m3", df_merged[methods] * 1000)
df_merged["unit"] = "kg"

# set ecoinvent baseline year to 2020
df_merged["database"] = df_merged["database"].replace({"ecoinvent-3.9.1-cutoff":"ecoinvent-default-2020"})

# add column for year based on the column "database"
df_merged["Year"] = df_merged["database"].str.split("-").str[2]
df_merged["Database"] = df_merged["database"].str.split("-").str[0:2].str.join("-")
df_merged.drop(columns=["database"], inplace=True)

# output the final dataframe

df_cooked = df_merged.copy()
df_cooked.to_csv(FILE_COOKED, sep=";", index=False)