import os
import pandas as pd

FILE_RAW = "/home/stew/code/gh/T-reX_LCA-MacroStudy/data/05_Calculations_output/tmp/markets_combined_rawresults_df.pickle"

FILE_ACTIVITIES = "/home/stew/code/gh/T-reX_LCA-MacroStudy/data/04_Filter_output/markets/activities_list_merged_TreX_macrostudy_markets.csv"

FILE_COOKED = "/home/stew/code/gh/T-reX_LCA-MacroStudy/data/06_Data_processing_output/markets_combined_cookedresults_df.csv"

## Load data from calculations
df_raw = pd.read_pickle(FILE_RAW)

# Columns to drop initially
df_raw_columns_drop = ['parameters full', 'emptied', 'log parameters', 'activity type']
df_raw = df_raw.drop(columns=df_raw_columns_drop, errors="ignore")

# Combine kg and m3 columns (convert m3 to kg where needed)
waste_conversions = {
    "Waste - Total": ("Total (kg)", "Total (m3)"),
    "Waste - Hazardous": ("Hazardous (kg)", "Hazardous (m3)"),
    "Waste - Landfilled": ("Landfill (kg)", "Landfill (m3)"),
    "Waste - Digested": ("Digestion (kg)", "Digestion (m3)"),
    "Waste - Incinerated": ("Incineration (kg)", "Incineration (m3)")
}

for new_col, (kg_col, m3_col) in waste_conversions.items():
    df_raw[new_col] = df_raw[kg_col] + 1000 * df_raw[m3_col]

# Other waste types
df_raw['Waste - Radioactive'] = 1000 * df_raw['Radioactive (m3)']
df_raw['Waste - Recycled'] = df_raw['Recycling (kg)']
df_raw['Waste - Openly burned'] = df_raw['Openburning (kg)']
df_raw['Waste - Composted'] = df_raw['Composting (kg)']
df_raw['Waste - Carbon dioxide (CCS)'] = df_raw['Carbondioxide (kg)']

# Custom waste metrics
# Drop all original columns that are now merged or not needed
cols_to_drop = [
    "Total (kg)", "Total (m3)", "Hazardous (kg)", "Hazardous (m3)",
    "Landfill (kg)", "Landfill (m3)", "Digestion (kg)", "Digestion (m3)",
    "Incineration (kg)", "Incineration (m3)", "Radioactive (m3)",
    "Recycling (kg)", "Openburning (kg)", "Composting (kg)", "Carbondioxide (kg)",
    "location", "reference product", "unit"
]
df_raw = df_raw.drop(columns=cols_to_drop, errors="ignore")


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

# list the metadata columns
df_merged.rename(columns={"prod_category": "Category", "prod_sub_category": "Subcategory"}, inplace=True)

cols_metadata = ['database', 'name', 'code', 'unit', 'reference product', 'location', 'Category', 'Subcategory']

# list the methods columns
cols_methods = [col for col in df_merged.columns if col not in cols_metadata]
# fill missing values in methods columns with 0
df_merged[cols_methods] = df_merged[cols_methods].fillna(0)

# for activities with unit m3, convert to kg

df_merged[cols_methods] = df_merged[cols_methods].where(df_merged["unit"] == "m3", df_merged[cols_methods] * 1000)
df_merged["unit"] = "kg"

# set ecoinvent baseline year to 2020
df_merged["database"] = df_merged["database"].replace({"ecoinvent-3.9.1-cutoff":"ecoinvent-default-2020"})

# remove date from end of database name if present
df_merged["database"] = df_merged["database"].str.replace(" 2025-09-04", "", regex=False)
df_merged["database"] = df_merged["database"].str.replace("_", "-", regex=False)
# add column for year based on the column "database"

# column names to title case
df_merged.columns = [col.title() for col in df_merged.columns]

df_merged["Year"] = df_merged["Database"].str.split("-").str[-1]
df_merged["Database - RCP"] = df_merged["Database"].str.split("-").str[-2]
df_merged["Database - SSP"] = df_merged["Database"].str.split("-").str[-3]
# df_merged.drop(columns=["Database"], inplace=True)

df_merged['Waste - Hazardous %'] = 100*df_merged['Waste - Hazardous'] / df_merged['Waste - Total']
df_merged["Waste - Circularity %"] = 100*(df_merged['Waste - Recycled'] + df_merged['Waste - Composted'] + df_merged['Waste - Digested']) / df_merged['Waste - Total']

# move the new columns to the front
cols_metadata = ['Code', 'Location', "Database",'Database - SSP', 'Database - RCP', 'Year', 'Name','Unit', 'Reference Product',  'Category', 'Subcategory']

# alpha sort the methods columns
cols_methods = sorted([col for col in df_merged.columns if col not in cols_metadata])

df_merged = df_merged[cols_metadata + cols_methods]

# column names to title case
df_merged.columns = [col.title() for col in df_merged.columns]
# output the final dataframe

df_cooked = df_merged.copy()





df_cooked.to_csv(FILE_COOKED, sep=";", index=False)