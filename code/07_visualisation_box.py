import os
from pathlib import Path
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
import plotly.io as pio
import re
## Set color palette
# sns.set_palette("colorblind")
sns.set_theme(style="whitegrid", context="talk")  # larger fonts
# pio.templates.default = "simple_white"
pio.templates.default = "plotly_white"

okabe_ito_9 = [
    "#D663EC", 
    "#57B424", 
    "#56B4E9",  
    "#F0B342", 
    "#0072B2",  
    "#D55E00",  
    "#FF22BD", 
    "#22D43F", 
    "#999999", 
]
## Set output dirs

DIR_RESULTS = Path("/home/stew/code/gh/T-reX_LCA-MacroStudy/data/07_Visualisation_output")

DIR_BOX = DIR_RESULTS / "boxplots"

os.makedirs(DIR_BOX, exist_ok=True)


## Import data
FILE_RESULTS = "/home/stew/code/gh/T-reX_LCA-MacroStudy/data/06_Data_processing_output/markets_combined_cookedresults_df.csv"
df = pd.read_csv(FILE_RESULTS, sep=";")

df.rename(columns={"Database - Ssp": "Database - SSP", "Database - Rcp": "Database - RCP"}, inplace=True)

cols_metadata = ['Code', 'Location', "Database",'Database - SSP', 'Database - RCP', 'Year', 'Name','Unit', 'Reference Product',  'Category', 'Subcategory']

# alpha sort the methods columns
cols_methods = sorted([col for col in df.columns if col not in cols_metadata], reverse=True)

# #make a box scatter plot for total waste and hazardous waste split by database
# df_box_total = df[df["Database"] == "ecoinvent-default-2020"]

# filter out waste products if the following strings are in the name
# list of substrings to filter out
waste_acts = [
    # "market for sewage sludge",
    # "market for manure",
    "market for waste discharge",
    # "market for liquid packaging board",
]

# build a regex OR pattern from the fragments
pattern = "|".join(map(re.escape, waste_acts))

# drop rows where "Name" contains any of those fragments
df_box_total = df.copy()
df_clean = df_box_total[~df_box_total["Name"].str.contains(pattern, case=False, na=False)]


def box_plot(df, database):
    os.makedirs(DIR_BOX / database, exist_ok=True)
    df_clean = df[df["Database"] == database].copy()

    # Make plotly boxplots
    for method in cols_methods:
        fig = px.box(
            df_clean,
            x=method,                # replace with your value column
            y="Category",
            color="Category",           # group by method
        orientation="h",          # horizontal box plot
        points="outliers",              # show all points as scatter
        log_x=False,                   # set x-axis to log scale
        hover_data=["Name", "Reference Product", "Subcategory", "Database"],  # add columns to hover
        color_discrete_sequence=okabe_ito_9,
    )
        fig.update_layout(yaxis=dict(showticklabels=True))
        fig.write_image(DIR_BOX / database / f"boxplot_{method.replace(' ', '_').replace('/', '_')}_plotly.svg")
        # save also as html
        fig.write_html(DIR_BOX / database / f"boxplot_{method.replace(' ', '_').replace('/', '_')}_plotly.html")
        print(f"Saved plotly boxplot for {method}")
        fig.update_layout(xaxis_type="log")
        fig.write_image(DIR_BOX / database / f"boxplot_{method.replace(' ', '_').replace('/', '_')}_plotly_log.svg")
        fig.write_html(DIR_BOX / database / f"boxplot_{method.replace(' ', '_').replace('/', '_')}_plotly_log.html")
    # fig.show()
    print(f"Saved all plotly boxplots for {database}")
    # Make seaborn boxplots

    for method in cols_methods:
        # df_clean.loc[df_clean[method] <= 0, method] = np.nan  
        plt.figure(figsize=(10, 6))
        ax = sns.boxplot(
            data=df_clean,
            x=method,
            y="Category",
            hue="Category",
            orient="h",
            showfliers=True,
            palette=okabe_ito_9
        )
        ax.set_xscale("log")  # log scale
        ax.set_ylabel("")     # remove y labels if desired
        #plt.title(f"{method} by Product category")
        plt.tight_layout()
        plt.savefig(
            DIR_BOX / database / f"boxplot_{method.replace(' ', '_').replace('/', '_')}_seaborn_log.svg",
            format="svg",  # optional, inferred from extension
            bbox_inches="tight"
        )
        
        # make non-log version
        plt.figure(figsize=(10, 6))
        ax = sns.boxplot(
            data=df_clean,
            x=method,
            y="Category",
            hue="Category",
            orient="h",
            showfliers=True,
            palette=okabe_ito_9
        )
        ax.set_xlim(left=0)   # x starts at 0
        # plt.show()
        ax.set_ylabel("")     # remove y labels if desired
        #plt.title(f"{method} by Product category")
        plt.tight_layout()
        plt.savefig(
            DIR_BOX / database / f"boxplot_{method.replace(' ', '_').replace('/', '_')}_seaborn.svg",
            format="svg",  # optional, inferred from extension
            bbox_inches="tight"
        )
        print(f"Saved boxplot for {method}")
    print(f"Saved all seaborn boxplots for {database}")
    
# Run box plot function for each database
for db in df["Database"].unique():
        df_box = df[df["Database"] == db]
        box_plot(df_box, db)

# if db in ['ei-cutoff-3.9-remind-SSP5-PkBudg500-2040','ei-cutoff-3.9-remind-SSP5-PkBudg500-2050']: