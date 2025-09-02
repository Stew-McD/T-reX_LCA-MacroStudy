import os
from pathlib import Path
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np


## Set color palette
# sns.set_palette("colorblind")
sns.set_theme(style="whitegrid", context="talk")  # larger fonts
okabe_ito_9 = [
    "#E69F00",  # orange
    "#56B4E9",  # sky blue
    "#009E73",  # bluish green
    "#F0E442",  # yellow
    "#0072B2",  # blue
    "#D55E00",  # vermillion
    "#CC79A7",  # reddish purple
    "#000000",  # black
    "#999999"   # gray
]
## Set output dirs

DIR_RESULTS = Path("/home/stew/code/gh/T-reX_LCA-MacroStudy/data/07_Visualisation_output")

DIR_BOX = DIR_RESULTS / "boxplots"

## Import data
FILE_RESULTS = "/home/stew/code/gh/T-reX_LCA-MacroStudy/data/06_Data_processing_output/markets_combined_cookedresults_df.csv"
df = pd.read_csv(FILE_RESULTS, sep=";")

df.rename(
    columns={
        "name" : "Name",
        "reference product": "Reference product",
        "unit": "Unit",
        "location": "Location",
        "prod_category": "Product category",
        "prod_sub_category": "Product subcategory",
        'ecosystem quality' : 'Ecosystem damage (species-year/kg)',
        'human health' : 'Human health damage (DALY/kg)',
        'natural resources' : 'Natural resource scarcity (USD2013/kg)',
        "Total waste (kg)": "Total waste (kg/kg)",
        "Hazardous waste (kg)": "Hazardous waste (kg/kg)",
    },
    inplace=True
)

cols = ['Code', 'Name', 'Reference product', 'Unit', 'Location',
       'Product category', 'Product subcategory',
       'Ecosystem damage (species-year/kg)', 'Human health damage (DALY/kg)',
       'Natural resource scarcity (USD2013/kg)', 'Total waste (kg/kg)',
       'Hazardous waste (kg/kg)', 'Year', 'Database']

methods = ['Ecosystem damage (species-year/kg)', 
           'Human health damage (DALY/kg)',
           'Natural resource scarcity (USD2013/kg)', 
           'Total waste (kg/kg)',
           'Hazardous waste (kg/kg)']


#make a box scatter plot for total waste and hazardous waste split by database
df_box_total = df[df["Database"] == "ecoinvent-default"]
df_clean = df_box_total.copy()

for method in methods:
    df_clean.loc[df_clean[method] <= 0, method] = np.nan  
    fig = px.box(
        df_clean,
        x=method,                # replace with your value column
        y="Product category",
        color="Product category",           # group by method
        orientation="h",          # horizontal box plot
        points="outliers",              # show all points as scatter
        log_x=True,                   # set x-axis to log scale
        hover_data=["Name", "Reference product", "Product subcategory", "Database"],  # add columns to hover
        color_discrete_sequence=okabe_ito_9,
    )
    fig.update_layout(yaxis=dict(showticklabels=True))
    fig.write_image(DIR_BOX / f"boxplot_{method.replace(' ', '_').replace('/', '_')}_plotly.svg")
    # save also as html
    fig.write_html(DIR_BOX / f"boxplot_{method.replace(' ', '_').replace('/', '_')}_plotly.html")
    print(f"Saved plotly boxplot for {method}")
    # fig.show()
    

## Make seaborn boxplots

for method in methods:
    df_clean.loc[df_clean[method] <= 0, method] = np.nan  
    plt.figure(figsize=(10, 6))
    ax = sns.boxplot(
        data=df_clean,
        x=method,
        y="Product category",
        hue="Product category",
        orient="h",
        showfliers=True,
        palette=okabe_ito_9
    )
    ax.set_xscale("log")  # log scale
    ax.set_xlim(left=0)   # x starts at 0
    ax.set_ylabel("")     # remove y labels if desired
    #plt.title(f"{method} by Product category")
    plt.tight_layout()
    plt.savefig(
        DIR_BOX / f"boxplot_{method.replace(' ', '_').replace('/', '_')}_seaborn.svg",
        format="svg",  # optional, inferred from extension
        bbox_inches="tight"
    )
    print(f"Saved boxplot for {method}")
    # plt.show()