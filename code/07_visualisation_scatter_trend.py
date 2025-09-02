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
# sns.set_theme(style="whitegrid", context="talk")  # larger fonts
sns.set_theme(style="whitegrid")  # larger fonts
okabe_ito_9 = [
    "#009E73",  # bluish green
    "#E69F00",  # orange
    "#56B4E9",  # sky blue
    "#F0E442",  # yellow
    "#0072B2",  # blue
    "#D55E00",  # vermillion
    "#CC79A7",  # reddish purple
    "#000000",  # black
    "#999999"   # gray
]
## Set output dirs

DIR_RESULTS = Path("/home/stew/code/gh/T-reX_LCA-MacroStudy/data/07_Visualisation_output")

DIR_FIGURES = DIR_RESULTS / "scatterplots-trend"

## Import data
FILE_RESULTS = "/home/stew/code/gh/T-reX_LCA-MacroStudy/data/06_Data_processing_output/markets_combined_cookedresults_df.csv"
df = pd.read_csv(FILE_RESULTS, sep=";")

df.rename(
    columns={
        "name" : "Name",
        "code": "Code",
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

# --- Ensure output dir exists ---
DIR_FIGURES.mkdir(parents=True, exist_ok=True)

# --- Define method groups (as requested) ---
waste_methods = ["Total waste (kg/kg)", "Hazardous waste (kg/kg)"]
other_methods = [
    "Ecosystem damage (species-year/kg)",
    "Human health damage (DALY/kg)",
    "Natural resource scarcity (USD2013/kg)"
]

## Use the premise databases
df_scatter = df[df["Database"] != "ecoinvent-default"].copy()

# --- Ensure numeric types for methods ---
for m in methods:
    df_scatter[m] = pd.to_numeric(df_scatter[m], errors="coerce")

sns.set_theme(style="whitegrid")
sns.set_palette(okabe_ito_9)

db_order = sorted(df_scatter["Database"].dropna().unique().tolist())
year_order = sorted(df_scatter["Year"].dropna().unique().tolist())

def _slug(s: str) -> str:
    return (
        s.lower()
         .replace("/", "-")
         .replace("(", "")
         .replace(")", "")
         .replace(" ", "_")
         .replace(",", "")
         .replace("__", "_")
    )

def _save_current(figpath: Path, dpi=300):
    figpath.parent.mkdir(parents=True, exist_ok=True)
    plt.tight_layout()
    plt.savefig(figpath, dpi=dpi, bbox_inches="tight")
    plt.close()

def make_boxplots_by_year_database(
    df_in: pd.DataFrame,
    method_list: list[str],
    out_dir: Path = DIR_FIGURES,
    hue_order: list[str] | None = None,
    x_order: list[int] | None = None,
):
    if hue_order is None:
        hue_order = sorted(df_in["Database"].dropna().unique().tolist())
    if x_order is None:
        x_order = sorted(df_in["Year"].dropna().unique().tolist())

    for method in method_list:
        safe = _slug(method)

        plt.figure(figsize=(10, 6))
        ax = sns.boxplot(
            data=df_in,
            x="Year",
            y=method,
            hue="Database",
            order=x_order,
            hue_order=hue_order,
            dodge=True,
            showfliers=False,
        )
        ax.set_title(f"{method}\nBox plots by Year and Database (linear)")
        ax.set_xlabel("Year")
        ax.set_ylabel(method)
        ax.legend(title="Database", bbox_to_anchor=(1.02, 1), loc="upper left", borderaxespad=0)

        _save_current(out_dir / f"{safe}__by-year_by-database__linear.png")

# --- Run for your requested method sets ---
make_boxplots_by_year_database(
    df_in=df_scatter,
    method_list=methods,     # or waste_methods / other_methods
    out_dir=DIR_FIGURES,
    hue_order=db_order,
    x_order=year_order,
)

print(f"Saved box plots to: {DIR_FIGURES}")