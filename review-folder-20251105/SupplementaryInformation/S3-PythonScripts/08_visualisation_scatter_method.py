import os
from pathlib import Path
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
from scipy.stats import pearsonr
from sklearn.linear_model import LinearRegression
import re

## Set color palette
# sns.set_palette("colorblind")
# sns.set_theme(style="whitegrid", context="talk")  # larger fonts
sns.set_theme(style="whitegrid")  # larger fonts
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

DIR_FIGURES = DIR_RESULTS / "scatterplots-methods"

os.makedirs(DIR_FIGURES, exist_ok=True)

## Import data
FILE_RESULTS = "/home/stew/code/gh/T-reX_LCA-MacroStudy/data/06_Data_processing_output/markets_combined_cookedresults_df.csv"
df = pd.read_csv(FILE_RESULTS, sep=";")

df.rename(columns={"Database - Ssp": "Database - SSP", "Database - Rcp": "Database - RCP"}, inplace=True)

cols_metadata = ['Code', 'Location', "Database",'Database - SSP', 'Database - RCP', 'Year', 'Name','Unit', 'Reference Product',  'Category', 'Subcategory']

# alpha sort the methods columns
cols_methods = sorted([col for col in df.columns if col not in cols_metadata], reverse=True)


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
df_clean = df.copy()
df_clean = df_clean[~df_clean["Name"].str.contains(pattern, case=False, na=False)]

# --- Define method groups 
waste_methods = ['Zirconium',
 'Zinc',
 'Water',
 'Waste - Total',
 'Waste - Recycled',
 'Waste - Radioactive',
 'Waste - Openly Burned',
 'Waste - Landfilled',
 'Waste - Incinerated',
 'Waste - Hazardous %',
 'Waste - Hazardous',
 'Waste - Digested',
 'Waste - Composted',
 'Waste - Circularity %',
 'Waste - Carbon Dioxide (Ccs)',
 'Vegetable Oil',
 'Vanadium',
 'Uranium',
 'Tungsten',
 'Titanium',
 'Tin',
 'Tellurium',
 'Tantalum',
 'Strontium',
 'Silver',
 'Silicon',
 'Selenium',
 'Scandium',
 'Sand',
 'Rhodium',
 'Rare Earth',
 'Platinum',
 'Phosphate Rock',
 'Petroleum',
 'Palladium',
 'Nickel',
 'Natural Gas',
 'Magnesium',
 'Lithium',
 'Lead',
 'Latex',
 'Indium',
 'Hydrogen',
 'Holmium',
 'Helium',
 'Graphite',
 'Gold',
 'Gallium',
 'Gadolinium',
 'Fluorspar',
 'Europium',
 'Erbium',
 'Electricity',
 'Dysprosium',
 'Copper',
 'Coke',
 'Cobalt',
 'Coal(Brown)',
 'Coal(Black)',
 'Chromium',
 'Cerium',
 'Cement',
 'Cadmium',
 'Borates',
 'Beryllium',
 'Bauxite',
 'Antimony',
 'Aluminium']

other_methods = [
    "Ecosystem Quality",
    'Human Health',
    'Natural Resources'
]

def compute_stats(d: pd.DataFrame, x_col: str, y_col: str, base: str):
    dd = d[[x_col, y_col]].dropna()
    dd = dd[(dd[x_col] > 0) & (dd[y_col] > 0)]
    if dd.empty:
        return None

    if base == "log10":
        x_t = np.log10(dd[x_col].to_numpy()).reshape(-1, 1)
        y_t = np.log10(dd[y_col].to_numpy())
        k_back = 10.0
    elif base == "ln":
        x_t = np.log(dd[x_col].to_numpy()).reshape(-1, 1)
        y_t = np.log(dd[y_col].to_numpy())
        k_back = np.e
    else:
        raise ValueError("base must be 'log10' or 'ln'")

    r, p = pearsonr(x_t.ravel(), y_t)
    model = LinearRegression()
    model.fit(x_t, y_t)
    b = float(model.coef_[0])
    a = float(model.intercept_)
    r2 = float(model.score(x_t, y_t))
    k = float(k_back ** a)  # back-transform: y = k * x^b

    return {"r": r, "p": p, "b": b, "a": a, "R2": r2,
            "k_backtransform": k, "n": int(len(dd))}

stats_log10_rows, stats_ln_rows = [], []

def scatter_plot(df, database):
    os.makedirs(DIR_FIGURES / database, exist_ok=True)
    df_clean = df[df["Database"] == database].copy()

    print(f"Making scatter plots for {database} with {len(df_clean)} rows")
    # Output stats CSVs
    STATS_LOG10_CSV = DIR_FIGURES / database /  "waste_vs_impacts_log10_stats.csv"
    STATS_LN_CSV    = DIR_FIGURES / database / "waste_vs_impacts_ln_stats.csv"

    for x_method in waste_methods:
        for y_method in other_methods:
            # Clean data
            print(f"Processing {x_method} vs {y_method}")
            d = df_clean[[x_method, y_method, "Category",
                            "Name","Reference Product","Subcategory",
                            "Database","Location","Unit"]].copy()
            d = d[(d[x_method] > 0) & (d[y_method] > 0)]

            if d.empty:
                continue

            # --- Log10 version ---
            s10 = None
            try:
                s10 = compute_stats(df_clean, x_method, y_method, "log10")
            except Exception as e:
                print(f"Error computing stats (log10) for {x_method} vs {y_method}: {e}")

            if s10:
                stats_log10_rows.append({"X (waste)": x_method, "Y (impact)": y_method, **s10})

                fig10 = px.scatter(
                    d, x=x_method, y=y_method,
                    color="Category",
                    color_discrete_sequence=okabe_ito_9,
                    hover_data=["Name","Reference Product","Subcategory","Database","Location","Unit"],
                    opacity=0.85
                )
                fig10.update_traces(marker=dict(size=7, line=dict(width=0.5, color="white")))
                fig10.update_layout(template="simple_white", font=dict(size=16),
                                    legend_title_text="Category",
                                    margin=dict(l=80, r=20, t=60, b=60),
                                    title=f"{x_method} vs {y_method} (log10)\n"
                                        f"r={s10['r']:.2f}, R²={s10['R2']:.2f}, b={s10['b']:.2f}, n={s10['n']}")
                fig10.update_xaxes(type="log", title=x_method, showgrid=True)
                fig10.update_yaxes(type="log", title=y_method, showgrid=True)

                # Regression line: y = k * x^b
                x_min, x_max = d[x_method].min(), d[x_method].max()
                x_grid = np.logspace(np.log10(x_min), np.log10(x_max), 100)
                y_grid = s10["k_backtransform"] * (x_grid ** s10["b"])
                fig10.add_trace(go.Scatter(x=x_grid, y=y_grid, mode="lines",
                                        line=dict(width=2, color="black"),
                                        showlegend=False))

                base10 = f"scatter_{x_method.replace(' ','_').replace('/','_')}__vs__{y_method.replace(' ','_').replace('/','_')}_log10"
                fig10.write_image(DIR_FIGURES / database / f"{base10}.svg", scale=2)
                fig10.write_html(DIR_FIGURES /  database / f"{base10}.html")
                print(f"Saved {base10}.svg and .html")

            # --- Natural log version ---
            sln = None
            try:
                sln = compute_stats(df_clean, x_method, y_method, "ln")
            except Exception as e:
                print(f"Error computing stats (ln) for {x_method} vs {y_method}: {e}")

            if sln:
                stats_ln_rows.append({"X (waste)": x_method, "Y (impact)": y_method, **sln})

                dln = d.copy()
                dln["x_ln"] = np.log(dln[x_method])
                dln["y_ln"] = np.log(dln[y_method])

                figln = px.scatter(
                    dln, x="x_ln", y="y_ln",
                    color="Category",
                    color_discrete_sequence=okabe_ito_9,
                    hover_data=["Name","Reference Product","Subcategory","Database","Location","Unit"],
                    opacity=0.85
                )
                figln.update_traces(marker=dict(size=7, line=dict(width=0.5, color="white")))
                figln.update_layout(template="simple_white", font=dict(size=16),
                                    legend_title_text="Category",
                                    margin=dict(l=80, r=20, t=60, b=60),
                                    title=f"ln({x_method}) vs ln({y_method})\n"
                                        f"r={sln['r']:.2f}, R²={sln['R2']:.2f}, b={sln['b']:.2f}, n={sln['n']}")
                figln.update_xaxes(title=f"ln({x_method})")
                figln.update_yaxes(title=f"ln({y_method})")

                # Regression line: y_ln = a + b * x_ln
                x_min, x_max = dln["x_ln"].min(), dln["x_ln"].max()
                x_grid = np.linspace(x_min, x_max, 100)
                y_grid = sln["a"] + sln["b"] * x_grid
                figln.add_trace(go.Scatter(x=x_grid, y=y_grid, mode="lines",
                                        line=dict(width=2, color="black"),
                                        showlegend=False))

                baseln = f"scatter_{x_method.replace(' ','_').replace('/','_')}__vs__{y_method.replace(' ','_').replace('/','_')}_ln"
                figln.write_image(DIR_FIGURES /  database / f"{baseln}.svg", scale=2)
                figln.write_html(DIR_FIGURES /  database / f"{baseln}.html")
                print(f"Saved {baseln}.svg and .html")

    # --- Save stats CSVs ---
    if stats_log10_rows:
        df_stats10 = pd.DataFrame(stats_log10_rows)[
            ["X (waste)", "Y (impact)", "n", "r", "p", "R2", "b", "a", "k_backtransform"]
        ]
        df_stats10.to_csv(STATS_LOG10_CSV, index=False)
        print(f"Saved stats (log10) → {STATS_LOG10_CSV}")

    if stats_ln_rows:
        df_statsln = pd.DataFrame(stats_ln_rows)[
            ["X (waste)", "Y (impact)", "n", "r", "p", "R2", "b", "a", "k_backtransform"]
        ]
        df_statsln.to_csv(STATS_LN_CSV, index=False)
        print(f"Saved stats (ln) → {STATS_LN_CSV}")


# Run box plot function for each database
for db in df["Database"].unique():
        df_box = df[df["Database"] == db]
        scatter_plot(df_box, db)

# if db in ['ei-cutoff-3.9-remind-SSP5-PkBudg500-2040','ei-cutoff-3.9-remind-SSP5-PkBudg500-2050']: