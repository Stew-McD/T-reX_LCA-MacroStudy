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

DIR_FIGURES = DIR_RESULTS / "scatterplots-method"

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

# Use ecoinvent-default subset (same as your boxplots)
df_scatter = df[df["Database"] == "ecoinvent-default"].copy()
from scipy.stats import pearsonr
from sklearn.linear_model import LinearRegression
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
import pandas as pd

# Output stats CSVs
STATS_LOG10_CSV = DIR_RESULTS / "waste_vs_impacts_log10_stats.csv"
STATS_LN_CSV    = DIR_RESULTS / "waste_vs_impacts_ln_stats.csv"

waste_methods = ["Total waste (kg/kg)", "Hazardous waste (kg/kg)"]
other_methods = [
    "Ecosystem damage (species-year/kg)",
    "Human health damage (DALY/kg)",
    "Natural resource scarcity (USD2013/kg)"
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

for x_method in waste_methods:
    for y_method in other_methods:
        # Clean data
        d = df_scatter[[x_method, y_method, "Product category",
                        "Name","Reference product","Product subcategory",
                        "Database","Location","Unit"]].copy()
        d = d[(d[x_method] > 0) & (d[y_method] > 0)]

        if d.empty:
            continue

        # --- Log10 version ---
        s10 = compute_stats(df_scatter, x_method, y_method, "log10")
        if s10:
            stats_log10_rows.append({"X (waste)": x_method, "Y (impact)": y_method, **s10})

            fig10 = px.scatter(
                d, x=x_method, y=y_method,
                color="Product category",
                color_discrete_sequence=okabe_ito_9,
                hover_data=["Name","Reference product","Product subcategory","Database","Location","Unit"],
                opacity=0.85
            )
            fig10.update_traces(marker=dict(size=7, line=dict(width=0.5, color="white")))
            fig10.update_layout(template="simple_white", font=dict(size=16),
                                legend_title_text="Product category",
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
            fig10.write_image(DIR_FIGURES / f"{base10}.svg", scale=2)
            fig10.write_html(DIR_FIGURES / f"{base10}.html")
            print(f"Saved {base10}.svg and .html")

        # --- Natural log version ---
        sln = compute_stats(df_scatter, x_method, y_method, "ln")
        if sln:
            stats_ln_rows.append({"X (waste)": x_method, "Y (impact)": y_method, **sln})

            dln = d.copy()
            dln["x_ln"] = np.log(dln[x_method])
            dln["y_ln"] = np.log(dln[y_method])

            figln = px.scatter(
                dln, x="x_ln", y="y_ln",
                color="Product category",
                color_discrete_sequence=okabe_ito_9,
                hover_data=["Name","Reference product","Product subcategory","Database","Location","Unit"],
                opacity=0.85
            )
            figln.update_traces(marker=dict(size=7, line=dict(width=0.5, color="white")))
            figln.update_layout(template="simple_white", font=dict(size=16),
                                legend_title_text="Product category",
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
            figln.write_image(DIR_FIGURES / f"{baseln}.svg", scale=2)
            figln.write_html(DIR_FIGURES / f"{baseln}.html")
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
