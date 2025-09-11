import os
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import re
from itertools import cycle, islice

# ---------- Style: small sans-serif, no grids ----------
plt.rcParams.update({
    "font.family": "sans-serif",
    "font.sans-serif": ["Helvetica", "Arial", "DejaVu Sans"],
    "font.size": 9,
    "axes.titlesize": 10,
    "axes.labelsize": 9,
    "xtick.labelsize": 8,
    "ytick.labelsize": 8,
    "legend.fontsize": 8,
    "axes.grid": False,  # no gridlines
})

# ---------- Palette (Okabe–Ito; purple & green first) ----------
okabe_ito_9 = [
    "#D663EC",  # purple
    "#57B424",  # green
    "#56B4E9",  # sky blue
    "#F0B342",  # yellow (muted from F0E442)
    "#0072B2",  # blue
    "#D55E00",  # vermillion
    "#DB089C",  # reddish purple
    "#000000",  # black
    "#999999",  # gray
]

# ---------- Paths ----------
DIR_RESULTS = Path("/home/stew/code/gh/T-reX_LCA-MacroStudy/data/07_Visualisation_output")
DIR_FIGURES = DIR_RESULTS / "scatterplots-trend"
DIR_FIGURES.mkdir(parents=True, exist_ok=True)

# ---------- Data ----------
FILE_RESULTS = "/home/stew/code/gh/T-reX_LCA-MacroStudy/data/06_Data_processing_output/markets_combined_cookedresults_df.csv"
df = pd.read_csv(FILE_RESULTS, sep=";")

# Normalise column names that sometimes differ
df.rename(columns={"Database - Ssp": "Database - SSP", "Database - Rcp": "Database - RCP"}, inplace=True)

# Metadata vs methods
maybe_meta = [
    'Code', 'Location', 'Database', 'Database - SSP', 'Database - RCP', 'Year',
    'Name', 'Unit', 'Reference product', 'Reference Product', 'Category', 'Subcategory'
]
cols_metadata = [c for c in maybe_meta if c in df.columns]
cols_methods = sorted([c for c in df.columns if c not in cols_metadata], reverse=True)

# Optional drop: waste fragments
waste_acts = ["market for waste discharge"]
pattern = "|".join(map(re.escape, waste_acts))
if "Name" in df.columns:
    df = df[~df["Name"].str.contains(pattern, case=False, na=False)].copy()

# Keep only premise db (not ecoinvent-default-2020)
df_scatter = df[df["Database"] != "ecoinvent-default-2020"].copy() if "Database" in df.columns else df.copy()

# Ensure numeric methods
for m in cols_methods:
    df_scatter[m] = pd.to_numeric(df_scatter[m], errors="coerce")

# Orders & palette mapping
year_order = sorted(df_scatter["Year"].dropna().unique().tolist()) if "Year" in df_scatter.columns else []
ssp_levels = sorted(df_scatter["Database - SSP"].dropna().unique().tolist()) if "Database - SSP" in df_scatter.columns else []
palette_colors = list(islice(cycle(okabe_ito_9), len(ssp_levels)))
ssp_palette = dict(zip(ssp_levels, palette_colors))

# ---------- Helpers ----------
def _slug(s: str) -> str:
    return (s.lower()
              .replace("/", "-").replace("(", "").replace(")", "")
              .replace(" ", "_").replace(",", "").replace("__", "_"))

def _save_svg(fig, figpath_base: Path, right_pad=0.78):
    """Save current Matplotlib figure to SVG only, leaving space on right for legend."""
    figpath_base.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout(rect=[0, 0, right_pad, 1])  # reserve space for legend
    fig.savefig(figpath_base.with_suffix(".svg"), bbox_inches="tight")
    plt.close(fig)

# ---------- Matplotlib grouped boxplots (no trendlines) ----------
def make_boxplots_by_year_database_matplotlib(
    df_in: pd.DataFrame,
    method_list: list[str],
    out_dir: Path = DIR_FIGURES,
    hue_order: list[str] | None = None,
    x_order: list[int] | None = None,
    palette: dict | None = None,
):
    if "Database - SSP" not in df_in.columns or "Year" not in df_in.columns:
        print("Missing 'Database - SSP' or 'Year'; skipping matplotlib boxplots.")
        return

    hue_order = hue_order or sorted(df_in["Database - SSP"].dropna().unique().tolist())
    x_order = x_order or sorted(df_in["Year"].dropna().unique().tolist())

    x_pos = np.arange(len(x_order))
    n_hue = max(len(hue_order), 1)

    # Tight, narrow boxes grouped by year
    group_width = 0.86                      # total width for all boxes at each year
    box_width = (group_width / n_hue) * 0.7 # individual box width (narrow)
    offsets = np.linspace(-group_width/2 + box_width/2, group_width/2 - box_width/2, n_hue)/1.5

    for method in method_list:
        safe = _slug(method)
        print(f"[Matplotlib] {method}")
        fig, ax = plt.subplots(figsize=(4, 3))

        # No grids; use major + minor ticks
        ax.grid(False)
        ax.minorticks_on()
        ax.tick_params(axis="y", which="major", length=4, width=0.8, direction="in")
        ax.tick_params(axis="y", which="minor", length=2, width=0.6, direction="in")
        ax.tick_params(axis="x", which="major", length=4, width=0.8, direction="in")
        ax.tick_params(axis="x", which="minor", length=0, width=0, direction="in")

        # Draw grouped boxplots
        handles = []
        for i, ssp in enumerate(hue_order):
            positions = x_pos + offsets[i]
            data_per_year = []
            for yr in x_order:
                vals = df_in[(df_in["Database - SSP"] == ssp) & (df_in["Year"] == yr)][method].values
                vals = vals[~np.isnan(vals)] if len(vals) else vals
                data_per_year.append(vals)

            bp = ax.boxplot(
                data_per_year,
                positions=positions,
                widths=box_width,
                patch_artist=True,
                manage_ticks=False,
                showfliers=False,
            )

            # Colors & strokes
            color = palette.get(ssp, "#999999") if isinstance(palette, dict) else okabe_ito_9[i % len(okabe_ito_9)]
            for patch in bp["boxes"]:
                patch.set_facecolor(color)
                patch.set_edgecolor("black")
                patch.set_linewidth(0.7)
            for whisker in bp["whiskers"]:
                whisker.set_color("black"); whisker.set_linewidth(0.7)
            for cap in bp["caps"]:
                cap.set_color("black"); cap.set_linewidth(0.7)
            for median in bp["medians"]:
                median.set_color("black"); median.set_linewidth(1.0)

            # For legend
            handles.append(plt.Line2D([0], [0], color=color, lw=6))

        # Axes & labels
        ax.set_xticks(x_pos)
        ax.set_xticklabels(x_order)
        ax.set_xlabel("Year")
        ax.set_ylabel(method + " (kg per kg product)")
        ax.margins(x=0.06)

        # Legend outside (right)
        ax.legend(handles, hue_order, title="pLCA Database",
                  loc="upper left", bbox_to_anchor=(1.01, 1.0),
                  frameon=False, borderaxespad=0, handlelength=1.5, handleheight=0.8, labelspacing=0.5, fontsize=8)

        _save_svg(fig, out_dir / f"{safe}__by-year_by-database-ssp__matplotlib")

# ---------- Run ----------
make_boxplots_by_year_database_matplotlib(
    df_in=df_scatter,
    method_list=cols_methods,
    out_dir=DIR_FIGURES,
    hue_order=ssp_levels,
    x_order=year_order,
    palette=ssp_palette,
)

print(f"Saved Matplotlib SVG box plots to: {DIR_FIGURES}")
