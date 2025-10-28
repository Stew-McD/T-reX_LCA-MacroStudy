# script to create summary tables from processed data

import pandas as pd

FILE_RESULTS = "../data/07_Price_data/markets_combined_cookedresults_df_priced.csv"

DIR_OUTPUT = "../data/10_data_processing_tables_scenarios_output/"

# load processed results data
df_results = pd.read_csv(FILE_RESULTS, sep=";")



methods = ['Waste - Carbon Dioxide (Ccs)',
       'Waste - Total',
       'Waste - Hazardous %', 
       'Waste - Circularity %', 
       'Waste - Landfilled', 
       'Waste - Recycled', 
       'Waste - Composted', 
       'Waste - Incinerated',
       'Waste - Openly Burned', 
       ]

letter = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"]

methods_letters = {method: letter[i] for i, method in enumerate(methods)}

meta_cols = ['Database', 'Database - Ssp', 'Database - Rcp','Year', ]

df_cut = df_results[meta_cols + methods]
df_cut = df_cut[df_cut["Database"] != "ecoinvent-default-2020"]

years = [2020, 2030, 2040, 2050]
scenarios = ["SSP1", "SSP5"]

all_methods_summaries = []

for method, letter in methods_letters.items():
    df = df_cut[["Database", "Database - Ssp", "Database - Rcp", "Year", method]]
    
    # Create a list to store all summaries for this method
    all_summaries = []
    
    for ssp in scenarios:
        for year in years:
            df_scenario = df[(df["Database - Ssp"] == ssp) & (df["Year"] == year)]
            summary = df_scenario[method].agg(["median", "std", "min", "max"])
            summary_sci = summary.apply(lambda x: f"{x:.2e}")
            
            # Add scenario and year info to the summary
            summary_with_info = pd.DataFrame({
                'Method': [method],
                'Letter': [letter],
                'SSP': [ssp],
                'Year': [year],
                'Median': [summary_sci['median']],
                'Std': [summary_sci['std']],
                'Min': [summary_sci['min']],
                'Max': [summary_sci['max']]
            })
            
            all_summaries.append(summary_with_info)
            all_methods_summaries.append(summary_with_info)  
    # Combine all summaries into one DataFrame
    combined_summary = pd.concat(all_summaries, ignore_index=True)
    
    # Save as single CSV per method
    output_file = f"{DIR_OUTPUT}{method.replace(' ', '_').replace('-', '').lower()}_summary_all_scenarios.csv"
    combined_summary.to_csv(output_file, sep=",", index=False)

# Create combined file with all methods
all_methods_combined = pd.concat(all_methods_summaries, ignore_index=True)
all_methods_combined.to_csv(f"{DIR_OUTPUT}all_methods_summary_all_scenarios.csv", sep=",", index=False)