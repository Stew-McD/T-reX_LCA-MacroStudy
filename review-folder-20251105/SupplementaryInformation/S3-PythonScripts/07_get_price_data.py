# used the scripts in Stew-McD/PriceDataForLCA to extract the price data
# this script combines the results into a single dataframe for further analysis

import pandas as pd

FILE_DATA = "../data/06_Data_processing_output/markets_combined_cookedresults_df.csv"

FILE_PRICE_DATA = "../data/07_Price_data/cutoff391_price_df.csv"

df_data = pd.read_csv(FILE_DATA, sep=";")

# load price data and select relevant columns
df_price = pd.read_csv(FILE_PRICE_DATA, sep=";")
cols_keep = ["name", "location", "amount"]
df_price = df_price[cols_keep]
df_price.rename(columns={"name": "Name", "location": "Location", "amount": "Price (EUR2005)"}, inplace=True)


# merge price data into main dataframe
df = df_data.merge(df_price, on=["Name", "Location"], how="left")

df.to_csv("../data/07_Price_data/markets_combined_cookedresults_df_priced.csv", sep=";", index=False)