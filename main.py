from dotenv import load_dotenv
import os
import numpy as np
import pandas as pd
import utilities as ut
import config

# Create an empty DataFrame with the desired column names
results_dictionary = []
ut.create_data_dirs()

# Loop over each ticker symbol
for ticker in config.TICKERS:

    # Download the historical data for the current ticker
    output_file = ut.download_stock_data_csv(ticker)

    if output_file:
        # Read in the csv file just created
        df = pd.read_csv(output_file)

        # Resample data to weekly instead of daily
        df_weekly = ut.resample_data_to_weekly(df)

        # Calculate Weekly Analysis Data And get final results
        final_ticker_entry = ut.calculate_weekly_data_points(ticker, df, df_weekly)

        # Add the dictionary of data to the final results array
        results_dictionary.append(final_ticker_entry)

        # Analyze Next Ticker
        print(f"{ticker}: CSP Analysis Complete\n\n")

print("-- ALL TICKERS: CSP ANALYSIS COMPLETED --")

# convert json array to dataframe and generate file
ut.generate_results_file(results_dictionary)
