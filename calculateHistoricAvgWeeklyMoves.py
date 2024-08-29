import pandas as pd
import yfinance as yf
import utilities as ut
from datetime import datetime

# Configuration
ticker_file = "config/ticker_file_config.txt"
csv_stock_data_naming_scheme = "_historical_stock_data.csv"
threashold_pct = 10

results_columns = ["Ticker", "Start Date", "End Date", "Weeks Analyzed",
                   "Avg Weekly Return", "Lowest Move Date", "Lowest Move Close", "Lowest Move %", "Safety % Set",
                   "Target % Below Strike", "% Occurred", "Last Close Date", "Last Close", "Recommended Strike"]

# Create an empty DataFrame with the desired column names
results_df = pd.DataFrame(columns=results_columns)

# Fetch historical data for all configured stocks
with open(ticker_file, 'r') as file:
    tickers = file.read().splitlines()

# Loop over each ticker symbol
for ticker in tickers:
    print(f"Downloading data for {ticker}...")

    # TODO: Create a dedicated function for downloading the file and saving off to a .csv
    # Download the historical data for the current ticker
    data = yf.download(ticker)
    if not data.empty:
        # Define the output CSV file name
        output_file = f'{ticker}{csv_stock_data_naming_scheme}'

        # Save the data to a CSV file
        data.to_csv(output_file)
        print(f"Data for {ticker} saved to {output_file}")

        # Read in the csv file just created
        df = pd.read_csv(output_file)

        # Resample data to weekly instead of daily
        df_weekly = ut.resample_data_to_weekly(df)

        # Calculate Weekly Analysis Data And get final results
        final_ticker_entry = ut.calculate_weekly_data_points(ticker, df, df_weekly, threashold_pct)

        # Convert the dictionary to a DataFrame & save entry
        entry_df = pd.DataFrame([final_ticker_entry])
        results_df = pd.concat([results_df, entry_df], ignore_index=True)

        # Analyze Next Ticker
        print(f"{ticker}: CSP Analysis Complete\n\n")

    else:
        print(f"No data found for {ticker}")
        continue

print("-- ALL TICKERS: CSP ANALYSIS COMPLETED --")

current_datetime = datetime.now()
timestamp = current_datetime.strftime("%Y%m%d_%H%M%S")

# Create a Results File using the timestamp
resultsFilename = f"results_{timestamp}.csv"
results_df.to_csv(resultsFilename, index=False)
