import pandas as pd
import yfinance as yf
import numpy as np
import math
from datetime import datetime
import sys

# Configuration
tickerFile = "config/ticker_file_config.txt"
csvStockDataNamingScheme = "_historical_stock_data.csv"
cspAssignmentThreasholdPercentage = 10
pd.set_option('display.max_rows', None)  # No limit on rows displayed
pd.set_option('display.max_columns', None)  # No limit on columns displayed (optional)
pd.set_option('display.width', None)  # No limit on the display width
pd.set_option('display.max_colwidth', None)  # No limit on column width (optional)

# Fetch historical data for all configured stocks
with open(tickerFile, 'r') as file:
    tickers = file.read().splitlines()

# Create an empty DataFrame with the desired column names
resultsColumns = ["Ticker", "Start Date", "End Date", "Weeks Analyzed",
                  "Avg Weekly Return", "Lowest Move Date", "Lowest Move Close", "Lowest Move %", "Safety % Set",
                  "Target % Below Strike", "% Occurred", "Last Close", "Recommended Strike"]

resultsDf = pd.DataFrame(columns=resultsColumns)

# Loop over each ticker symbol
for ticker in tickers:
    print(f"Downloading data for {ticker}...")

    # Download the historical data for the current ticker
    data = yf.download(ticker)
    if not data.empty:
        # Define the output CSV file name
        output_file = f'{ticker}{csvStockDataNamingScheme}'

        # Save the data to a CSV file
        data.to_csv(output_file)
        print(f"Data for {ticker} saved to {output_file}")

        # Read in the csv file just created
        df = pd.read_csv(output_file)

        # Resample data to weekly frequency, taking the last close price of each week
        df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d')

        # Resample using the Date column and take the last entry of each week
        df_weekly = df.resample('W', on='Date').last()

        # Reset the index to make Date a regular column again
        df_weekly.reset_index(inplace=True)

        # Calculate the weekly return
        df_weekly['Weekly Return'] = df_weekly['Close'].pct_change(fill_method=None) * 100

        # Find the total number of weeks in the dataset
        total_weeks = df_weekly.shape[0]
        print(f"Start: {df_weekly.iloc[0]['Date'].date()}")
        print(f"End: {df_weekly.iloc[-1]['Date'].date()}")
        print(f"Total Weeks: {total_weeks}")

        # Calculate the average weekly return
        average_weekly_return = df_weekly['Weekly Return'].mean()
        print(f"Average Weekly Return: {average_weekly_return}")

        # Calculate the week with the biggest move down
        lowest_weekly_move = df_weekly['Weekly Return'].min()
        min_return_index = df_weekly['Weekly Return'].idxmin()
        min_return_row = df_weekly.loc[min_return_index]
        print(
            f"Lowest Weekly Move: {min_return_row['Date'].date()}, {min_return_row['Close']}, {min_return_row['Weekly Return']:.6f}")

        # Loop through and find the lowest weekly move that happens less than 10% of the time
        lowest_weekly_move_int = int(lowest_weekly_move)
        for num in np.arange(-.5, lowest_weekly_move_int, -0.5):

            # Find the weeks with a weekly return less than the negative threshold
            weeks_with_large_negative_movements = df_weekly[df_weekly['Weekly Return'] < num]

            # Calculate Total number of weeks with a return less than the negative threshold
            total_negative_movement_weeks = weeks_with_large_negative_movements.shape[0]
            percentOccurred = total_negative_movement_weeks / total_weeks * 100

            # Save off Results Data for Recommended Strike
            if percentOccurred < cspAssignmentThreasholdPercentage:
                print(f"Target CSP Percentage that happens less than {cspAssignmentThreasholdPercentage} of the time: {num}, {percentOccurred}")

                # Get latest close price and calculate target strike
                last_close_value = df.iloc[-1]['Close']
                target_csp_strike = math.floor(last_close_value * ((100 + num) / 100) * 2) / 2
                print(f"Last Close: {last_close_value}")
                print(f"Target CSP Strike: {target_csp_strike}")

                resultsEntry = {
                    "Ticker": ticker,
                    "Start Date": df_weekly.iloc[0]['Date'].date(),
                    "End Date": df_weekly.iloc[-1]['Date'].date(),
                    "Weeks Analyzed": total_weeks,
                    "Avg Weekly Return": average_weekly_return,
                    "Lowest Move Date": min_return_row['Date'].date(),
                    "Lowest Move Close": min_return_row['Close'],
                    "Lowest Move %": np.trunc(min_return_row['Weekly Return'] * 1000)/1000,
                    "Safety % Set": cspAssignmentThreasholdPercentage,
                    "Target % Below Strike": num,
                    "% Occurred": percentOccurred,
                    "Last Close": last_close_value,
                    "Recommended Strike": target_csp_strike
                }

                # Convert the dictionary to a DataFrame & save entry
                entry_df = pd.DataFrame([resultsEntry])
                resultsDf = pd.concat([resultsDf, entry_df], ignore_index=True)

                break

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
resultsDf.to_csv(resultsFilename, index=False)
