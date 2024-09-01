import os

import pandas as pd
import numpy as np
import yfinance as yf
import math
import config
from datetime import datetime


def create_data_dirs():
    # Ensure the directories needed exist
    if not os.path.exists(config.STOCK_DATA_DIR):
        os.makedirs(config.STOCK_DATA_DIR)

        # Ensure the directories needed exist
    if not os.path.exists(config.RESULTS_DATA_DIR):
        os.makedirs(config.RESULTS_DATA_DIR)


def download_stock_data_csv(ticker):
    print(f"Downloading data for {ticker}...")
    data = yf.download(ticker)

    if not data.empty:
        # Define the output CSV file name
        output_file_name = f'{ticker}{config.STOCK_DATA_FILE_ENDING}'
        full_path = os.path.join(config.STOCK_DATA_DIR, output_file_name)

        # Save the data to a CSV file
        data.to_csv(full_path)
        print(f"Data for {ticker} saved to {full_path}")

        return full_path

    print(f"ERROR: No download data found for {ticker}...")
    return False


def resample_data_to_weekly(df):
    # Resample data to weekly frequency, taking the last close price of each week
    df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d')

    # Resample using the Date column and take the last entry of each week
    df_weekly = df.resample('W', on='Date').last()

    # Reset the index to make Date a regular column again
    df_weekly.reset_index(inplace=True)

    return df_weekly


def calculate_weekly_data_points(ticker, df, df_weekly):
    # Add Weekly Return Data for each entry
    df_weekly['Weekly Return'] = df_weekly['Close'].pct_change(fill_method=None) * 100

    # ---------------------------------------------
    # -- CALCULATE ALL DATA FIELDS FOR THE ENTRY --
    # ---------------------------------------------

    # Calculate "Data Start Date"
    result_data_start_date = df_weekly.iloc[0]['Date'].date()

    # Calculate "Data End Date"
    result_data_end_date = df_weekly.iloc[-1]['Date'].date()

    # Calculate "Weeks Analyzed"
    result_total_weeks = df_weekly.shape[0]

    # Calculate "Avg Weekly Return"
    result_avg_weekly_return = np.round(df_weekly['Weekly Return'].mean(), 2)

    # Calculate "Lowest Move Date", "Lowest Move Date", "Lowest Move %"
    lowest_move_index = df_weekly['Weekly Return'].idxmin()

    result_lowest_move_date = df_weekly.loc[lowest_move_index]['Date'].date()
    result_lowest_move_close = np.round(df_weekly.loc[lowest_move_index]['Close'], 2)
    result_lowest_move_pct = np.round(df_weekly.loc[lowest_move_index]['Weekly Return'], 3)

    # Calculate "Last Close Date"
    result_last_close_date = df.iloc[-1]['Date'].date()

    # Calculate "Last Close"
    result_last_close_price = np.round(df.iloc[-1]['Close'], 2)

    # Calculate "Target % Below Close" and "% Occurred"
    result_required_pct_below_close, result_pct_occurred = calculate_target_pct_below_close(df_weekly)

    # Calculate "Recommended Strike"
    csp_strike_precise = result_last_close_price * ((100 + result_required_pct_below_close) / 100) * 2
    result_target_csp_strike = math.floor(csp_strike_precise) / 2

    # Gather data into single json result entry
    results_entry = {
        "Ticker": ticker,
        "Data Start Date": result_data_start_date,
        "Data End Date": result_data_end_date,
        "Total Weeks": result_total_weeks,
        "Avg Weekly Return": result_avg_weekly_return,
        "Lowest Move Date": result_lowest_move_date,
        "Lowest Move Close": result_lowest_move_close,
        "Lowest Move %": result_lowest_move_pct,
        "Last Close Date": result_last_close_date,
        "Last Close Price": result_last_close_price,
        "CSP Safety %": config.CSP_SAFETY_PCT,
        "Target Strike (% Under Close)": result_required_pct_below_close,
        "% Chance Assigned": result_pct_occurred,
        "Target Strike": result_target_csp_strike
    }

    return results_entry


def calculate_target_pct_below_close(df_weekly):
    lowest_weekly_move_int = int(df_weekly['Weekly Return'].min())

    for num in np.arange(-.5, lowest_weekly_move_int, -0.5):

        # Find the weeks with a weekly return less than the negative threshold
        weeks_with_large_negative_movements = df_weekly[df_weekly['Weekly Return'] < num]

        # Calculate Total number of weeks with a return less than the negative threshold
        total_weeks = df_weekly.shape[0]
        total_negative_movement_weeks = weeks_with_large_negative_movements.shape[0]
        percent_occurred = total_negative_movement_weeks / total_weeks * 100

        # Save off Results Data for Recommended Strike
        if percent_occurred < config.CSP_SAFETY_PCT:

            percent_occurred = np.round(percent_occurred, 2)
            return num, percent_occurred

    print("Could not calculate a weekly move % that happens below the threshold")
    return "ERROR"


def generate_results_file(results_json):
    final_results_columns = ["Ticker", "Data Start Date", "Data End Date", "Total Weeks",
                             "Avg Weekly Return", "Lowest Move Date", "Lowest Move Close", "Lowest Move %",
                             "Last Close Date", "Last Close Price", "CSP Safety %", "Target Strike (% Under Close)",
                             "% Chance Assigned", "Target Strike"]

    final_results_df = pd.DataFrame(columns=final_results_columns)
    for result in results_json:
        # Convert the dictionary to a DataFrame & save entry
        final_entry_df = pd.DataFrame([result])
        final_results_df = pd.concat([final_results_df, final_entry_df], ignore_index=True)

    current_datetime = datetime.now()
    timestamp = current_datetime.strftime("%Y%m%d_%H%M%S")

    # Sort results by lowest % move for target strike
    final_results_sorted_df = final_results_df.sort_values(by='Target Strike (% Under Close)', ascending=False)

    # Create a Results File using the timestamp
    results_filename = f"RESULTS_{timestamp}.csv"
    full_path = os.path.join(config.RESULTS_DATA_DIR, results_filename)

    final_results_sorted_df.to_csv(full_path, index=False)
