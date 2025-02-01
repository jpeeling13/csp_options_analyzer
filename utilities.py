import os
import pandas as pd
import numpy as np
import config
from datetime import datetime
from stock_data_entry import StockDataEntry


def create_data_dirs():
    # Ensure the directories needed exist
    if not os.path.exists(config.STOCK_DATA_DIR):
        os.makedirs(config.STOCK_DATA_DIR)

        # Ensure the directories needed exist
    if not os.path.exists(config.RESULTS_DATA_DIR):
        os.makedirs(config.RESULTS_DATA_DIR)


def download_stock_data_csv(ticker, data):
    print(f"Saving data for {ticker}...")

    output_file_name = f'{ticker}{config.STOCK_DATA_FILE_ENDING}'
    full_path = os.path.join(config.STOCK_DATA_DIR, output_file_name)

    # Save the data to a CSV file
    data.to_csv(full_path)
    return full_path


def resample_data_to_weekly(df):
    # Resample using the index (which contains the Date) and take the last entry of each week
    df_weekly = df.resample('W').last()

    # Reset the index to make the Date a regular column again
    df_weekly.reset_index(inplace=True)

    return df_weekly

def generate_results_file(sde_results):
    final_results_df = StockDataEntry.new_dataframe()

    # Collect all result entries in a list
    result_entries = []

    for result in sde_results:
        result_entry = result.to_dict()
        result_entries.append(result_entry)

    # Create DataFrame from result_entries
    result_entries_df = pd.DataFrame(result_entries)

    # Drop columns that are entirely NA
    result_entries_df = result_entries_df.dropna(how='all', axis=1)

    # Only concatenate if result_entries_df is not empty
    if not result_entries_df.empty:
        final_results_df = pd.concat([final_results_df, result_entries_df], ignore_index=True)
    else:
        print("No valid entries to concatenate.")

    current_datetime = datetime.now()
    timestamp = current_datetime.strftime("%Y%m%d_%H%M%S")

    # Sort results by lowest % move for target strike
    final_results_sorted_df = final_results_df.sort_values(by='ticker', ascending=True)

    # Create a Results File using the timestamp
    results_filename = f"RESULTS_{timestamp}.csv"
    full_path = os.path.join(config.RESULTS_DATA_DIR, results_filename)

    final_results_sorted_df.to_csv(full_path, index=False)
