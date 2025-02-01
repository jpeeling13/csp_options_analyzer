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


def calculate_tgt_strike_pct_data(df_weekly):
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


def calculate_tgt_strike_pct_runs(df_weekly, tgt_strike_pct):
    # Calculate the percentage change in Close price from the previous week
    df_weekly['pct_change'] = df_weekly['Close'].pct_change()

    # Flag weeks when the stock closed above the tgt_strike_pct move
    df_weekly['above_tgt_pct_move'] = df_weekly['pct_change'] >= tgt_strike_pct / 100

    # Identify consecutive runs of 'above_tgt_pct_move'
    # Create a column that marks the start of a new run by comparing it to the previous row
    df_weekly['run_change'] = df_weekly['above_tgt_pct_move'].ne(df_weekly['above_tgt_pct_move'].shift()).cumsum()

    # Filter out only the groups where the stock was above the tgt_strike_pct move
    positive_runs = df_weekly[df_weekly['above_tgt_pct_move']]

    # Group by run_change and count the length of each run
    run_lengths = positive_runs.groupby('run_change').size()

    # Calculate statistics
    longest_run = run_lengths.max()
    shortest_run = run_lengths.min()
    average_run = np.round(run_lengths.mean(), 2)

    return longest_run, shortest_run, average_run


# Calculate the RSI (Relative Strength Index) with a 14-day window
def calculate_rsi(df, window=14):
    delta = df['Close'].diff(1)

    # Separate gains and losses
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)

    # Calculate the exponential moving average (Wilder's method)
    avg_gain = gain.ewm(com=window - 1, adjust=False).mean()
    avg_loss = loss.ewm(com=window - 1, adjust=False).mean()

    # Calculate relative strength (RS)
    rs = avg_gain / avg_loss

    # Calculate RSI
    rsi = 100 - (100 / (1 + rs))

    return np.round(rsi.iloc[-1], 2)


def calculate_macd(df, short_window=12, long_window=26, signal_window=9):
    """
    Calculate the MACD, Signal Line, and MACD Histogram for the most recent date.

    Parameters:
    df (DataFrame): DataFrame containing historical stock data with 'Close' prices.
    short_window (int): The window for the short-term EMA (default 12).
    long_window (int): The window for the long-term EMA (default 26).
    signal_window (int): The window for the signal line EMA (default 9).

    Returns:
    dict: Dictionary containing the most recent MACD Line, Signal Line, and MACD Histogram.
    """
    # Calculate the short-term (12-period) EMA of the Close price
    df['EMA_12'] = df['Close'].ewm(span=short_window, adjust=False).mean()

    # Calculate the long-term (26-period) EMA of the Close price
    df['EMA_26'] = df['Close'].ewm(span=long_window, adjust=False).mean()

    # Calculate the MACD Line (12-period EMA - 26-period EMA)
    df['MACD_Line'] = df['EMA_12'] - df['EMA_26']

    # Calculate the Signal Line (9-period EMA of the MACD Line)
    df['Signal_Line'] = df['MACD_Line'].ewm(span=signal_window, adjust=False).mean()

    # Calculate the MACD Histogram (MACD Line - Signal Line)
    df['MACD_Histogram'] = df['MACD_Line'] - df['Signal_Line']

    # Get the most recent values (last row of the DataFrame)
    most_recent = df.iloc[-1]

    # Return only the most recent MACD values
    return {
        'MACD_Line': np.round(most_recent['MACD_Line'], 3),
        'Signal_Line': np.round(most_recent['Signal_Line'], 3),
        'MACD_Histogram': np.round(most_recent['MACD_Histogram'], 3)
    }


def calculate_bollinger_bands(df, window=20, num_std_dev=2):
    """
    Calculate the Bollinger Bands for the most recent date.

    Parameters:
    df (DataFrame): DataFrame containing historical stock data with 'Close' prices.
    window (int): The window size for the moving average (default 20).
    num_std_dev (int): The number of standard deviations for the bands (default 2).

    Returns:
    dict: Dictionary containing the most recent Bollinger Bands (Upper, Lower, Middle) and Close price.
    """
    # Calculate the 20-day Simple Moving Average (SMA) for the 'Close' prices
    df['SMA'] = df['Close'].rolling(window=window).mean()

    # Calculate the rolling standard deviation of the 'Close' prices
    df['STD_DEV'] = df['Close'].rolling(window=window).std(ddof=0)

    # Calculate the Upper and Lower Bollinger Bands
    df['Upper_Band'] = df['SMA'] + (num_std_dev * df['STD_DEV'])
    df['Lower_Band'] = df['SMA'] - (num_std_dev * df['STD_DEV'])

    # Get the most recent values (last row of the DataFrame)
    most_recent = df.iloc[-1]

    # Return only the most recent Bollinger Bands and close price
    return {
        'Upper_Band': np.round(most_recent['Upper_Band'], 2),
        'Lower_Band': np.round(most_recent['Lower_Band'], 2),
        'Middle_Band': np.round(most_recent['SMA'], 2)
    }


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
