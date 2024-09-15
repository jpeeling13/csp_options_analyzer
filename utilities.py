import os
import mibian
import pandas as pd
import numpy as np
import yfinance as yf
import config
from datetime import datetime
from csp_options_analyzer.stock_data_entry import StockDataEntry


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


def calculate_tgt_strike_pct_runs(df_weekly, tgt_strike_pct):
    # Calculate the percentage change in Close price from the previous week
    df_weekly['pct_change'] = df_weekly['Close'].pct_change()

    # Flag weeks when the stock closed above the tgt_strike_pct move
    df_weekly['above_tgt_pct_move'] = df_weekly['pct_change'] >= tgt_strike_pct/100

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


def calculate_one_week_options_data(ticker, recommended_strike):
    options_data_dictionary = {
        "option_strike_used": "N/A",
        "option_expiry_date": "N/A",
        "option_days_to_exp": "N/A",
        "option_last_price": "N/A",
        "option_volume": "N/A",
        "option_open_interest": "N/A",
        "option_impl_vol": "N/A",
        "option_delta": "N/A",
        "option_theta": "N/A",
        "option_gamma": "N/A",
        "option_vega": "N/A",
        "option_rho": "N/A"
    }

    # Fetch the ticker data
    ticker_data = yf.Ticker(ticker)

    # Get the current stock price
    stock_price = np.round(ticker_data.history(period="1d")["Close"].iloc[0], 3)

    # Get options expiration dates
    options_expiration_dates = ticker_data.options

    # Find the expiration date that is at least 6 calendar days from today
    today = pd.Timestamp.today().normalize()
    six_days_out = today + pd.Timedelta(days=5)

    # Find the first expiration date that is 7 or more days away
    option_selected_expiry_date = None
    for expiration_date in options_expiration_dates:
        if pd.Timestamp(expiration_date) >= six_days_out:
            option_selected_expiry_date = expiration_date
            break
        else:
            print(f"Today's Date: {today}")
            print(f"Six Day's Out: {six_days_out}")
            print(f"Option's Date is less than 6 days out: {expiration_date}")

    if option_selected_expiry_date:

        # Fetch the options chain for the selected expiration date
        options_chain = ticker_data.option_chain(option_selected_expiry_date)
        puts = options_chain.puts
        put_option = puts[puts['strike'] == recommended_strike]

        # If no exact match, find the closest strike price
        if put_option.empty:
            available_strikes = puts['strike'].values
            closest_strike = min(available_strikes, key=lambda x: abs(x - recommended_strike))

            put_option = puts[puts['strike'] == closest_strike]

        # Get the first matching option (there should be only one, but just in case)
        put_option = put_option.iloc[0]

        # Extract relevant data from the option
        option_strike_used = put_option["strike"]
        option_days_to_exp = (pd.to_datetime(option_selected_expiry_date) - today).days
        option_last_price = put_option['lastPrice']
        option_volume = put_option["volume"]
        option_open_interest = put_option["openInterest"]
        option_implied_volatility = np.round(put_option['impliedVolatility'] * 100, 3)  # mibian expects percentage

        # Get the current 3-month Treasury bill rate
        tbill = yf.Ticker("^IRX")
        tbill_data = tbill.history(period="1d")
        current_rate = tbill_data["Close"].iloc[0]

        # Use mibian to calculate Greeks
        bs = mibian.BS([stock_price, option_strike_used, current_rate, option_days_to_exp],
                       volatility=option_implied_volatility, putPrice=option_last_price)

        # Save off values to dictionary
        options_data_dictionary["option_strike_used"] = option_strike_used
        options_data_dictionary["option_expiry_date"] = option_selected_expiry_date
        options_data_dictionary["option_days_to_exp"] = option_days_to_exp
        options_data_dictionary["option_last_price"] = option_last_price
        options_data_dictionary["option_volume"] = option_volume
        options_data_dictionary["option_open_interest"] = option_open_interest
        options_data_dictionary["option_impl_vol"] = option_implied_volatility
        options_data_dictionary["option_delta"] = np.round(bs.putDelta, 3)
        options_data_dictionary["option_theta"] = np.round(bs.putTheta, 3)
        options_data_dictionary["option_gamma"] = np.round(bs.gamma, 3)
        options_data_dictionary["option_vega"] = np.round(bs.vega, 3)
        options_data_dictionary["option_rho"] = np.round(bs.putRho, 3)

    else:
        print("No expiration date found that is 7 or more days out.")

    return options_data_dictionary


def generate_results_file(sde_results):
    final_results_df = StockDataEntry.new_dataframe()

    # Collect all result entries in a list
    result_entries = []

    for result in sde_results:
        result_entry = result.to_dict()
        result_entries.append(result_entry)

    # Use pd.concat to append all rows at once
    final_results_df = pd.concat([final_results_df, pd.DataFrame(result_entries)], ignore_index=True)


    current_datetime = datetime.now()
    timestamp = current_datetime.strftime("%Y%m%d_%H%M%S")

    # Sort results by lowest % move for target strike
    final_results_sorted_df = final_results_df.sort_values(by='tgt_strike_pct', ascending=False)

    # Create a Results File using the timestamp
    results_filename = f"RESULTS_{timestamp}.csv"
    full_path = os.path.join(config.RESULTS_DATA_DIR, results_filename)

    final_results_sorted_df.to_csv(full_path, index=False)
