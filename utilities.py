import os

import mibian
import pandas as pd
import numpy as np
import yfinance as yf
import math
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
    sde = StockDataEntry()

    # ---------------------------------------------
    # -- CALCULATE ALL DATA FIELDS FOR THE ENTRY --
    # ---------------------------------------------

    # Set the ticker
    sde.ticker = ticker
    sde.data_start_date = df_weekly.iloc[0]['Date'].date()
    sde.data_end_date = df_weekly.iloc[-1]['Date'].date()
    sde.total_weeks = df_weekly.shape[0]
    sde.avg_weekly_return = np.round(df_weekly['Weekly Return'].mean(), 2)

    # Calculate "Lowest Move Date", "Lowest Move Date", "Lowest Move %"
    lowest_move_index = df_weekly['Weekly Return'].idxmin()
    sde.lowest_move_date = df_weekly.loc[lowest_move_index]['Date'].date()
    sde.lowest_move_close = np.round(df_weekly.loc[lowest_move_index]['Close'], 2)
    sde.lowest_move_pct = np.round(df_weekly.loc[lowest_move_index]['Weekly Return'], 3)

    sde.last_close_date = df.iloc[-1]['Date'].date()
    sde.last_close_price = np.round(df.iloc[-1]['Close'], 2)
    sde.csp_safety_pct = config.CSP_SAFETY_PCT
    sde.target_strike_pct_under_close, sde.pct_chance_assigned = calculate_target_pct_below_close(df_weekly)

    # Calculate "Target Strike"
    csp_strike_precise = sde.last_close_price * ((100 + sde.target_strike_pct_under_close) / 100) * 2
    sde.target_strike = math.floor(csp_strike_precise) / 2

    # Calculate Recommended Option Data
    ticker_options_data = calculate_one_week_options_data(sde.ticker, sde.target_strike)
    sde.max_contracts = np.floor(config.CASH_ON_HAND / (ticker_options_data["option_strike_used"] * 100))

    # Calculate Potential Profit
    potential_profit_float = np.round(sde.max_contracts * ticker_options_data["option_last_price"] * 100, 2)
    sde.potential_profit = f"${potential_profit_float:,.2f}"

    sde.option_strike_used = ticker_options_data["option_strike_used"]
    sde.option_expiry_date = ticker_options_data["option_expiry_date"]
    sde.option_days_to_expiry = ticker_options_data["option_days_to_exp"]
    sde.option_last_price = ticker_options_data["option_last_price"]
    sde.option_volume = ticker_options_data["option_volume"]
    sde.option_open_interest = ticker_options_data["option_open_interest"]
    sde.option_implied_vol = ticker_options_data["option_impl_vol"]
    sde.option_delta = ticker_options_data["option_delta"]
    sde.option_theta = ticker_options_data["option_theta"]
    sde.option_gamma = ticker_options_data["option_gamma"]
    sde.option_vega = ticker_options_data["option_vega"]
    sde.option_rho = ticker_options_data["option_rho"]
    sde.cash_on_hand = config.CASH_ON_HAND

    return sde


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

    # Find the expiration date that is at least 7 calendar days from today
    today = pd.Timestamp.today().normalize()
    six_days_out = today + pd.Timedelta(days=6)

    # Find the first expiration date that is 7 or more days away
    option_selected_expiry_date = None
    for expiration_date in options_expiration_dates:
        if pd.Timestamp(expiration_date) >= six_days_out:
            option_selected_expiry_date = expiration_date
            break
        else:
            print(f"six days out from today: {six_days_out}")
            print(f"options expiration date is not >= 6 days out: {pd.Timestamp(expiration_date)}")

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
            print(f"No exact match for strike {recommended_strike}. Using closest strike: {closest_strike}")

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
    final_results_sorted_df = final_results_df.sort_values(by='target_strike_pct_under_close', ascending=False)

    # Create a Results File using the timestamp
    results_filename = f"RESULTS_{timestamp}.csv"
    full_path = os.path.join(config.RESULTS_DATA_DIR, results_filename)

    final_results_sorted_df.to_csv(full_path, index=False)
