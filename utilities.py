import os

import mibian
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

    # Calculate Recommended Option Data
    result_options_data = calculate_one_week_options_data(ticker, result_target_csp_strike)

    # Calculate Max Contracts & Potential Profit
    result_max_contracts = np.floor(config.CASH_ON_HAND / (result_options_data["option_strike_used"] * 100))
    potential_profit_float = np.round(result_max_contracts * result_options_data["option_last_price"] * 100, 2)
    result_potential_profit = f"${potential_profit_float:,.2f}"

    # Gather data into single dictionary result entry
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
        "Target Strike": result_target_csp_strike,
        "Option Strike Used": result_options_data["option_strike_used"],
        "Option Expiry Date": result_options_data["option_expiry_date"],
        "Option Days to Expiry": result_options_data["option_days_to_exp"],
        "Option Last Price": result_options_data["option_last_price"],
        "Option Volume": result_options_data["option_volume"],
        "Option Open Interest": result_options_data["option_open_interest"],
        "Option Implied Vol": result_options_data["option_impl_vol"],
        "Option Delta": result_options_data["option_delta"],
        "Option Theta": result_options_data["option_theta"],
        "Option Gamma": result_options_data["option_gamma"],
        "Option Vega": result_options_data["option_vega"],
        "Option Rho": result_options_data["option_rho"],
        "Cash On Hand": config.CASH_ON_HAND,
        "Max Contracts": result_max_contracts,
        "Potential Profit": result_potential_profit
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
    today = pd.Timestamp.today()
    seven_days_out = today + pd.Timedelta(days=7)

    # Find the first expiration date that is 7 or more days away
    option_selected_expiry_date = None
    for expiration_date in options_expiration_dates:
        if pd.Timestamp(expiration_date) >= seven_days_out:
            option_selected_expiry_date = expiration_date
            break

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


def generate_results_file(results_dictionary):
    final_results_columns = ["Ticker", "Data Start Date", "Data End Date", "Total Weeks",
                             "Avg Weekly Return", "Lowest Move Date", "Lowest Move Close", "Lowest Move %",
                             "Last Close Date", "Last Close Price", "CSP Safety %", "Target Strike (% Under Close)",
                             "% Chance Assigned", "Target Strike", "Option Strike Used", "Option Expiry Date",
                             "Option Days to Expiry",
                             "Option Last Price", "Option Volume", "Option Open Interest", "Option Implied Vol",
                             "Option Delta", "Option Theta", "Option Gamma",
                             "Option Vega", "Option Rho", "Cash On Hand", "Max Contracts", "Potential Profit"]

    final_results_df = pd.DataFrame(columns=final_results_columns)
    for result in results_dictionary:
        # Convert the dictionary to a DataFrame & save entry
        final_entry_df = pd.DataFrame([result])

        # Check if final_entry_df is empty or contains only NA values before concatenating
        if not final_entry_df.empty and not final_entry_df.dropna(how="all").empty:
            final_results_df = pd.concat([final_results_df, final_entry_df], ignore_index=True)
        else:
            print("Skipping empty or all-NA DataFrame during concatenation.")

    current_datetime = datetime.now()
    timestamp = current_datetime.strftime("%Y%m%d_%H%M%S")

    # Sort results by lowest % move for target strike
    final_results_sorted_df = final_results_df.sort_values(by='Target Strike (% Under Close)', ascending=False)

    # Create a Results File using the timestamp
    results_filename = f"RESULTS_{timestamp}.csv"
    full_path = os.path.join(config.RESULTS_DATA_DIR, results_filename)

    final_results_sorted_df.to_csv(full_path, index=False)
