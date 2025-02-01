import yfinance as yf
import numpy as np
import pandas as pd
import mibian

def set_sde_target_csp_options_data(sde, timeframe=5):
    # Fetch the ticker data
    ticker_data = yf.Ticker(sde.ticker)

    # Get the current stock price
    stock_price = np.round(ticker_data.history(period="1d")["Close"].iloc[0], 3)

    # Get options expiration dates
    options_expiration_dates = ticker_data.options

    # Find the expiration date that is at least 6 calendar days from today
    today = pd.Timestamp.today().normalize()
    x_days_out = today + pd.Timedelta(days=timeframe)

    # Find the first expiration date that is 7 or more days away
    option_selected_expiry_date = None
    for expiration_date in options_expiration_dates:
        if pd.Timestamp(expiration_date) >= x_days_out:
            option_selected_expiry_date = expiration_date
            break

    if option_selected_expiry_date:

        # Fetch the options chain for the selected expiration date
        options_chain = ticker_data.option_chain(option_selected_expiry_date)

        #calculate Max Pain for the Expiration Date
        sde.max_pain = set_max_pain(options_chain)

        # Calculate CSP Options Data
        puts = options_chain.puts
        put_option = puts[puts['strike'] == sde.tgt_strike]

        # If no exact match, find the closest strike price
        if put_option.empty:
            available_strikes = puts['strike'].values
            closest_strike = min(available_strikes, key=lambda x: abs(x - sde.tgt_strike))

            put_option = puts[puts['strike'] == closest_strike]

        # Get the first matching option (there should be only one, but just in case)
        put_option = put_option.iloc[0]

        # Extract relevant data from the option
        sde.csp_strike_used = put_option["strike"]
        sde.csp_expiry_date = option_selected_expiry_date
        sde.csp_days_to_expiry = (pd.to_datetime(option_selected_expiry_date) - today).days
        sde.csp_last_price = put_option['lastPrice']
        sde.csp_volume = put_option["volume"]
        sde.csp_open_interest = put_option["openInterest"]
        sde.csp_implied_vol = np.round(put_option['impliedVolatility'] * 100, 3)  # mibian expects percentage

        # Get the current 3-month Treasury bill rate
        tbill = yf.Ticker("^IRX")
        tbill_data = tbill.history(period="1d")
        current_rate = tbill_data["Close"].iloc[0]

        # Use mibian to calculate Greeks
        bs = mibian.BS([stock_price, sde.csp_strike_used, current_rate, sde.csp_days_to_expiry],
                        volatility=sde.csp_implied_vol, putPrice=sde.csp_last_price)

        # Save off values to dictionary
        sde.csp_delta = np.round(bs.putDelta, 3)
        sde.csp_theta = np.round(bs.putTheta, 3)
        sde.csp_gamma = np.round(bs.gamma, 3)
        sde.csp_vega = np.round(bs.vega, 3)
        sde.csp_rho = np.round(bs.putRho, 3)

    else:
        print("No expiration date found that is 7 or more days out.")

"""
Calculates the max pain price for a given option chain.
option_chain: DataFrame containing ['strike', 'openInterest', 'type']
"""
def set_max_pain(options_chain):
    calls = options_chain.calls[['strike', 'openInterest']].copy()
    calls['type'] = 'call'
    puts = options_chain.puts[['strike', 'openInterest']].copy()
    puts['type'] = 'put'

    # Merge calls and puts
    option_chain = pd.concat([calls, puts])

    # Calculate max pain for this expiration

    strikes = option_chain['strike'].unique()
    total_pain = []

    for strike in strikes:
        # Calculate total loss for calls
        call_loss = ((strike - option_chain[option_chain['type'] == 'call']['strike']).clip(lower=0) *
                     option_chain[option_chain['type'] == 'call']['openInterest']).sum()

        # Calculate total loss for puts
        put_loss = ((option_chain[option_chain['type'] == 'put']['strike'] - strike).clip(lower=0) *
                    option_chain[option_chain['type'] == 'put']['openInterest']).sum()

        # Total loss at this strike
        total_pain.append((strike, call_loss + put_loss))

    # Find the strike price with the lowest total pain
    max_pain_price = min(total_pain, key=lambda x: x[1])[0]

    return np.round(max_pain_price, 2)