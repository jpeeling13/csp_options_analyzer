import numpy as np
import pandas as pd
import utilities as ut
import math

from csp_options_analyzer import config


class StockDataEntry:
    def __init__(self):
        self.ticker = None
        self.data_start_date = None
        self.data_end_date = None
        self.total_weeks = None
        self.avg_weekly_return = None
        self.lowest_move_date = None
        self.lowest_move_close = None
        self.lowest_move_pct = None
        self.last_close_date = None
        self.last_close_price = None
        self.two_hundred_day_ma = None
        self.fifty_day_ma = None
        self.rsi = None
        self.csp_safety_pct = None
        self.tgt_strike_pct = None
        self.tgt_strike_pct_hist_run_max = None
        self.tgt_strike_pct_hist_run_min = None
        self.tgt_strike_pct_hist_run_avg = None
        self.pct_chance_assigned = None
        self.tgt_strike = None
        self.option_strike_used = None
        self.option_expiry_date = None
        self.option_days_to_expiry = None
        self.option_last_price = None
        self.option_volume = None
        self.option_open_interest = None
        self.option_implied_vol = None
        self.option_delta = None
        self.option_theta = None
        self.option_gamma = None
        self.option_vega = None
        self.option_rho = None
        self.cash_on_hand = None
        self.max_contracts = None
        self.potential_profit = None

    @classmethod
    def new_dataframe(cls):
        # Get all attributes of the class but exclude methods and dunder (double underscore) attributes
        columns = [
            k for k, v in cls.__dict__.items()
            if not (k.startswith('__') and k.endswith('__')) and not callable(v) and k != 'new_dataframe'
        ]

        # Create an empty DataFrame with the filtered columns (attribute names)
        df = pd.DataFrame(columns=columns)

        return df

    def to_dict(self):
        # Filter out any attributes that start with "__"
        return {k: v for k, v in self.__dict__.items() if not k.startswith('__')}

    def _validate_fields(self):
        # Loop through the instance's dictionary to check if any field is None
        for field, value in self.__dict__.items():
            if value is None:
                raise ValueError(f"The field '{field}' is not set.")
        print("All fields are set correctly.")

    def calculate_weekly_data_points(self, ticker, df, df_weekly):
        # Add Weekly Return Data for each entry
        df_weekly['Weekly Return'] = df_weekly['Close'].pct_change(fill_method=None) * 100

        # ---------------------------------------------
        # -- CALCULATE ALL DATA FIELDS FOR THE ENTRY --
        # ---------------------------------------------

        # Set the ticker
        self.ticker = ticker
        self.data_start_date = df_weekly.iloc[0]['Date'].date()
        self.data_end_date = df_weekly.iloc[-1]['Date'].date()
        self.total_weeks = df_weekly.shape[0]
        self.avg_weekly_return = np.round(df_weekly['Weekly Return'].mean(), 2)

        # Calculate "Lowest Move Date", "Lowest Move Date", "Lowest Move %"
        lowest_move_index = df_weekly['Weekly Return'].idxmin()
        self.lowest_move_date = df_weekly.loc[lowest_move_index]['Date'].date()
        self.lowest_move_close = np.round(df_weekly.loc[lowest_move_index]['Close'], 2)
        self.lowest_move_pct = np.round(df_weekly.loc[lowest_move_index]['Weekly Return'], 3)

        self.last_close_date = df.iloc[-1]['Date'].date()
        self.last_close_price = np.round(df.iloc[-1]['Close'], 2)

        # Calculate 200 Day MA, 50 Day MA, and RSI
        df['200_MA'] = df['Close'].rolling(window=200).mean()
        self.two_hundred_day_ma = np.round(df['200_MA'].iloc[-1], 2)

        df['50_MA'] = df['Close'].rolling(window=50).mean()
        self.fifty_day_ma = np.round(df['50_MA'].iloc[-1], 2)

        self.rsi = ut.calculate_rsi(df)

        self.csp_safety_pct = config.CSP_SAFETY_PCT
        self.tgt_strike_pct, self.pct_chance_assigned = ut.calculate_target_pct_below_close(df_weekly)
        self.tgt_strike_pct_hist_run_max, self.tgt_strike_pct_hist_run_min, \
            self.tgt_strike_pct_hist_run_avg = ut.calculate_tgt_strike_pct_runs(df_weekly, self.tgt_strike_pct)

        # Calculate "Target Strike"
        csp_strike_precise = self.last_close_price * ((100 + self.tgt_strike_pct) / 100) * 2
        self.tgt_strike = math.floor(csp_strike_precise) / 2

        # Calculate Recommended Option Data
        ticker_options_data = ut.calculate_one_week_options_data(self.ticker, self.tgt_strike)
        self.max_contracts = np.floor(config.CASH_ON_HAND / (ticker_options_data["option_strike_used"] * 100))

        # Calculate Potential Profit
        potential_profit_float = np.round(self.max_contracts * ticker_options_data["option_last_price"] * 100, 2)
        self.potential_profit = f"${potential_profit_float:,.2f}"

        self.option_strike_used = ticker_options_data["option_strike_used"]
        self.option_expiry_date = ticker_options_data["option_expiry_date"]
        self.option_days_to_expiry = ticker_options_data["option_days_to_exp"]
        self.option_last_price = ticker_options_data["option_last_price"]
        self.option_volume = ticker_options_data["option_volume"]
        self.option_open_interest = ticker_options_data["option_open_interest"]
        self.option_implied_vol = ticker_options_data["option_impl_vol"]
        self.option_delta = ticker_options_data["option_delta"]
        self.option_theta = ticker_options_data["option_theta"]
        self.option_gamma = ticker_options_data["option_gamma"]
        self.option_vega = ticker_options_data["option_vega"]
        self.option_rho = ticker_options_data["option_rho"]
        self.cash_on_hand = config.CASH_ON_HAND

        self._validate_fields()

        return self
