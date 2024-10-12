import numpy as np
import pandas as pd
import utilities as ut
import math
import mibian
import yfinance as yf

from csp_options_analyzer import config


class StockDataEntry:
    def __init__(self):
        self.df_daily = None
        self.df_weekly = None
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
        self.macd_line = None
        self.macd_signal = None
        self.macd_histogram = None
        self.bollinger_upper = None
        self.bollinger_lower = None
        self.bollinger_middle = None
        self.csp_safety_pct = None
        self.tgt_strike_pct = None
        self.tgt_strike_pct_hist_run_max = None
        self.tgt_strike_pct_hist_run_min = None
        self.tgt_strike_pct_hist_run_avg = None
        self.pct_chance_assigned = None
        self.tgt_strike = None
        self.csp_strike_used = None
        self.csp_expiry_date = None
        self.csp_days_to_expiry = None
        self.csp_last_price = None
        self.csp_volume = None
        self.csp_open_interest = None
        self.csp_implied_vol = None
        self.csp_delta = None
        self.csp_theta = None
        self.csp_gamma = None
        self.csp_vega = None
        self.csp_rho = None
        self.cash_on_hand = None
        self.max_contracts = None
        self.potential_profit = None
        self.ind_tgt_strike_pct = None
        self.ind_tgt_strike_pct_occurs = None
        self.ind_tgt_strike_pct_current_run = None

    @classmethod
    def new_dataframe(cls):
        # Create an instance of the class
        instance = cls()

        # Get all instance attributes but exclude 'df_' prefixed attributes and dunder attributes
        columns = [
            k for k, v in instance.__dict__.items()
            if not k.startswith('df_') and not k.startswith('__') and not callable(v) and k != 'new_dataframe'
        ]

        # Create an empty DataFrame with the filtered columns (attribute names)
        df = pd.DataFrame(columns=columns)

        return df

    def to_dict(self):
        # Include attributes that don't start with '__' and don't start with 'df_'
        return {k: v for k, v in self.__dict__.items() if not k.startswith('__') and not k.startswith('df_')}

    def _validate_fields(self):
        # Loop through the instance's dictionary to check if any field is None
        for field, value in self.__dict__.items():
            if value is None:
                raise ValueError(f"The field '{field}' is not set.")

    # Calculate all fields for the new Stock Data Entry
    def calculate_all_data_fields(self, ticker, df, df_weekly):

        self.ticker = ticker
        self.df_daily = df

        df_weekly['Weekly Return'] = df_weekly['Close'].pct_change(fill_method=None) * 100
        self.df_weekly = df_weekly

        # ---------------------------------------------
        # -- CALCULATE ALL DATA FIELDS FOR THE ENTRY --
        # ---------------------------------------------

        self.set_sde_metadata()
        self.set_sde_lowest_move_last_close_data()
        self.set_sde_ta_data()
        self.set_sde_target_csp_metadata()
        self.set_sde_target_csp_options_data()

        # Calculate Max Contracts & Potential Profit
        self.cash_on_hand = config.CASH_ON_HAND
        self.max_contracts = np.floor(self.cash_on_hand / (self.csp_strike_used * 100))

        potential_profit_float = np.round(self.max_contracts * self.csp_last_price * 100, 2)
        self.potential_profit = f"${potential_profit_float:,.2f}"

        # Calculate the special indicators to determine whether to recommend the option or not
        self.set_ind_tgt_strike_pct()

        # Perform safety check to ensure all fields are set
        self._validate_fields()

    def set_sde_metadata(self):
        self.data_start_date = self.df_weekly.iloc[0]['Date'].date()
        self.data_end_date = self.df_weekly.iloc[-1]['Date'].date()
        self.total_weeks = self.df_weekly.shape[0]
        self.avg_weekly_return = np.round(self.df_weekly['Weekly Return'].mean(), 2)

    def set_sde_lowest_move_last_close_data(self):
        lowest_move_index = self.df_weekly['Weekly Return'].idxmin()
        self.lowest_move_date = self.df_weekly.loc[lowest_move_index]['Date'].date()
        self.lowest_move_close = np.round(self.df_weekly.loc[lowest_move_index]['Close'], 2)
        self.lowest_move_pct = np.round(self.df_weekly.loc[lowest_move_index]['Weekly Return'], 3)
        self.last_close_date = self.df_daily.index[-1].date()
        self.last_close_price = np.round(self.df_daily.iloc[-1]['Close'], 2)

    def set_sde_ta_data(self):
        # Calculate 200 Day MA
        self.df_daily['200_MA'] = self.df_daily['Close'].rolling(window=200).mean()
        self.two_hundred_day_ma = np.round(self.df_daily['200_MA'].iloc[-1], 2)

        # Calculate 50 Day MA
        self.df_daily['50_MA'] = self.df_daily['Close'].rolling(window=50).mean()
        self.fifty_day_ma = np.round(self.df_daily['50_MA'].iloc[-1], 2)

        # Calculate RSI
        self.rsi = ut.calculate_rsi(self.df_daily)

        # Calculate MACD
        macd_data = ut.calculate_macd(self.df_daily)
        self.macd_line = macd_data['MACD_Line']
        self.macd_signal = macd_data['Signal_Line']
        self.macd_histogram = macd_data['MACD_Histogram']

        # Calculate Bollinger Bands
        bollinger_data = ut.calculate_bollinger_bands(self.df_daily)
        self.bollinger_upper = bollinger_data['Upper_Band']
        self.bollinger_lower = bollinger_data['Lower_Band']
        self.bollinger_middle = bollinger_data['Middle_Band']

    def set_sde_target_csp_metadata(self):
        self.csp_safety_pct = config.CSP_SAFETY_PCT
        self.tgt_strike_pct, self.pct_chance_assigned = ut.calculate_tgt_strike_pct_data(self.df_weekly)
        self.tgt_strike_pct_hist_run_max, self.tgt_strike_pct_hist_run_min, \
            self.tgt_strike_pct_hist_run_avg = ut.calculate_tgt_strike_pct_runs(self.df_weekly, self.tgt_strike_pct)

        # Calculate "Target Strike"
        csp_strike_precise = self.last_close_price * ((100 + self.tgt_strike_pct) / 100) * 2
        self.tgt_strike = math.floor(csp_strike_precise) / 2

    def set_sde_target_csp_options_data(self, timeframe=5):
        # Fetch the ticker data
        ticker_data = yf.Ticker(self.ticker)

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
            puts = options_chain.puts
            put_option = puts[puts['strike'] == self.tgt_strike]

            # If no exact match, find the closest strike price
            if put_option.empty:
                available_strikes = puts['strike'].values
                closest_strike = min(available_strikes, key=lambda x: abs(x - self.tgt_strike))

                put_option = puts[puts['strike'] == closest_strike]

            # Get the first matching option (there should be only one, but just in case)
            put_option = put_option.iloc[0]

            # Extract relevant data from the option
            self.csp_strike_used = put_option["strike"]
            self.csp_expiry_date = option_selected_expiry_date
            self.csp_days_to_expiry = (pd.to_datetime(option_selected_expiry_date) - today).days
            self.csp_last_price = put_option['lastPrice']
            self.csp_volume = put_option["volume"]
            self.csp_open_interest = put_option["openInterest"]
            self.csp_implied_vol = np.round(put_option['impliedVolatility'] * 100, 3)  # mibian expects percentage

            # Get the current 3-month Treasury bill rate
            tbill = yf.Ticker("^IRX")
            tbill_data = tbill.history(period="1d")
            current_rate = tbill_data["Close"].iloc[0]

            # Use mibian to calculate Greeks
            bs = mibian.BS([stock_price, self.csp_strike_used, current_rate, self.csp_days_to_expiry],
                           volatility=self.csp_implied_vol, putPrice=self.csp_last_price)

            # Save off values to dictionary
            self.csp_delta = np.round(bs.putDelta, 3)
            self.csp_theta = np.round(bs.putTheta, 3)
            self.csp_gamma = np.round(bs.gamma, 3)
            self.csp_vega = np.round(bs.vega, 3)
            self.csp_rho = np.round(bs.putRho, 3)

        else:
            print("No expiration date found that is 7 or more days out.")

    # INDICATOR: Check if ticker has closed below tgt_strike_pct within most recent tgt_strike_pct_hist_run_avg
    # The rationale for this indicator is that if the stock has closed below the tgt_strike_pct within the last
    # average number of weeks it takes to drop below that pct, then it's unlikely to happen again. Conversely, if it
    # hasn't dropped below the pct yet, then it's likely to happen soon.
    def set_ind_tgt_strike_pct(self):

        # Ensure the DataFrame is sorted by date
        self.df_weekly = self.df_weekly.sort_values(by='Date')
        avg_run_weeks = round(self.tgt_strike_pct_hist_run_avg)

        # Filter for the last y weeks
        df_last_avg_run_weeks = self.df_weekly.tail(avg_run_weeks)

        # Calculate the percentage change compared to the previous week's close
        df_last_avg_run_weeks.loc[:, 'pct_change'] = np.round(df_last_avg_run_weeks['Close'].pct_change() * 100, 2)

        # Find the rows where the percentage change is below the threshold (x_pct)
        closed_below_threshold = df_last_avg_run_weeks[df_last_avg_run_weeks['pct_change'] < self.tgt_strike_pct]

        # Always calculate the last occurrence in the entire dataset
        self.df_weekly.loc[:, 'pct_change'] = np.round(self.df_weekly['Close'].pct_change() * 100, 2)  # Calculate for entire dataset
        full_closed_below_threshold = self.df_weekly[self.df_weekly['pct_change'] < self.tgt_strike_pct]
        weeks_since_last_occurrence = -1

        if not full_closed_below_threshold.empty:
            print("got in here...")
            last_occurrence_date = full_closed_below_threshold.iloc[-1]['Date']
            most_recent_date = self.df_weekly['Date'].max()

            # Calculate the time difference in weeks
            weeks_since_last_occurrence = (most_recent_date - last_occurrence_date).days / 7
            weeks_since_last_occurrence = int(weeks_since_last_occurrence)

        # Check if there are any such occurrences
        if not closed_below_threshold.empty:
            self.ind_tgt_strike_pct = True
            self.ind_tgt_strike_pct_occurs = len(closed_below_threshold)
            self.ind_tgt_strike_pct_current_run = weeks_since_last_occurrence

        else:
            self.ind_tgt_strike_pct = False
            self.ind_tgt_strike_pct_occurs = 0
            self.ind_tgt_strike_pct_current_run = weeks_since_last_occurrence

