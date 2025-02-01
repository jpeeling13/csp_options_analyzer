import numpy as np
import pandas as pd
import sde_meta
import sde_ta
import sde_csp_meta
import sde_csp_options
import sde_indicators
import sde_profit


class StockDataEntry:
    def __init__(self):

        # METADATA FIELDS
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

        # TA DATA FIELDS
        self.two_hundred_day_ma = None
        self.fifty_day_ma = None
        self.rsi = None
        self.macd_line = None
        self.macd_signal = None
        self.macd_histogram = None
        self.bollinger_upper = None
        self.bollinger_lower = None
        self.bollinger_middle = None

        # TARGET CSP METADATA
        self.csp_safety_pct = None
        self.tgt_strike_pct = None
        self.tgt_strike_pct_hist_run_max = None
        self.tgt_strike_pct_hist_run_min = None
        self.tgt_strike_pct_hist_run_avg = None
        self.pct_chance_assigned = None
        self.tgt_strike = None

        # TARGET CSP OPTIONS DATA
        self.csp_strike_used = None
        self.csp_expiry_date = None
        self.csp_days_to_expiry = None
        self.max_pain = None
        self.csp_last_price = None
        self.csp_volume = None
        self.csp_open_interest = None
        self.csp_implied_vol = None
        self.csp_delta = None
        self.csp_theta = None
        self.csp_gamma = None
        self.csp_vega = None
        self.csp_rho = None

        # POTENTIAL PROFIT FIELDS
        self.cash_on_hand = None
        self.max_contracts = None
        self.potential_profit = None

        # SPECIAL INDICATORS FIELDS
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

        sde_meta.set_sde_metadata(self)
        sde_ta.set_sde_ta_data(self)
        sde_csp_meta.set_sde_target_csp_metadata(self)
        sde_csp_options.set_sde_target_csp_options_data(self)
        sde_profit.set_sde_profit_data(self)
        sde_indicators.set_ind_tgt_strike_pct(self)

        # Perform safety check to ensure all fields are set
        self._validate_fields()