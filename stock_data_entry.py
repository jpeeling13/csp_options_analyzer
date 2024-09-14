import pandas as pd


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
        self.csp_safety_pct = None
        self.target_strike_pct_under_close = None
        self.pct_chance_assigned = None
        self.target_strike = None
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

    def to_dict(self):
        # Filter out any attributes that start with "__"
        return {k: v for k, v in self.__dict__.items() if not k.startswith('__')}

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
