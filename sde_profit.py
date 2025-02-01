import config
import numpy as np

def set_sde_profit_data(sde):
    sde.cash_on_hand = config.CASH_ON_HAND
    sde.max_contracts = np.floor(sde.cash_on_hand / (sde.csp_strike_used * 100))

    potential_profit_float = np.round(sde.max_contracts * sde.csp_last_price * 100, 2)
    sde.potential_profit = f"${potential_profit_float:,.2f}"