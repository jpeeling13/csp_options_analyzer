import numpy as np

def set_sde_metadata(sde):
    # set general metadata fields
    sde.data_start_date = sde.df_weekly.iloc[0]['Date'].date()
    sde.data_end_date = sde.df_weekly.iloc[-1]['Date'].date()
    sde.total_weeks = sde.df_weekly.shape[0]
    sde.avg_weekly_return = np.round(sde.df_weekly['Weekly Return'].mean(), 2)

    # set lowest move fields
    lowest_move_index = sde.df_weekly['Weekly Return'].idxmin()
    sde.lowest_move_date = sde.df_weekly.loc[lowest_move_index]['Date'].date()
    sde.lowest_move_close = np.round(sde.df_weekly.loc[lowest_move_index]['Close'], 2)
    sde.lowest_move_pct = np.round(sde.df_weekly.loc[lowest_move_index]['Weekly Return'], 3)

    # set last close fields
    sde.last_close_date = sde.df_daily.index[-1].date()
    sde.last_close_price = np.round(sde.df_daily.iloc[-1]['Close'], 2)