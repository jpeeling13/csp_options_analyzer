import numpy as np

# INDICATOR: Check if ticker has closed below tgt_strike_pct within most recent tgt_strike_pct_hist_run_avg
# The rationale for this indicator is that if the stock has closed below the tgt_strike_pct within the last
# average number of weeks it takes to drop below that pct, then it's unlikely to happen again. Conversely, if it
# hasn't dropped below the pct yet, then it's likely to happen soon.
def set_ind_tgt_strike_pct(sde):

    # Ensure the DataFrame is sorted by date
    sde.df_weekly = sde.df_weekly.sort_values(by='Date')
    avg_run_weeks = round(sde.tgt_strike_pct_hist_run_avg)

    # Filter for the last y weeks
    df_last_avg_run_weeks = sde.df_weekly.tail(avg_run_weeks)

    # Calculate the percentage change compared to the previous week's close
    df_last_avg_run_weeks.loc[:, 'pct_change'] = np.round(df_last_avg_run_weeks['Close'].pct_change() * 100, 2)

    # Find the rows where the percentage change is below the threshold (x_pct)
    closed_below_threshold = df_last_avg_run_weeks[df_last_avg_run_weeks['pct_change'] < sde.tgt_strike_pct]

    # Always calculate the last occurrence in the entire dataset
    sde.df_weekly.loc[:, 'pct_change'] = np.round(sde.df_weekly['Close'].pct_change() * 100, 2)  # Calculate for entire dataset
    full_closed_below_threshold = sde.df_weekly[sde.df_weekly['pct_change'] < sde.tgt_strike_pct]
    weeks_since_last_occurrence = -1

    if not full_closed_below_threshold.empty:
        print("got in here...")
        last_occurrence_date = full_closed_below_threshold.iloc[-1]['Date']
        most_recent_date = sde.df_weekly['Date'].max()

        # Calculate the time difference in weeks
        weeks_since_last_occurrence = (most_recent_date - last_occurrence_date).days / 7
        weeks_since_last_occurrence = int(weeks_since_last_occurrence)

    # Check if there are any such occurrences
    if not closed_below_threshold.empty:
        sde.ind_tgt_strike_pct = True
        sde.ind_tgt_strike_pct_occurs = len(closed_below_threshold)
        sde.ind_tgt_strike_pct_current_run = weeks_since_last_occurrence

    else:
        sde.ind_tgt_strike_pct = False
        sde.ind_tgt_strike_pct_occurs = 0
        sde.ind_tgt_strike_pct_current_run = weeks_since_last_occurrence