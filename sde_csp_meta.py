import config
import numpy as np
import math

def set_sde_target_csp_metadata(sde):
    sde.csp_safety_pct = config.CSP_SAFETY_PCT
    sde.tgt_strike_pct, sde.pct_chance_assigned = calculate_tgt_strike_pct_data(sde.df_weekly)
    sde.tgt_strike_pct_hist_run_max, sde.tgt_strike_pct_hist_run_min, \
        sde.tgt_strike_pct_hist_run_avg = calculate_tgt_strike_pct_runs(sde.df_weekly, sde.tgt_strike_pct)

    # Calculate "Target Strike"
    csp_strike_precise = sde.last_close_price * ((100 + sde.tgt_strike_pct) / 100) * 2
    sde.tgt_strike = math.floor(csp_strike_precise) / 2


def calculate_tgt_strike_pct_data(df_weekly):
    lowest_weekly_move_int = int(df_weekly['Weekly Return'].min())

    for num in np.arange(-.5, lowest_weekly_move_int, -0.5):

        # Find the weeks with a weekly return less than or equal to the negative threshold
        total_negative_movement_weeks = df_weekly[df_weekly['Weekly Return'] <= num]

        # Calculate Total number of weeks with a return less than the negative threshold
        total_weeks = df_weekly.shape[0]
        total_negative_movement_weeks = total_negative_movement_weeks.shape[0]
        percent_occurred = total_negative_movement_weeks / total_weeks * 100

        # Save off Results Data for Recommended Strike
        if percent_occurred < config.CSP_SAFETY_PCT:
            percent_occurred = np.round(percent_occurred, 2)
            return num, percent_occurred

    print("Could not calculate a weekly move % that happens below the threshold")
    return "ERROR"

def calculate_tgt_strike_pct_runs(df_weekly, tgt_strike_pct):
    # Calculate the percentage change in Close price from the previous week
    df_weekly['pct_change'] = df_weekly['Close'].pct_change()

    # Flag weeks when the stock closed above the tgt_strike_pct move
    df_weekly['above_tgt_pct_move'] = df_weekly['pct_change'] >= tgt_strike_pct / 100

    # Identify consecutive runs of 'above_tgt_pct_move'
    # Create a column that marks the start of a new run by comparing it to the previous row
    df_weekly['run_change'] = df_weekly['above_tgt_pct_move'].ne(df_weekly['above_tgt_pct_move'].shift()).cumsum()

    # Filter out only the groups where the stock was above the tgt_strike_pct move
    positive_runs = df_weekly[df_weekly['above_tgt_pct_move']]

    # Group by run_change and count the length of each run
    run_lengths = positive_runs.groupby('run_change').size()

    # Calculate statistics
    longest_run = run_lengths.max()
    shortest_run = run_lengths.min()
    average_run = np.round(run_lengths.mean(), 2)

    return longest_run, shortest_run, average_run