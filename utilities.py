import pandas as pd
import numpy as np
import math


def resample_data_to_weekly(df):
    # Resample data to weekly frequency, taking the last close price of each week
    df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d')

    # Resample using the Date column and take the last entry of each week
    df_weekly = df.resample('W', on='Date').last()

    # Reset the index to make Date a regular column again
    df_weekly.reset_index(inplace=True)

    return df_weekly


def find_lowest_weekly_move_below_threshold(threshold_pct, df_weekly):
    lowest_weekly_move_int = int(df_weekly['Weekly Return'].min())

    for num in np.arange(-.5, lowest_weekly_move_int, -0.5):

        # Find the weeks with a weekly return less than the negative threshold
        weeks_with_large_negative_movements = df_weekly[df_weekly['Weekly Return'] < num]

        # Calculate Total number of weeks with a return less than the negative threshold
        total_weeks = df_weekly.shape[0]
        total_negative_movement_weeks = weeks_with_large_negative_movements.shape[0]
        percent_occurred = total_negative_movement_weeks / total_weeks * 100

        # Save off Results Data for Recommended Strike
        if percent_occurred < threshold_pct:
            print(
                f"Target CSP Percentage that happens less than {threshold_pct} of the time: {num}, {percent_occurred}")

            return num, percent_occurred

    print("Could not calculate a weekly move % that happens below the threshold")
    return "ERROR"


def calculate_weekly_data_points(ticker, df, df_weekly, threshold_pct):

    # Calculate Weekly Return
    df_weekly['Weekly Return'] = df_weekly['Close'].pct_change(fill_method=None) * 100

    # Find Start Date, End Date & Total # of Weeks In The Dataset
    RESULT_TOTAL_WEEKS = df_weekly.shape[0]
    RESULT_DATA_START_DATE = df_weekly.iloc[0]['Date'].date()
    RESULT_DATA_END_DATE = df_weekly.iloc[-1]['Date'].date()

    # Calculate the average weekly return
    RESULT_AVG_WEEKLY_RETURN = df_weekly['Weekly Return'].mean()

    # Calculate the week with the biggest move down
    RESULT_LOWEST_MOVE_DATE = df_weekly.loc[df_weekly['Weekly Return'].idxmin()]['Date'].date()
    RESULT_LOWEST_MOVE_CLOSE = df_weekly.loc[df_weekly['Weekly Return'].idxmin()]['Close']
    RESULT_LOWEST_MOVE_PCT = np.trunc(df_weekly.loc[df_weekly['Weekly Return'].idxmin()]['Weekly Return'] * 1000) / 1000

    # Calculate smallest % move down that occurs below threshold and translate to recommended weekly csp strike price
    RESULT_TARGET_PCT_BELOW_THRESHOLD, \
    RESULT_PCT_BELOW_THRESHOLD_OCCURRED = find_lowest_weekly_move_below_threshold(threshold_pct, df_weekly)
    RESULT_LAST_CLOSE_DATE = df.iloc[-1]['Date'].date()
    RESULT_LAST_CLOSE_PRICE = df.iloc[-1]['Close']
    RESULT_TARGET_CSP_STRIKE = math.floor(RESULT_LAST_CLOSE_PRICE * ((100 + RESULT_TARGET_PCT_BELOW_THRESHOLD) / 100) * 2) / 2

    print(f"Start: {RESULT_DATA_START_DATE}")
    print(f"End: {RESULT_DATA_END_DATE}")
    print(f"Total Weeks: {RESULT_TOTAL_WEEKS}")
    print(f"Average Weekly Return: {RESULT_AVG_WEEKLY_RETURN}")
    print(
        f"Lowest Weekly Move: {RESULT_LOWEST_MOVE_DATE}, "
        f"{RESULT_LOWEST_MOVE_CLOSE}, {RESULT_LOWEST_MOVE_PCT: .6f}")

    results_entry = {
        "Ticker": ticker,
        "Start Date": RESULT_DATA_START_DATE,
        "End Date": RESULT_DATA_END_DATE,
        "Weeks Analyzed": RESULT_TOTAL_WEEKS,
        "Avg Weekly Return": RESULT_AVG_WEEKLY_RETURN,
        "Lowest Move Date": RESULT_LOWEST_MOVE_DATE,
        "Lowest Move Close": RESULT_LOWEST_MOVE_CLOSE,
        "Lowest Move %": RESULT_LOWEST_MOVE_PCT,
        "Safety % Set": threshold_pct,
        "Target % Below Strike": RESULT_TARGET_PCT_BELOW_THRESHOLD,
        "% Occurred": RESULT_PCT_BELOW_THRESHOLD_OCCURRED,
        "Last Close Date": RESULT_LAST_CLOSE_DATE,
        "Last Close": RESULT_LAST_CLOSE_PRICE,
        "Recommended Strike": RESULT_TARGET_CSP_STRIKE
    }

    return results_entry
