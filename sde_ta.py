import numpy as np

def set_sde_ta_data(sde):
    # Calculate 200 Day MA
    sde.df_daily['200_MA'] = sde.df_daily['Close'].rolling(window=200).mean()
    sde.two_hundred_day_ma = np.round(sde.df_daily['200_MA'].iloc[-1], 2)

    # Calculate 50 Day MA
    sde.df_daily['50_MA'] = sde.df_daily['Close'].rolling(window=50).mean()
    sde.fifty_day_ma = np.round(sde.df_daily['50_MA'].iloc[-1], 2)

    # Calculate RSI
    sde.rsi = calculate_rsi(sde.df_daily)

    # Calculate MACD
    macd_data = calculate_macd(sde.df_daily)
    sde.macd_line = macd_data['MACD_Line']
    sde.macd_signal = macd_data['Signal_Line']
    sde.macd_histogram = macd_data['MACD_Histogram']

    # Calculate Bollinger Bands
    bollinger_data = calculate_bollinger_bands(sde.df_daily)
    sde.bollinger_upper = bollinger_data['Upper_Band']
    sde.bollinger_lower = bollinger_data['Lower_Band']
    sde.bollinger_middle = bollinger_data['Middle_Band']


# Calculate the RSI (Relative Strength Index) with a 14-day window
def calculate_rsi(df, window=14):
    delta = df['Close'].diff(1)

    # Separate gains and losses
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)

    # Calculate the exponential moving average (Wilder's method)
    avg_gain = gain.ewm(com=window - 1, adjust=False).mean()
    avg_loss = loss.ewm(com=window - 1, adjust=False).mean()

    # Calculate relative strength (RS)
    rs = avg_gain / avg_loss

    # Calculate RSI
    rsi = 100 - (100 / (1 + rs))

    return np.round(rsi.iloc[-1], 2)

def calculate_macd(df, short_window=12, long_window=26, signal_window=9):
    """
    Calculate the MACD, Signal Line, and MACD Histogram for the most recent date.

    Parameters:
    df (DataFrame): DataFrame containing historical stock data with 'Close' prices.
    short_window (int): The window for the short-term EMA (default 12).
    long_window (int): The window for the long-term EMA (default 26).
    signal_window (int): The window for the signal line EMA (default 9).

    Returns:
    dict: Dictionary containing the most recent MACD Line, Signal Line, and MACD Histogram.
    """
    # Calculate the short-term (12-period) EMA of the Close price
    df['EMA_12'] = df['Close'].ewm(span=short_window, adjust=False).mean()

    # Calculate the long-term (26-period) EMA of the Close price
    df['EMA_26'] = df['Close'].ewm(span=long_window, adjust=False).mean()

    # Calculate the MACD Line (12-period EMA - 26-period EMA)
    df['MACD_Line'] = df['EMA_12'] - df['EMA_26']

    # Calculate the Signal Line (9-period EMA of the MACD Line)
    df['Signal_Line'] = df['MACD_Line'].ewm(span=signal_window, adjust=False).mean()

    # Calculate the MACD Histogram (MACD Line - Signal Line)
    df['MACD_Histogram'] = df['MACD_Line'] - df['Signal_Line']

    # Get the most recent values (last row of the DataFrame)
    most_recent = df.iloc[-1]

    # Return only the most recent MACD values
    return {
        'MACD_Line': np.round(most_recent['MACD_Line'], 3),
        'Signal_Line': np.round(most_recent['Signal_Line'], 3),
        'MACD_Histogram': np.round(most_recent['MACD_Histogram'], 3)
    }


def calculate_bollinger_bands(df, window=20, num_std_dev=2):
    """
    Calculate the Bollinger Bands for the most recent date.

    Parameters:
    df (DataFrame): DataFrame containing historical stock data with 'Close' prices.
    window (int): The window size for the moving average (default 20).
    num_std_dev (int): The number of standard deviations for the bands (default 2).

    Returns:
    dict: Dictionary containing the most recent Bollinger Bands (Upper, Lower, Middle) and Close price.
    """
    # Calculate the 20-day Simple Moving Average (SMA) for the 'Close' prices
    df['SMA'] = df['Close'].rolling(window=window).mean()

    # Calculate the rolling standard deviation of the 'Close' prices
    df['STD_DEV'] = df['Close'].rolling(window=window).std(ddof=0)

    # Calculate the Upper and Lower Bollinger Bands
    df['Upper_Band'] = df['SMA'] + (num_std_dev * df['STD_DEV'])
    df['Lower_Band'] = df['SMA'] - (num_std_dev * df['STD_DEV'])

    # Get the most recent values (last row of the DataFrame)
    most_recent = df.iloc[-1]

    # Return only the most recent Bollinger Bands and close price
    return {
        'Upper_Band': np.round(most_recent['Upper_Band'], 2),
        'Lower_Band': np.round(most_recent['Lower_Band'], 2),
        'Middle_Band': np.round(most_recent['SMA'], 2)
    }