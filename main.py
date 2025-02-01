import yfinance as yf
import utilities as ut
import config
from stock_data_entry import StockDataEntry


# Create an empty DataFrame with the desired column names
sde_results = []
ut.create_data_dirs()

# Loop over each ticker symbol
for ticker in config.TICKERS:
    df = yf.download(ticker)

    if not df.empty:

        if config.DOWNLOAD_TICKER_DATA_TO_CSV:
            output_file = ut.download_stock_data_csv(ticker, df)

        # Resample data to weekly instead of daily
        df_weekly = ut.resample_data_to_weekly(df)

        # Calculate Weekly Analysis Data And get final results
        final_ticker_entry = StockDataEntry()
        final_ticker_entry.calculate_all_data_fields(ticker, df, df_weekly)

        # Add the Stock Data Entries to the final results array
        sde_results.append(final_ticker_entry)

        # Analyze Next Ticker
        print(f"{ticker}: CSP Analysis Complete\n\n")
    else:
        print(f"NO Data Found for Ticker: {ticker}")


print("-- ALL TICKERS: CSP ANALYSIS COMPLETED --")

# convert the array of stock data entries to a final dataframe and generate file
ut.generate_results_file(sde_results)