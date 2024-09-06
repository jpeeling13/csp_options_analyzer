import os
import numpy as np
from dotenv import load_dotenv

# Load the environment variables
load_dotenv()

STOCK_DATA_FILE_ENDING = os.getenv('STOCK_DATA_FILE_ENDING')
CSP_SAFETY_PCT = np.float64(os.getenv('CSP_SAFETY_PCT'))
TICKERS = os.getenv("TICKERS", "AAPL,AMZN,GOOG,META,SPY").split(",")
STOCK_DATA_DIR = os.getenv('STOCK_DATA_DIR')
RESULTS_DATA_DIR = os.getenv('RESULTS_DATA_DIR')
CASH_ON_HAND = np.float64(os.getenv('CASH_ON_HAND'))
