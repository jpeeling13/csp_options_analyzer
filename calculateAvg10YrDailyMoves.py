import pandas as pd

# Load the data (assuming you have downloaded it in CSV format with 'Date' and 'Close' columns)
df = pd.read_csv('~/downloads/sp500-10-year-daily-chart.csv')

# Ensure the Date column is parsed as datetime
df['DATE'] = pd.to_datetime(df['DATE'], format='%m/%d/%y')

# Calculate the daily return
df['CLOSE'] = df['CLOSE'].pct_change(fill_method=None) * 100

# Calculate the average daily return
average_daily_return = df['CLOSE'].mean()

# Calculate the average daily movement (absolute value of returns)
average_daily_movement = df['CLOSE'].abs().mean()

# Find the total number of days in the dataset
total_days = df.shape[0]

# Find the days with more than x% daily movement
x_movement=1.5
days_with_large_movements = df[df['CLOSE'].abs() > x_movement].sort_values(by='DATE')

# Total number of days with more than x% movement
total_large_movement_days = days_with_large_movements.shape[0]

# Output the results
print(f"Total Number of Days: {total_days}")
print(f"Average Daily Return: {average_daily_return}%")
print(f"Average Daily Movement: {average_daily_movement}%")
print(f"Total Number of Days with More Than {x_movement} Movement: {total_large_movement_days}")
print(f"\nDays with More Than {x_movement} Movement in Chronological Order:")
print(days_with_large_movements[['DATE', 'CLOSE']])