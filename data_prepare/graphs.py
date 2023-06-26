import pandas as pd

# Load the data
df = pd.read_csv('/Users/munkhdelger/PycharmProjects/BDT_traffic/data/ATVS_timeseries.csv')

# Convert the 'timestamp' column to datetime
df['timestamp'] = pd.to_datetime(df['timestamp'])

# Extract the year from the 'timestamp' column
df['year'] = df['timestamp'].dt.year

# Count the occurrences of each year
year_counts = df['year'].value_counts()

# Print the count of time series data for each year
for year, count in year_counts.items():
    print(f"Year: {year}, Count: {count}")
