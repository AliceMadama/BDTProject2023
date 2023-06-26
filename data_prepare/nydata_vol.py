import pandas as pd

# Read a CSV file into a DataFrame
df = pd.read_csv('/Users/munkhdelger/PycharmProjects/BDT_traffic/data/Traffic_Volume_Counts.csv')
print(df.shape)
nan_count = df.isna().sum()
print(sum(nan_count))
# Reshape the DataFrame by pivoting
df_pivoted = df.melt(id_vars=['ID', 'SegmentID', 'Roadway Name', 'From', 'To', 'Direction', 'Date'],
                     var_name='Time_Period', value_name='Value')


print(df_pivoted.shape)
nan_count = df_pivoted.isna().sum()
print(sum(nan_count))
# Split the 'Time_Period' column into separate start time and end time columns
df_pivoted[['Start_Time', 'End_Time']] = df_pivoted['Time_Period'].str.split('-', expand=True)

# Create the 'Timestamp' column by combining the 'Date', 'Start_Time', 'End_Time', and 'AM/PM' columns
df_pivoted['Timestamp'] = pd.to_datetime(df_pivoted['Date'] + ' ' + df_pivoted['Start_Time'] + df_pivoted['End_Time'].str[-2:])

# Drop the original 'Date', 'Time_Period', 'Start_Time', and 'End_Time' columns
df_pivoted = df_pivoted.drop(['Date', 'Time_Period', 'Start_Time', 'End_Time'], axis=1)

# Sort the DataFrame by 'Timestamp'
df_pivoted = df_pivoted.sort_values(['Timestamp'])

# Optionally, you can set the 'Timestamp' column as the index
df_pivoted.set_index('Timestamp', inplace=True)

# Print the pivoted DataFrame
print(df_pivoted.shape)
nan_count = df_pivoted.isna().sum()
print(sum(nan_count))

# Save the DataFrame to a CSV file
df_pivoted.to_csv('/Users/munkhdelger/PycharmProjects/BDT_traffic/data/Traffic_Volume_Counts_pivoted.csv', index=True)
