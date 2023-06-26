import pandas as pd


def parse_datetime(row):
    # Extract the date and time components from the row
    day = row['D']
    month = row['M']
    year = row['Yr']
    hour = row['HH']
    minute = row['MM']

    # Construct a string representing the datetime in a format recognized by pandas
    datetime_str = f"{year}-{month}-{day} {hour}:{minute}"

    # Parse the datetime string using pandas
    return pd.to_datetime(datetime_str, format='%Y-%m-%d %H:%M')


def my_f():
    # Read the CSV file
    df = pd.read_csv('/Users/munkhdelger/PycharmProjects/BDT_traffic/data/Automated_Traffic_Volume_Counts.csv')

    # Combine the date and time columns into a single string column
    df['timestamp'] = df['Yr'].astype(str) + '-' + df['M'].astype(str) + '-' + df['D'].astype(str) + ' ' + df[
        'HH'].astype(str) + ':' + df['MM'].astype(str)

    # Convert the combined column to datetime format
    df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y-%m-%d %H:%M')

    df = df.sort_values('timestamp')
    # Set the timestamp column as the index
    df.set_index('timestamp', inplace=True)
    # df = df[(df['Yr'] > 2019) & (df['Yr'] < 2021)]
    # Drop the unnecessary date and time columns
    df.drop(['D', 'M', 'Yr', 'HH', 'MM'], axis=1, inplace=True)

    df.to_csv('/Users/munkhdelger/PycharmProjects/BDT_traffic/data/ATVS_timeseries.csv')

    # # Group the data by timestamp and road, and sum the volumes
    # grouped_df = df.groupby(['timestamp', 'street'])['Vol'].sum().reset_index()
    # # grouped_df.set_index('timestamp', inplace=True)
    #
    # data = grouped_df.sort_values('timestamp')
    #
    # pivoted_data = data.pivot(index='timestamp', columns='street', values='Vol').reset_index()
    # # Save the DataFrame to a CSV file
    #
    # pivoted_data.to_csv('/Users/munkhdelger/PycharmProjects/BDT_traffic/data/ATVS_timeseries_pivot.csv')



    # # Group the data by timestamp and road, and sum the volumes
    # grouped_df = df.groupby(['timestamp', 'street'])['Vol'].sum().reset_index()
    # # grouped_df.set_index('timestamp', inplace=True)
    #
    # data = grouped_df.sort_values('timestamp')
    #
    # pivoted_data = data.pivot(index='timestamp', columns='street', values='Vol').reset_index()
    # # Save the DataFrame to a CSV file
    #
    # pivoted_data.to_csv('/Users/munkhdelger/PycharmProjects/BDT_traffic/data/ATVS_timeseries_pivot.csv')
    #

def main():
    my_f()
    df = pd.read_csv('/Users/munkhdelger/PycharmProjects/BDT_traffic/data/ATVS_timeseries.csv')

    # Convert 'timestamp' column to datetime type
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # Group by 'timestamp' and calculate the sum and count
    grouped_df = df.groupby('timestamp').agg({'Vol': ['sum', 'count']}).reset_index()

    # Rename the columns for clarity
    grouped_df.columns = ['timestamp', 'Vol_sum', 'Row_count']

    # Sort the dataframe by timestamp
    sorted_df = grouped_df.sort_values('timestamp')

    print(sorted_df)
    sorted_df.to_csv('/Users/munkhdelger/PycharmProjects/BDT_traffic/data/ATVS_timeseries_one.csv', index = False)

if __name__ == "__main__":
    main()
