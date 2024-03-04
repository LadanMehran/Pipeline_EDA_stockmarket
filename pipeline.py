import pandas as pd
import requests
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


# Extract
# fetching intraday data for IBM, MSFT, INTC, and ORCL stocks from alpha vantage API

def get_stock_data(stock_symbols, api_key):
    stock_dfs = {}

    for symbol in stock_symbols:
        api_url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={symbol}&interval=5min&apikey={api_key}'
        res = requests.get(api_url)
        data = res.json()

        time_series_data = data.get('Time Series (5min)')

        if time_series_data:
            df = pd.DataFrame.from_dict(time_series_data, orient='index')
            df.reset_index(inplace=True)
            df.rename(columns={'index': 'Datetime'}, inplace=True)

            stock_dfs[symbol] = df
        else:
            print(f"No data available for {symbol}")

    return stock_dfs

stock_symbols = ['IBM', 'MSFT', 'INTC', 'ORCL']
stock_data = get_stock_data(stock_symbols, api_key)

# Transform


# changing dtype (all the data fetched from the API are strings, changing dates to datetime, OHLC prices to float and volume to int)

def preprocess_stock_data(df_list):
    for df in df_list:
        df['Datetime'] = pd.to_datetime(df['Datetime'])

        ohlc_columns = ['1. open', '2. high', '3. low', '4. close']
        df[ohlc_columns] = df[ohlc_columns].astype(float)

        df['5. volume'] = df['5. volume'].str.replace(',', '').astype(int)

        # Remove the numbers in column names 
        df.columns = [col.split('. ')[-1] for col in df.columns]
        
preprocess_stock_data(stock_data)

# Checking for null/missing values (there were no missing values in the data)

def check_null_values(df_list):
    for symbol, df in zip(['IBM', 'MSFT', 'INTC', 'ORCL'], df_list):
        if df.isnull().values.any():
            print(f"DataFrame {symbol} has null values.")
        else:
            print(f"DataFrame {symbol} has no null values.")
            
check_null_values(stock_data)
# Calculate Returns

def calculate_daily_return(df_list):
    for df in df_list:
        df['daily_return'] = df['close'].pct_change()
        
calculate_daily_return(stock_data)
# calculate the avg volume over the past 50 minutes (rolling_avg): 

def calculate_avg_volume(df_list, window_size=10):
    for df in df_list:
        df['avg_volume'] = df['volume'].rolling(window=window_size).mean()
        
calculate_avg_volume(stock_data)
        
# Load
# loading the data for each stock df in a separate table in snowflake

stock_dfs = {'IBM': ibm_df, 'MSFT': msft_df, 'INTC': intc_df, 'ORCL': orcl_df}

conn = snowflake.connector.connect(
    user="",
    password="",
    account="",
    database="intraday_stock",
    schema="processed_data"
)

for symbol, df in stock_dfs.items():

    table_name = f"table_{symbol}"  
    write_pandas(conn, df, table_name, auto_create_table=True, use_logical_type = True)