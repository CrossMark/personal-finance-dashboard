# -*- coding: utf-8 -*-
"""
This script can be used to scrape cryptocurrency prices (in EUR) from Bitvavo.
"""

import pandas as pd
import requests
import datetime
import sqlalchemy as sql

def request_prices(market_list):
    """
    Get the current crypto prices from the cryptocurrencies that are given in the ``market_list``.
    
    Sends a GET request to the Bitvavo ticker endpoint.
    From the response, the price is saved. 
    Also the scrape time, almost equal to the request time is saved.
    This information is returned in a DataFrame.

    Parameters
    ----------
    market_list : list of str
        List of market codes of the cryptocurrenties to scrape.

    Returns
    -------
    df_crypto : DataFrame
        DataFrame with market code, price and scrape time in local time.

    """
    base_url = 'https://api.bitvavo.com/v2/ticker/price'
    response_list = []
    
    for market in market_list:
        request_url = base_url + '?market=' + market
        response = requests.get(request_url)
        response_dict = response.json()
        response_dict['timestamp'] = datetime.datetime.now()
        response_list.append(response_dict)
    
    df_crypto = pd.DataFrame.from_records(response_list)
    return df_crypto        

def create_db_engine(connection_string='sqlite:///database.db'):
    """Return a database engine, with sqlite database as default."""
    engine = sql.create_engine(connection_string)
    return engine

def write_to_db(df, sql_engine, table_name='crypto'):
    """Write a dataframe to a database, with a default table name 'funds'."""
    try:
        df.to_sql(table_name, con=sql_engine, if_exists='append', index=False)
        print("Crypto prices succesfully written to the database.")
    except:
        print("Something went wrong when writing crypto prices to the database.")

def main():
    market_request_lst = ['BTC-EUR', 'ETH-EUR', 'ADA-EUR', 'XRP-EUR']
    df_crypto = request_prices(market_request_lst)
    engine = create_db_engine() # add db connection string here
    write_to_db(df_crypto, engine)


if __name__ == '__main__':
    main()