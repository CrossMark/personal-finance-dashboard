# -*- coding: utf-8 -*-"
"""
This script can be used to scrape fund prices (in EUR) from Morningstar.
"""

from bs4 import BeautifulSoup
import datetime
import pandas as pd
import requests
import sqlalchemy as sql


def scrape_funds(funds_list):
    """
    Scrape fund prices from the funds that are given in the ``funds_list``.
    
    On the Morningstar fund page is a small table containing fund information.
    From this table, the fund price and the corresponding date is scraped.
    This information is returned in a DataFrame.

    Parameters
    ----------
    funds_list : list of str
        List of Morningstar codes of the funds to scrape.

    Returns
    -------
    df_funds : DataFrame
        DataFrame with fund code, date, price and scrape time in local time.

    """
    
    base_url = 'https://www.morningstar.nl/nl/funds/snapshot/snapshot.aspx'
    fund_price_list = []
    
    for fund in funds_list:
        request_url = base_url + '?id=' + fund
        
        # Get the page content and the time of scraping
        try:
            page = requests.get(request_url).content
            scrape_timestamp = datetime.datetime.now()
            print("Succesful GET request")
        except:
            print("Unsuccessful GET request")
            
        soup = BeautifulSoup(page, 'html.parser')
        
        first_row = soup.find_all(class_='overviewKeyStatsTable')[0].find_all('tr')[1]

        # Get date from table
        date = first_row.find_all('span', class_ = 'heading')[0].get_text()
        
        # Get price from table and replace comma for a dot
        price = first_row.find_all('td', class_ = 'line text')[0].get_text()[4:].replace(',', '.')
        
        fund_price_list.append([fund, date, price, scrape_timestamp])
    df_funds = pd.DataFrame.from_records(fund_price_list, columns=['fund', 'date', 'price', 'scrape_timestamp'])
    return df_funds

def create_db_engine(connection_string='sqlite:///database.db'):
    """Return a database engine, with sqlite database as default."""
    engine = sql.create_engine(connection_string)
    return engine

def write_to_db(df, sql_engine, table_name='funds'):
    """Write a dataframe to a database, with a default table name 'funds'."""
    try:
        df.to_sql(table_name, con=sql_engine)
        print("Fund prices succesfully written to the database.")
    except:
        print("Something went wrong when writing fund prices to the database.")

def main():
    funds_to_scrape_lst = [] # add fund codes here
    df_funds = scrape_funds(funds_to_scrape_lst)
    engine = create_db_engine() # add db connection string here
    write_to_db(df_funds, engine)


if __name__ == '__main__':
    main()