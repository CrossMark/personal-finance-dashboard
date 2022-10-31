# -*- coding: utf-8 -*-"
"""
This script can be used to scrape fund prices (in EUR) from Morningstar.
"""

from bs4 import BeautifulSoup
import datetime
import pandas as pd
import requests
from os import getenv


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

def run(self):
    # F0000152HT - NN (L) Climate & Environment- N Dis EUR
    # F00000QLRL - NN (L) Global Eq Impact Opportunities N Cap EUR
    # F00000X99D - NN (L) Green Bond N Cap EUR	
    # F0000152ID - NN (L) Health and Well-being N Dis EUR
    # F0000152IE - NN (L) Smart Connectivity N Dis EUR
    # F0GBR04FE4 - NN Global Sustainable Opportunities Fund

    funds_to_scrape_lst = ['F0000152HT', 'F00000QLRL', 'F00000X99D', 'F0000152ID', 'F0000152IE', 'F0GBR04FE4']
    df_funds = scrape_funds(funds_to_scrape_lst)

    df_funds['date'] = pd.to_datetime(df_funds['date'], dayfirst=True)
    df_funds['price'] = pd.to_numeric(df_funds['price'])

    df_funds.to_gbq(destination_table=getenv('DESTINATION_TABLE'), project_id=getenv('PROJECT_ID'), if_exists='append')
    return 'OK'