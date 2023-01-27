# insert_into_datawarehouse - Script to insert data into the data warehouse.
#
# Author: Duarte Junior <duarte.jr105@gmail.com>
# Based in this work: https://github.com/imdevskp/covid_19_jhu_data_web_scrap_and_cleaning
#
# Licensed to GNU General Public License under a Contributor Agreement.
import os
import sys
import requests
import numpy as np
import pandas as pd
from glob import glob
from datetime import datetime
from bs4 import BeautifulSoup
from datetime import datetime

# Control variables
COUNTRIES = {'Argentina': 1, 'Chile': 2, 'Ecuador': 3, "Spain": 4, "Mexico": 5}
EXCECUTE_DATE = datetime.strftime(datetime.today(), "%Y-%m-%d")

# This part of the script is based in the work of Devakumar kp @imdevskp
def get_data(dest):
    # get data
    # ========

    # link at which web data recides
    link = 'https://www.worldometers.info/coronavirus/'
    # get web data
    req = requests.get(link)
    # parse web data
    soup = BeautifulSoup(req.content, "html.parser")

    # find the table
    # ==============
    # our target table is the last table in the page

    # get the table head
    # table head may contain the column names, titles, subtitles
    thead = soup.find_all('thead')[-1]
    # print(thead)

    # get all the rows in table head
    # it usually have only one row, which has the column names
    head = thead.find_all('tr')
    # print(head)

    # get the table tbody
    # it contains the contents
    tbody = soup.find_all('tbody')[0]
    # print(tbody)

    # get all the rows in table body
    # each row is each state's entry
    body = tbody.find_all('tr')
    # print(body)

    # get the table contents
    # ======================

    # container for header rows / column title
    head_rows = []
    # container for table body / contents
    body_rows = []

    # loop through the head and append each row to head
    for tr in head:
        td = tr.find_all(['th', 'td'])
        row = [i.text for i in td]
        head_rows.append(row)
    # print(head_rows)

    # loop through the body and append each row to body
    for tr in body:
        td = tr.find_all(['th', 'td'])
        row = [i.text for i in td]
        body_rows.append(row)
    # print(head_rows)

    # save contents in a dataframe
    # ============================

    # skip last 3 rows, it contains unwanted info
    # head_rows contains column title
    df_bs = pd.DataFrame(body_rows[:len(body_rows)-6],
                         columns=head_rows[0])

    # drop unwanted rows
    df_bs = df_bs.iloc[8:, :-3].reset_index(drop=True)

    # drop unwanted columns
    df_bs = df_bs.drop('#', axis=1)
    df_bs = df_bs.drop(['Continent', '1 Caseevery X ppl', '1 Deathevery X ppl',
                        '1 Testevery X ppl'], axis=1)

    # rename columns
    df_bs.columns = ['Country', 'TotalCases', 'NewCases', 'TotalDeaths',
                     'NewDeaths', 'TotalRecovered', 'NewRecovered', 'ActiveCases',
                     'Serious,Critical', 'Tot Cases/1M pop', 'Deaths/1M pop',
                     'TotalTests', 'Tests/1M pop', 'Population']

    df_bs['Date'] = EXCECUTE_DATE
    
    # # rearrange and subselect columns
    df_bs = df_bs[['Country', 'Date', 'Population', 'TotalCases', 'NewCases',
                   'TotalDeaths', 'NewDeaths', 'TotalRecovered', 'NewRecovered',
                   'ActiveCases', 'Serious,Critical', 'Tot Cases/1M pop',
                   'Deaths/1M pop', 'TotalTests', 'Tests/1M pop']]

    # # fix data
    for col in df_bs.columns[2:]:
        # replace comma with empty string
        df_bs[col] = df_bs[col].str.replace('[,+ ]', '', regex=True)
        # replace 'N/A' with empty string
        df_bs[col] = df_bs[col].str.replace('N/A', '', regex=False)

    # # replace empty strings with np.nan
    df_bs = df_bs.replace('', np.nan)

    # # save as .csv file

    if not os.path.exists(dest):
        os.makedirs(dest)
    
    df_bs.to_csv(f'{dest}/worldometer_data_{EXCECUTE_DATE}.csv', index=False)


def export_csv(src: str, dest: str):
    """
    Export the woldometer data into CSV format.

    Args:
        src (str): Source folder with the data.
        dest (str): Destination folder with the data.
    """
    files = glob(f'{src}/*.csv') # List all CSV files in the source folder 

    # If the destination folder does not exist creates a new one.
    if not os.path.exists(dest):
        os.makedirs(dest)

    # Loop to interate in each file
    for i, file in enumerate(files):
        # if is the first file
        if i == 0:
            df = pd.read_csv(file) # read the data
            df = df[df["Country"].isin(COUNTRIES.keys())] # initialize the dataframe
        # In other case
        else:
            df_i = pd.read_csv(file) # read the data
            df_i = df_i[df_i["Country"].isin(COUNTRIES.keys())] # select data of specific countries
            df = pd.concat([df, df_i]) # concat the new data with the old data
    
    df['Country'] = df['Country'].replace(COUNTRIES) # rename countries names
    df = df.fillna(0) # Fill nan values with zeros
    df = df.drop_duplicates() # Remove duplicated rows
    
    # Calculates the mortality ratio.
    df['Mortality'] = df['TotalDeaths'] / df['Population']
    
    # Export the data into CSV format.
    df.to_csv(f'{dest}/worldometer_data.csv', index=False)


if __name__ == "__main__":
    src  = sys.argv[1] # Gets source folder
    dest = sys.argv[2] # Gets destination folder
    op   = sys.argv[3] # Get the operation. get to collect data or export to export data

    if op == "get": # If the operation is get it collect the data
        get_data(dest)
    if op == 'export': # If the operation is export it export the data
        export_csv(src, dest)
