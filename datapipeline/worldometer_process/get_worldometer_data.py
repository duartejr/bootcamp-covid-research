# import libraries
# ================

from datetime import datetime
import os
import re
from glob import glob
import requests 
import pandas as pd
from bs4 import BeautifulSoup
import re
import numpy as np
from datetime import datetime
import sys

countries = {'Argentina':1, 'Chile':2, 'Ecuador':3, "Spain":4, "Mexico":5}
execute_date = datetime.strftime(datetime.today(), "%Y-%m-%d")

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
	df_bs = df_bs.drop(['Continent', '1 Caseevery X ppl', '1 Deathevery X ppl', '1 Testevery X ppl'], axis=1)

	# rename columns
	df_bs.columns = ['Country', 'TotalCases', 'NewCases', 'TotalDeaths', 'NewDeaths',
	       'TotalRecovered', 'NewRecovered', 'ActiveCases', 'Serious,Critical',
	       'Tot Cases/1M pop', 'Deaths/1M pop', 'TotalTests', 'Tests/1M pop',
	       'Population']

	df_bs['Date'] = execute_date
	# # rearrange and subselect columns
	df_bs = df_bs[['Country', 'Date', 'Population', 'TotalCases', 'NewCases', 'TotalDeaths', 'NewDeaths',
	       'TotalRecovered', 'NewRecovered', 'ActiveCases', 'Serious,Critical',
	       'Tot Cases/1M pop', 'Deaths/1M pop', 'TotalTests', 'Tests/1M pop' ]]

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
	df_bs.to_csv(f'{dest}/worldometer_data_{execute_date}.csv', index=False)


def export_csv(src, dest):
	files = glob(f'{src}/*.csv')

	if not os.path.exists(dest):
		os.makedirs(dest)

	for i, file in enumerate(files):
		if i == 0:
			df = pd.read_csv(file)
			df = df[df["Country"].isin(countries.keys())]
		else:
			df_i = pd.read_csv(file)
			df_i = df_i[df_i["Country"].isin(countries.keys())]
			df = pd.concat([df, df_i])
	
	df['Country'] = df['Country'].replace(countries)
	df = df.fillna(0)
	df = df.drop_duplicates()
	df.to_csv(f'{dest}/worldometer_data.csv', index=False)


if __name__ == "__main__":
	src = sys.argv[1]
	dest = sys.argv[2]
	op = sys.argv[3]

	if op == "get":
		get_data(dest)
	if op == 'export':
		export_csv(src, dest)