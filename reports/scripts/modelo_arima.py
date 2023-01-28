import pandas as pd
import os
import numpy as np
# import seaborn as sns
from datetime import datetime
from statsmodels.tsa.arima.model import ARIMA

def arima_model(train_data, model_name='', p=0, d=0, q=0, P=0, D=0, Q=0, m=0):
    
    model = ARIMA(train_data['New_Confirmed'], order=(p, d, q), seasonal_order=(P, D, Q, m))
    model = model.fit()
    model.save(f'/mnt/d/bootcamp-covid/model/arima_{model_name}.pkl')

dir_covid_data = '../../datalake/silver/covid_data/series_with_calc_fields'
covid_file = f'{dir_covid_data}/AR.parquet'
covid_data = pd.read_parquet(covid_file)
covid_data['date'] = pd.to_datetime(covid_data.date)
covid_data = covid_data[((covid_data.date > "2020-01-01") & (covid_data.date < "2020-7-31"))]
covid_data.set_index('date', inplace=True)

# # Dados necessários para o ARIMA
reg_data = covid_data[['Country_Region', 'New_Confirmed']].dropna() # Remoção de valores NaN
# reg_data_ar = reg_data[reg_data['Country/Region'] == 'Argentina']
reg_data_ar = reg_data[['New_Confirmed']]
reg_data_ar = reg_data_ar.loc[reg_data_ar.ge(100).idxmax()[0]:]
train_data = reg_data_ar.iloc[:92]

arima_model(train_data, p=2, d=2, q=7, model_name='AR')

covid_file = f'{dir_covid_data}/MX.parquet'
covid_data = pd.read_parquet(covid_file)
covid_data['date'] = pd.to_datetime(covid_data.date)
covid_data = covid_data[((covid_data.date > "2020-01-01") & (covid_data.date < "2020-7-31"))]
covid_data.set_index('date', inplace=True)
reg_data = covid_data[['Country_Region', 'New_Confirmed']].dropna() # Remoção de valores NaN
reg_data_mx = reg_data[['New_Confirmed']]
reg_data_mx = reg_data_mx.loc[reg_data_mx.ge(100).idxmax()[0]:]
train_data = reg_data_mx.iloc[:88]

arima_model(train_data, p=1, d=1, q=1, P=1, D=0, Q=0, m=7, model_name='MX')

covid_file = f'{dir_covid_data}/CH.parquet'
covid_data = pd.read_parquet(covid_file)
covid_data['date'] = pd.to_datetime(covid_data.date)
covid_data = covid_data[((covid_data.date > "2020-01-01") & (covid_data.date < "2020-7-31"))]
covid_data.set_index('date', inplace=True)
reg_data = covid_data[['Country_Region', 'New_Confirmed']].dropna() # Remoção de valores NaN
reg_data_ch = reg_data[['New_Confirmed']]
reg_data_ch = reg_data_ch.loc[reg_data_ch.ge(100).idxmax()[0]:]
anomaly = reg_data_ch.query('New_Confirmed > 35_000').index
previous_dates = [anomaly.values[:][0] - np.timedelta64(i, 'D') for i in range(1, 8)]
previous_values = reg_data_ch.loc[previous_dates]
mean_previous = previous_values.mean()
reg_data_ch.loc[anomaly] = mean_previous
reg_data_ch.fillna(mean_previous, inplace=True)
anomaly = reg_data_ch.query('New_Confirmed == 0').index
for index in anomaly:
    print(index)
    previous_dates = [index - np.timedelta64(i, 'D') for i in range(1, 8)]
    previous_values = reg_data_ch.loc[previous_dates]
    mean_previous = previous_values.mean()
    reg_data_ch.loc[index] = mean_previous
    reg_data_ch.fillna(mean_previous, inplace=True)
train_data = reg_data_ch.iloc[:93]

arima_model(train_data, p=1, d=1, q=3, model_name='CH')

covid_file = f'{dir_covid_data}/EC.parquet'
covid_data = pd.read_parquet(covid_file)
covid_data['date'] = pd.to_datetime(covid_data.date)
covid_data = covid_data[((covid_data.date > "2020-01-01") & (covid_data.date < "2020-7-31"))]
covid_data.set_index('date', inplace=True)
reg_data = covid_data[['Country_Region', 'New_Confirmed']].dropna() # Remoção de valores NaN
reg_data_eq = reg_data[['New_Confirmed']]
reg_data_eq = reg_data_eq.loc[reg_data_eq.ge(100).idxmax()[0]:]
anomaly = reg_data_eq.query('New_Confirmed > 10_000').index
previous_dates = [anomaly.values[:][0] - np.timedelta64(i, 'D') for i in range(1, 8)]
previous_values = reg_data_eq.loc[previous_dates]
mean_previous = previous_values.mean()
reg_data_eq.loc[anomaly] = mean_previous
reg_data_eq.fillna(mean_previous, inplace=True)
negatives = reg_data_eq.query('New_Confirmed < 0').index
reg_data_eq.loc[negatives] = 0
anomaly = reg_data_eq.query('New_Confirmed == 0').index
for index in anomaly:
    print(index)
    previous_dates = [index - np.timedelta64(i, 'D') for i in range(1, 8)]
    previous_values = reg_data_eq.loc[previous_dates]
    mean_previous = previous_values.mean()
    reg_data_eq.loc[index] = mean_previous
    reg_data_eq.fillna(mean_previous, inplace=True)
train_data = reg_data_eq.iloc[:88]
arima_model(train_data, p=1, d=1, q=1, model_name='EC')

covid_file = f'{dir_covid_data}/ES.parquet'
covid_data = pd.read_parquet(covid_file)
covid_data['date'] = pd.to_datetime(covid_data.date)
covid_data = covid_data[((covid_data.date > "2020-01-01") & (covid_data.date < "2020-7-31"))]
covid_data.set_index('date', inplace=True)
reg_data = covid_data[['Country_Region', 'New_Confirmed']].dropna() # Remoção de valores NaN
reg_data_es = reg_data[['New_Confirmed']]
reg_data_es = reg_data_es.loc[reg_data_es.ge(100).idxmax()[0]:]
negatives = reg_data_es.query('New_Confirmed < 0').index
reg_data_es.loc[negatives] = 0
anomaly = reg_data_es.query('New_Confirmed == 0').index
for index in anomaly:
    print(index)
    try:
        previous_dates = [index - np.timedelta64(i, 'D') for i in range(1, 8)]
        previous_values = reg_data_es.loc[previous_dates]
        mean_previous = previous_values.mean()
        reg_data_es.loc[index] = mean_previous
        reg_data_es.fillna(mean_previous, inplace=True)
    except:
        pass
train_data = reg_data_es.iloc[:103]

arima_model(train_data, p=2, d=1, q=2, model_name='ES')
