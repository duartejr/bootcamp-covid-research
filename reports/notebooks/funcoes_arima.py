import pandas as pd
import os
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
from statsmodels.tsa.arima.model import ARIMA
from sklearn.metrics import mean_absolute_error, r2_score
from pmdarima.arima import auto_arima
from statsmodels.graphics.tsaplots import plot_pacf
from statsmodels.graphics.tsaplots import plot_acf
from statsmodels.tsa.stattools import adfuller
from numpy import log

import warnings
warnings.filterwarnings('ignore')


def read_data(country):
    dir_covid_data = '../../datalake/silver/covid_data/series_with_calc_fields'
    covid_file = f'{dir_covid_data}/{country}.parquet'
    covid_data = pd.read_parquet(covid_file)
    covid_data['date'] = pd.to_datetime(covid_data.date)
    covid_data = covid_data[((covid_data.date > "2020-01-01") & (covid_data.date < "2020-7-31"))]
    covid_data.set_index('date', inplace=True)
    reg_data = covid_data[['New_Confirmed']].dropna() # Remoção de valores NaN
    return reg_data


def arima_model_plot(train_data, test_data, p=0, d=0, q=0, country='', model_name='',
                save_model=False):
    
    model = ARIMA(train_data['New_Confirmed'], order=(p, d, q))
    model = model.fit()
    print(model.summary())
    
    fcst = pd.DataFrame([])
    data = train_data.copy()
    pred = model.predict(start=0, end=len(train_data['New_Confirmed'])-1)
    print('MAE treino:', mean_absolute_error(pred, train_data))
    print('R2:', r2_score(train_data, pred))
    
    for i in range(len(test_data)):
        fcst = pd.concat([fcst, model.forecast()])
        data = pd.concat([data, test_data.iloc[i:i+1]])
        model_test = ARIMA(data['New_Confirmed'], order=(p, d, q))
        model = model_test.smooth(model.params)
    
    # if save_model:
    #     model.save(f'D:/bootcamp-covid/model/arima_{save_model}.pkl')
    print('MAE validação:', mean_absolute_error(test_data, fcst))
    print('R2:', r2_score(test_data, fcst))
    print('='*100)
    fig, ax = plt.subplots(1, 1, figsize=(10,6))
    plt.plot(train_data['New_Confirmed'], label='Dados de treino', marker='.')
    plt.plot(pred, label='Treino predito')
    plt.plot(fcst, label='Previstos da série teste')
    plt.plot(test_data, label='Série de teste')
    plt.legend()
    plt.title(f'Modelo {model_name} para o {country}');


def sarima_model_plot(train_data, test_data, p=0, d=0, q=0, P=0, D=0, Q=0, m=0, country='', model_name=''):
    model = ARIMA(train_data['New_Confirmed'], order=(p, d, q), seasonal_order=(P, D, Q, m))
    model = model.fit()
    print(model.summary())
    
    fcst = pd.DataFrame([])
    data = train_data.copy()
    pred = model.predict(start=0, end=len(train_data['New_Confirmed'])-1)
    print('='*100)
    print('MAE treino:', mean_absolute_error(pred, train_data))
    print('R2:', r2_score(train_data, pred))
    
    for i in range(len(test_data)):
        fcst = pd.concat([fcst, model.forecast()])
        data = pd.concat([data, test_data.iloc[i:i+1]])
        model_test = ARIMA(data['New_Confirmed'], order=(p, d, q), seasonal_order=(P, D, Q, m))
        model = model_test.smooth(model.params)
    
    
    print('MAE validação:', mean_absolute_error(test_data, fcst))
    print('R2:', r2_score(test_data, fcst))
    print('='*100)
    fig, ax = plt.subplots(1, 1, figsize=(10,6))
    plt.plot(train_data['New_Confirmed'], label='Dados de treino')
    plt.plot(pred, label='Treino predito')
    plt.plot(fcst, label='Previstos da série teste')
    plt.plot(test_data, label='Série de teste')
    plt.legend()
    plt.title(f'Modelo {model_name} para o {country}')
    plt.show()
    print('MAE validação:', mean_absolute_error(test_data, fcst))
    print('R2:', r2_score(test_data, fcst))
    print('='*100)

def autoArima(train_data):
    return auto_arima(train_data,
                          start_p = 1, max_p = 7,
                          d = 1, max_d = 3,
                          start_q = 1, max_q = 7,
                          start_P = 1, max_P = 7,
                          D = 1, max_D = 3,
                          start_Q = 1, max_Q = 7,
                          m = 7, 
                          max_order = 14*3,
                          seasonal = True,
                          erro_action = 'warn',
                          trace = False,
                          supress_warnings = True,
                          stepwise = True,
                          random_state = 21,
                          n_fits = 100
                          )

def plot_diffs(train_data):
    # Original Series
    fig, axes = plt.subplots(4, 3, figsize=(20, 10))
    axes[0, 0].plot(train_data['New_Confirmed']); 
    axes[0, 0].set_title('Original Series')
    plot_acf(train_data['New_Confirmed'], ax=axes[0, 1], marker='')
    plot_pacf(train_data['New_Confirmed'], ax=axes[0, 2], marker='')
    result = adfuller(train_data['New_Confirmed'])
    print('ADF Statistic 0 Order: %f' % result[0])
    print('p-value: %f' % result[1])


    # 1st Differencing
    axes[1, 0].plot(train_data['New_Confirmed'].diff()); 
    axes[1, 0].set_title('1st Order Differencing')
    plot_acf(train_data['New_Confirmed'].diff().dropna(), ax=axes[1, 1], marker='')
    plot_pacf(train_data['New_Confirmed'].diff().dropna(), ax=axes[1, 2], marker='')
    result = adfuller(train_data['New_Confirmed'].diff().dropna())
    print('ADF Statistic 1st Order: %f' % result[0])
    print('p-value: %f' % result[1])


    # # 2nd Differencing
    axes[2, 0].plot(train_data['New_Confirmed'].diff().diff()); 
    axes[2, 0].set_title('2nd Order Differencing')
    plot_acf(train_data['New_Confirmed'].diff().diff().dropna(), ax=axes[2, 1], marker='')
    plot_pacf(train_data['New_Confirmed'].diff().diff().dropna(), ax=axes[2, 2], marker='')
    result = adfuller(train_data['New_Confirmed'].diff().diff().dropna())
    print('ADF Statistic 2nd Order: %f' % result[0])
    print('p-value: %f' % result[1])

    # # 3th Differencing
    axes[3, 0].plot(train_data['New_Confirmed'].diff().diff().diff()); 
    axes[3, 0].set_title('3nd Order Differencing')
    plot_acf(train_data['New_Confirmed'].diff().diff().diff().dropna(), ax=axes[3, 1], marker='')
    plot_pacf(train_data['New_Confirmed'].diff().diff().diff().dropna(), ax=axes[3, 2], marker='')
    result = adfuller(train_data['New_Confirmed'].diff().diff().diff().dropna())
    print('ADF Statistic 3th order: %f' % result[0])
    print('p-value: %f' % result[1])

    plt.show()
    