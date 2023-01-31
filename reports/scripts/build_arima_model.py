# build_arima_model.py - Script to creates the serial files of the ARIMA models.
#
# Author: Duarte Junior <duarte.jr105@gmail.com>
#
# Licensed to GNU General Public License under a Contributor Agreement.
import numpy as np
import pandas as pd
from statsmodels.tsa.arima.model import ARIMA


def arima_model(train_data: "pandas.dataframe", model_name='', p=0, d=0, q=0, 
                P=0, D=0, Q=0, m=0):
    """
    This function uses the ARIMA or SARIMA method to create a time series model 
    for a specified country and save the model as a pickle file. It takes the 
    time series data for the country as input and uses it to fit the ARIMA model. 
    The resulting model can then be used to make predictions about future values 
    in the time series. The pickle file allows the model to be saved and reused 
    later without having to refit the model each time it is needed.

    Args:
        train_data (pandas.dataframe): Dataframe with the time series of COVID
                                       daily case of a given country. This data
                                       is used to adjust the model parameters.
        model_name (str, optional): The name of the model which will used to
                                    differenciate the output files. Defaults to ''.
        p (int, optional): The auto regressive order of the model. Defaults to 0.
        d (int, optional): The diferentiaton order of the model. Defaults to 0.
        q (int, optional): The moving average order of the model. Defaults to 0.
        P (int, optional): The auto regressive order of the seasonal component of
                           the model. Defaults to 0.
        D (int, optional): The diferentiation order of the seasonal component of
                           the model. Defaults to 0.
        Q (int, optional): The moving average order of the seasona component of
                           the model. Defaults to 0.
        m (int, optional): The size of the period od seasonality. Defaults to 0.
    """
    model = ARIMA(train_data['New_Confirmed'], 
                  order = (p, d, q), 
                  seasonal_order = (P, D, Q, m))
    model = model.fit()
    model.save(f'/mnt/d/bootcamp-covid/model/arima_{model_name}.pkl')

# ============================================================================
# ============          Building the model for Argentina          ============
# ============================================================================

# Path of the time series data
dir_covid_data = '../../datalake/silver/covid_data/series_with_calc_fields'
covid_file = f'{dir_covid_data}/AR.parquet' # File with the data
covid_data = pd.read_parquet(covid_file) # Read the data
# It is necessary convert the date column to the right data format.
covid_data['date'] = pd.to_datetime(covid_data.date)
# It is using just the data between 2020-01-01 and 2020-07-31 to train the model.
covid_data = covid_data[((covid_data.date > "2020-01-01") & (covid_data.date < "2020-7-31"))]
covid_data.set_index('date', inplace=True) # Defines the first columns as the index
# the model is fitted to the new confirmed cases.
reg_data = covid_data[['Country_Region', 'New_Confirmed']].dropna() # Drops NaN values
reg_data_ar = reg_data[['New_Confirmed']]
# Choosing the time when the country began to register at least 100 new cases per day.
reg_data_ar = reg_data_ar.loc[reg_data_ar.ge(100).idxmax()[0]:]
# This period was chosen because it encompasses the period when the country 
# began to have at least 100 new daily cases. The number of cases was chosen 
# because after this point, it is considered that the country has community 
# transmission and the pandemic is officially a health problem in the country.
train_data = reg_data_ar.iloc[:92] # trian dataset
arima_model(train_data, p=2, d=2, q=7, model_name='AR') # Buikds the model


# ============================================================================
# ============           Building the model for Mexico            ============
# ============================================================================

covid_file = f'{dir_covid_data}/MX.parquet'
covid_data = pd.read_parquet(covid_file)
covid_data['date'] = pd.to_datetime(covid_data.date)
covid_data = covid_data[((covid_data.date > "2020-01-01") & (covid_data.date < "2020-7-31"))]
covid_data.set_index('date', inplace=True)
reg_data = covid_data[['Country_Region', 'New_Confirmed']].dropna()
reg_data_mx = reg_data[['New_Confirmed']]
reg_data_mx = reg_data_mx.loc[reg_data_mx.ge(100).idxmax()[0]:]
train_data = reg_data_mx.iloc[:88]
arima_model(train_data, p=1, d=1, q=1, P=1, D=0, Q=0, m=7, model_name='MX')


# ============================================================================
# ============            Building the model for Chile            ============
# ============================================================================

covid_file = f'{dir_covid_data}/CH.parquet'
covid_data = pd.read_parquet(covid_file)
covid_data['date'] = pd.to_datetime(covid_data.date)
covid_data = covid_data[((covid_data.date > "2020-01-01") & (covid_data.date < "2020-7-31"))]
covid_data.set_index('date', inplace=True)
reg_data = covid_data[['Country_Region', 'New_Confirmed']].dropna() 
reg_data_ch = reg_data[['New_Confirmed']]
reg_data_ch = reg_data_ch.loc[reg_data_ch.ge(100).idxmax()[0]:]

# During the exploratory analysis, some inconsistencies were found. 
# The following steps are used to address these inconsistencies.
# The first anomaly found was a day with more then 35000 new cases in a single day.
# This number is highly different of the other. So was decided to remove it and
# replace with the average of the 7 days before it.
anomaly = reg_data_ch.query('New_Confirmed > 35_000').index
previous_dates = [anomaly.values[:][0] - np.timedelta64(i, 'D') for i in range(1, 8)]
previous_values = reg_data_ch.loc[previous_dates]
mean_previous = previous_values.mean()
reg_data_ch.loc[anomaly] = mean_previous
reg_data_ch.fillna(mean_previous, inplace=True)

# Another inconsistency was the days with zero cases, even during the first wave 
# of the pandemic. Considering that it is highly improbable for this to occur 
# and there are only a few such days, it was decided to replace these with the
# average of the cases in the preceding seven days.
anomaly = reg_data_ch.query('New_Confirmed == 0').index
for index in anomaly:
    previous_dates = [index - np.timedelta64(i, 'D') for i in range(1, 8)]
    previous_values = reg_data_ch.loc[previous_dates]
    mean_previous = previous_values.mean()
    reg_data_ch.loc[index] = mean_previous
    reg_data_ch.fillna(mean_previous, inplace=True)

train_data = reg_data_ch.iloc[:93]
arima_model(train_data, p=1, d=1, q=3, model_name='CH')

# ============================================================================
# ============           Building the model for Ecuador           ============
# ============================================================================

covid_file = f'{dir_covid_data}/EC.parquet'
covid_data = pd.read_parquet(covid_file)
covid_data['date'] = pd.to_datetime(covid_data.date)
covid_data = covid_data[((covid_data.date > "2020-01-01") & (covid_data.date < "2020-7-31"))]
covid_data.set_index('date', inplace=True)
reg_data = covid_data[['Country_Region', 'New_Confirmed']].dropna() 
reg_data_eq = reg_data[['New_Confirmed']]
reg_data_eq = reg_data_eq.loc[reg_data_eq.ge(100).idxmax()[0]:]

# Where also founded inconsistences in the time series to Equador.
# First where found days is number of case very high, a outiler when compared
# with the other days. It was replaced with the average of cases from the previous
# seven days.
anomaly = reg_data_eq.query('New_Confirmed > 10_000').index
previous_dates = [anomaly.values[:][0] - np.timedelta64(i, 'D') for i in range(1, 8)]
previous_values = reg_data_eq.loc[previous_dates]
mean_previous = previous_values.mean()
reg_data_eq.loc[anomaly] = mean_previous
reg_data_eq.fillna(mean_previous, inplace=True)

# In Ecuador, days with negative number of cases were found, which is clearly an
# inconsistency. These negative days were filled with the average of the previous
# seven days.
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


# ============================================================================
# ============            Building the model for Spain            ============
# ============================================================================

covid_file = f'{dir_covid_data}/ES.parquet'
covid_data = pd.read_parquet(covid_file)
covid_data['date'] = pd.to_datetime(covid_data.date)
covid_data = covid_data[((covid_data.date > "2020-01-01") & (covid_data.date < "2020-7-31"))]
covid_data.set_index('date', inplace=True)
reg_data = covid_data[['Country_Region', 'New_Confirmed']].dropna() 
reg_data_es = reg_data[['New_Confirmed']]
reg_data_es = reg_data_es.loc[reg_data_es.ge(100).idxmax()[0]:]

# In Spain, days with negative number of cases were found, which is clearly an
# inconsistency. These negative days were filled with the average of the previous
# seven days.
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
