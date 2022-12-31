import pandas as pd
import os
# import seaborn as sns
from datetime import datetime
from statsmodels.tsa.arima.model import ARIMA

dir_covid_data = '../../datalake/silver/covid_data/'
covid_file = f'{dir_covid_data}/full_grouped_filtro_1.csv'
covid_data = pd.read_csv(covid_file)
covid_data.drop('Unnamed: 0', axis=1, inplace=True)
covid_data['Date'] = pd.to_datetime(covid_data['Date'])
covid_data.set_index('Date', inplace=True)
# daily_cases = covid_data.groupby('Country/Region')['Confirmed'].diff(1)
# daily_deaths = covid_data.groupby('Country/Region')['Deaths'].diff(1)
# daily_recovered = covid_data.groupby('Country/Region')['Recovered'].diff(1)
# daily_active = covid_data.groupby('Country/Region')['Active'].diff(1)

# covid_data['DailyCases'] = daily_cases # Número de casos diários
# covid_data['DailyDeaths'] = daily_deaths # Número de mortes diárias
# covid_data['DailyRecovered'] = daily_recovered # Número de casos recuperados diários
# covid_data['DailyActive'] = daily_active # Número de casos ativos diário

# # Dados necessários para o ARIMA
reg_data = covid_data[['Country/Region', 'New cases']].dropna() # Remoção de valores NaN

def arima_model(train_data, test_data, p=0, d=0, q=0, country='', model_name='',
                save_model=False):
    
    model = ARIMA(train_data['New cases'], order=(p, d, q))
    model = model.fit()
    print(model.summary())
    
    fcst = pd.DataFrame([])
    data = train_data.copy()
    pred = model.predict(start=0, end=len(train_data['New cases'])-1)
    
    for i in range(len(test_data)):
        fcst = pd.concat([fcst, model.forecast()])
        data = pd.concat([data, test_data.iloc[i:i+1]])
        model_test = ARIMA(data['New cases'], order=(p, d, q))
        model = model_test.smooth(model.params)
    
    if save_model:
        model.save(f'/mnt/d/bootcamp-covid/model/arima_{save_model}.pkl')
    
    # fig, ax = plt.subplots(1, 1, figsize=(10,6))
    # plt.plot(train_data['New cases'], label='Dados de treino')
    # plt.plot(pred, label='Treino predito')
    # plt.plot(fcst, label='Previstos da série teste')
    # plt.plot(test_data, label='Série de teste')
    # plt.legend()
    # plt.title(f'Modelo {model_name} para o {country}');
    
reg_data_ar = reg_data[reg_data['Country/Region'] == 'Argentina']
reg_data_ar = reg_data_ar[['New cases']]

train_data = reg_data_ar.iloc[:103]
test_data = reg_data_ar.iloc[103:]

arima_model(train_data, test_data, p=3, d=2, q=1, model_name='ARIMA', country='Argentina', save_model='AR')

reg_data_mx = reg_data[reg_data['Country/Region'] == 'Mexico']
reg_data_mx = reg_data_mx[['New cases']]
reg_data_mx = reg_data_mx.loc[reg_data_mx.ne(0).idxmax()[0]:]

train_data = reg_data_mx.iloc[:106]
test_data = reg_data_mx.iloc[106:]

arima_model(train_data, test_data, p=2, d=2, q=1, model_name='ARIMA', country='México', save_model='MX')

reg_data_ch = reg_data[reg_data['Country/Region'] == 'Chile']
reg_data_ch = reg_data_ch[['New cases']]
reg_data_ch = reg_data_ch.loc[reg_data_ch.ne(0).idxmax()[0]:]

train_data = reg_data_ch.iloc[:109]
test_data = reg_data_ch.iloc[109:]

arima_model(train_data, test_data, p=4, d=2, q=1, model_name='ARIMA', country='Chile', save_model='CH')

reg_data_eq = reg_data[reg_data['Country/Region'] == 'Ecuador']
reg_data_eq = reg_data_eq[['New cases']]
reg_data_eq = reg_data_eq.loc[reg_data_eq.ne(0).idxmax()[0]:]

train_data = reg_data_eq.iloc[:104]
test_data = reg_data_eq.iloc[104:]
arima_model(train_data, test_data, p=1, q=1, model_name='ARIMA', country='Equador', save_model='EC')

reg_data_es = reg_data[reg_data['Country/Region'] == 'Spain']
reg_data_es = reg_data_es[['New cases']]
reg_data_es = reg_data_es.loc[reg_data_es.ne(0).idxmax()[0]:]

train_data = reg_data_es.iloc[:125]
test_data = reg_data_es.iloc[125:]

arima_model(train_data, test_data, p=3, d=2, q=2, model_name='ARIMA', country='Espanha', save_model='ES')
