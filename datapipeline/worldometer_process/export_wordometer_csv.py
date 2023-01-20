import pandas as pd


countries = {'Argentina':1, 'Chile':2, 'Ecuador':3, "Spain":4, "Mexico":5}

df = pd.read_csv('worldometer_data.csv')
df = df[df["Country"].isin(countries.keys())]
df['Country'] = df['Country'].replace(countries)
df.to_csv('worldometer_data_v2.csv', index=False)
print(countries.keys())
print(df.columns)