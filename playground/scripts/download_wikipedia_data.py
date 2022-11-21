#%%
import pandas as pd
url = "https://pt.wikipedia.org/wiki/Lista_de_pa%C3%ADses_por_popula%C3%A7%C3%A3o"
pd.read_html(url, attrs={'class':"wikitable"})[0].to_csv('pop_paises.csv', index=False)
url = "https://pt.wikipedia.org/wiki/Lista_de_pa%C3%ADses_por_PIB_nominal_per_capita"
print(len(pd.read_html(url, attrs={'class':"wikitable"})))
pd.read_html(url, attrs={'class':"wikitable"})[0].to_csv('pib_paises_fmi.csv', index=False)
pd.read_html(url, attrs={'class':"wikitable"})[1].to_csv('pib_paises_banco-mundial.csv', index=False)
pd.read_html(url, attrs={'class':"wikitable"})[2].to_csv('pib_paises_onu.csv', index=False)
#%%