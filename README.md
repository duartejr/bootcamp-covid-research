# Bootcamp COVID

## Status: Em Andamento

## Introdução

Este repositório contém o projeto de pesquisa realizado durante o segundo bootcamp de dados da Blue.

O objetivo principal do projeto é realizar uma pesquisa a fim de identificar impactos da pandemia de COVID-19 em alguns países de língua espanhola. Para tanto foram coletados dados de diversas fontes como: Kaggle, Twitter e companhias de energia.

Com estes dados é esperado avaliar o impacto da pandemia no mercado de energia dos países em estudo e o humor da população em relação a pandemia.

## Estrutura do diretório

|- datalake (não incluso no github por limitações de espaço) -> contém os dados utilizados no projeto
|--- bronze -> contém os dados brutos, da forma como foram coletados
|----- ember -> dados de energia
|----- kaggle -> dados de covid
|----- twitter -> tweets 
|----- wikipedia -> infomações gerais sobre os países
|--- silver -> dados processados
|----- covid_data
|----- twitter
|----- energia
|----- wikipedia
|--- gold -> dados utilizados na construção do dashboard
|- datapipeline -> pipeline para coleta e processamento dos dados utilizando AirFlow
|--- airflow -> dags do Airflow para automatização dos processos
|--- spark -> scripts utilizando PySpark para processamento dos dados
|- model -> modelo de predição de casos
|- playground -> diretório com scripts e notebooks de teste
|--- dashboard
|--- notebooks
|--- scripts
|- reports -> contém os relatórios, scripts e notebooks entregues ao longo do bootcamp
|--- dashboard -> dashboard desenvolvido PowerBi para visualização final dos resultados
|--- docs -> relatórios entregues ao longo do projeto
|--- notebooks -> notebooks desenvolvidas
|--- scripts -> scripts desenvolvidos 


## Apresentação
Link para a apresentação do projeto: https://docs.google.com/presentation/d/1Gve7B-95HWTEZBnpYkYF0znJOFafiXDK_EwWljjhioU/edit?usp=sharing
