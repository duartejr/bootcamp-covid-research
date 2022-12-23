# Bootcamp COVID

## Status: Em Andamento

## Introdução

Este repositório contém o projeto de pesquisa realizado durante o segundo bootcamp de dados da Blue.

O objetivo principal do projeto é realizar uma pesquisa a fim de identificar impactos da pandemia de COVID-19 em alguns países de língua espanhola. Para tanto foram coletados dados de diversas fontes como: Kaggle, Twitter e companhias de energia.

Com estes dados é esperado avaliar o impacto da pandemia no mercado de energia dos países em estudo e o humor da população em relação a pandemia.

## Estrutura do diretório

|-> datalake (não incluso no github por limitações de espaço) -> contém os dados utilizados no projeto</br>
|---> bronze -> contém os dados brutos, da forma como foram coletados</br>
|-----> ember -> dados de energia</br>
|-----> kaggle -> dados de covid</br>
|-----> twitter -> tweets </br>
|-----> wikipedia -> infomações gerais sobre os países</br>
|---> silver -> dados processados</br>
|-----> covid_data</br>
|-----> twitter</br>
|-----> energia</br>
|-----> wikipedia</br>
|---> gold -> dados utilizados na construção do dashboard</br>
|-> datapipeline -> pipeline para coleta e processamento dos dados utilizando AirFlow</br>
|---> airflow -> dags do Airflow para automatização dos processos</br>
|---> spark -> scripts utilizando PySpark para processamento dos dados</br>
|-> model -> modelo de predição de casos</br>
|-> playground -> diretório com scripts e notebooks de teste</br>
|---> dashboard</br>
|---> notebooks</br>
|---> scripts</br>
|-> reports -> contém os relatórios, scripts e notebooks entregues ao longo do bootcamp</br>
|---> dashboard -> dashboard desenvolvido PowerBi para visualização final dos resultados</br>
|---> docs -> relatórios entregues ao longo do projeto</br>
|---> notebooks -> notebooks desenvolvidas</br>
|---> scripts -> scripts desenvolvidos </br>


## Apresentação
Link para a apresentação do projeto: https://docs.google.com/presentation/d/1Gve7B-95HWTEZBnpYkYF0znJOFafiXDK_EwWljjhioU/edit?usp=sharing
