# Pipeline ETL no Airflow Para Carga de Dados na AWS API Open Weather

O projeto tem como objetivo automatizar um fluxo de extração, transformação e carga de dados meteorológicos utilizando o Apache Airflow e a AWS. 

Através de uma DAG agendada para rodar periodicamente, o pipeline realiza a extração de dados da API do OpenWeatherMap, transformando essas informações em um formato legível com a previsão do tempo para a cidade de Recife. 

Em seguida, esses dados são salvos localmente e enviados para um bucket S3 na AWS. 

O passo a passo do projeto envolve: 

* (1) configurar o ambiente do Airflow com Docker e CeleryExecutor; 
* (2) implementar funções Python para extrair, transformar e carregar os dados; 
* (3) orquestrar essas funções como tarefas sequenciais em uma DAG do Airflow;
* (4) configurar as credenciais da AWS para permitir o upload automático dos arquivos.

<img src="https://i.imgur.com/mWWHJwR.png" style="width:100%;height:auto"/>

Tecnologias:  
* AWS S3
* Apache Airflow
* Docker
* Python

## Configurações da Stack Docker 

Documentação arquivo do docker compose mais recente para airflow: https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html

Acessando pasta do projeto com cd/path  

Criar as imagens Docker do Airflow e inicializar o banco de dados:
```
docker compose up airflow-init
```

Inicializar o Airflow:
```
docker compose up
```

## Configurando Airflow

Configuração padrão de pastas do Airflow:

> config  
> dags  
> logs  
> plugins  
> .env  

http://localhost:8080/login

* User: airflow
* Senha: airflow

## API Weather API

Weather API is a team of IT experts and data scientists that has been practising deep weather data science. For each point on the globe, OpenWeather provides historical, current and forecasted weather data via light-speed APIs. Headquarters in London, UK.

https://www.weatherapi.com/

## Pipeline Scripts

```
dag.py
```

Esse código define um pipeline de ETL (Extração, Transformação e Carga) utilizando o Apache Airflow, que é executado automaticamente a cada minuto. 

Ele extrai dados climáticos da API do OpenWeatherMap para a cidade de Recife, transforma esses dados em um texto descritivo com a data, temperatura em Celsius e condição do tempo, e então salva essa informação em um arquivo .txt local com um timestamp no nome.  

Em seguida, o arquivo é enviado para um bucket S3 da AWS.  

O processo é dividido em três tarefas conectadas sequencialmente no DAG: extração, transformação e carga, utilizando operadores PythonOperator.  

O código também configura as credenciais da AWS diretamente no script (embora isso não seja recomendado em ambientes reais por questões de segurança).

## Resultado

Arquivo .txt sendo gravado no bucket S3:
<img src="https://i.imgur.com/FSJX4O0.png" style="width:100%;height:auto"/>
