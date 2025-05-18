import os
import requests
import json
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "Data Universe Blog",                     # Nome do responsável pela DAG
    "depends_on_past": False,                          # Indica que as execuções passadas não afetam a atual
    "start_date": datetime(2024, 5, 3),                # Data de início da DAG
    "retries": 1,                                      # Quantidade de tentativas de reexecução em caso de falha
    "retry_delay": timedelta(minutes=1),               # Tempo de espera entre tentativas
}

dub_dag = DAG(
    "dag_local",                            # Nome do DAG
    default_args=default_args,                         # Argumentos padrão definidos anteriormente
    schedule_interval=None,                            # Define o agendamento: neste caso, executa a DAG manualmente
    catchup=False                                      # Desativa a execução retroativa de DAGs passadas
)

def dub_extrai_dados():

    url = "http://api.weatherapi.com/v1/current.json?key=a06b8ffd337e47e09df43730251704&q=Sao Paulo&aqi=yes"
    response = requests.get(url).json()
    return response
    print(response)

def dub_transforma_dados(response):

    hoje = datetime.now()
    response_data = dub_extrai_dados()

    dicionario_dados_transformados = {
            "temperature": [response_data["current"]["temp_c"]],
            "wind_speed": [response_data["current"]["wind_kph"]],
            "condition": [response_data["current"]["condition"]["text"]],
            "precipitation": [response_data["current"]["precip_mm"]],
            "humidity": [response_data["current"]["humidity"]],
            "feels_like_temp": [response_data["current"]["feelslike_c"]],
            "pressure": [response_data["current"]["pressure_mb"]],
            "visibility": [response_data["current"]["vis_km"]],
            "is_day": [response_data["current"]["is_day"]],
            "timestamp": [hoje.isoformat()]
        }
    
    return dicionario_dados_transformados
    print(dicionario_dados_transformados)

def dub_carrega_dados(dicionario_dados_transformados):

    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')

    # Salva os dados transformados em um arquivo local dentro do container worker do Docker (não fica visível nos arquivos locais)
    # Acessar o conteúdo dos arquivos dentro do container docker
    # sudo docker exec -it [id do container worker] /bin/bash
    # Listar arquivos
    # ls -l
    local_file_path = f'./dados_previsao_tempo_{timestamp}.txt'

    with open(local_file_path, 'w') as file:
        file.write(f"Aqui está a previsão do tempo para a cidade escolhida:\n")
        file.write(json.dumps(dicionario_dados_transformados) + '\n')


tarefa_extrai_dados = PythonOperator(
    task_id='dub_extraindo_dados',                    # Identificador único da tarefa no DAG
    python_callable=dub_extrai_dados,                 # Função que será executada
    dag=dub_dag                                       # DAG à qual a tarefa pertence
)

tarefa_transforma_dados = PythonOperator(
    task_id='dub_transformando_dados',
    python_callable=dub_transforma_dados,             # Função de transformação
    op_args=[tarefa_extrai_dados.output],             # Passa como argumento a saída da tarefa de extração
    dag=dub_dag
)

tarefa_carrega_dados = PythonOperator(
    task_id='dub_carregando_dados',
    python_callable=dub_carrega_dados,                # Função de carga
    op_args=[tarefa_transforma_dados.output],         # Passa a saída da transformação
    dag=dub_dag
)

tarefa_extrai_dados >> tarefa_transforma_dados >> tarefa_carrega_dados