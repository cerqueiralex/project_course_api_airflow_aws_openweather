# Importa a biblioteca 'os' para manipulação de variáveis de ambiente e caminhos de arquivos
import os

# Importa a biblioteca boto3 para interagir com serviços da AWS, como o S3
import boto3

# Importa a biblioteca requests para realizar chamadas HTTP (usada para acessar a API do tempo)
import requests

# Converter o dict para str ou JSON antes de passar para write():
import json

# Importa as classes necessárias do Airflow para definir o DAG e operadores Python
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Importa objetos para manipulação de datas e horários
from datetime import datetime, timedelta

# Define os argumentos padrão da DAG
default_args = {
    "owner": "Data Universe Blog",                     # Nome do responsável pela DAG
    "depends_on_past": False,                          # Indica que as execuções passadas não afetam a atual
    "start_date": datetime(2024, 5, 3),                # Data de início da DAG
    "retries": 1,                                      # Quantidade de tentativas de reexecução em caso de falha
    "retry_delay": timedelta(minutes=1),              # Tempo de espera entre tentativas
}

# Criação da DAG principal com nome "projeto_open_weather"
dub_dag = DAG(
    "projeto_open_weather",                                        # Nome do DAG
    default_args=default_args,                         # Argumentos padrão definidos anteriormente
    schedule_interval=None,                     # Define o agendamento: neste caso, executa a DAG manualmente
    catchup=False                                      # Desativa a execução retroativa de DAGs passadas
)

# Função que extrai os dados da API de clima (OpenWeatherMap)
def dub_extrai_dados():

    # URL base da API
    URL_BASE = "http://api.weatherapi.com/v1/current.json"
    
    # Chave da API (deve ser substituída por uma válida)
    API_KEY = "sua-chave-de-api"

    # Cidade para a qual a previsão será coletada
    CIDADE = "Sao Paulo"

    # Monta a URL completa para a chamada à API, com cidade e chave
    url = f"{URL_BASE}?key={API_KEY}&q={CIDADE}&aqi=yes"

    # Realiza a requisição HTTP GET e obtém a resposta em formato JSON
    response = requests.get(url).json()
    
    # Retorna os dados brutos da API
    return response

# Função que transforma os dados brutos em um formato compreensível
def dub_transforma_dados(response):

    # Cria um dicionário com os dados que interessam:
    hoje = datetime.now()
    response_data = dub_extrai_dados()

    # Criando um dicionário a partir dos dados JSON
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
    
    # Retorna os dados já formatados
    return dicionario_dados_transformados

# Função que salva os dados transformados localmente e os envia para o S3
def dub_carrega_dados(dicionario_dados_transformados):

    # Define as credenciais da AWS como variáveis de ambiente (não seguro em produção!)
    os.environ['AWS_ACCESS_KEY_ID'] = 'coloque-aqui-sua-api-key'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'coloque-aqui-sua-api-key'
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-2'  # Região padrão da AWS (pode variar)
    
    # Gera um timestamp para criar um nome único de arquivo
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')

    # Cria diretório se não existir
    output_dir = "/opt/airflow/data"
    os.makedirs(output_dir, exist_ok=True)

    # Define o caminho local onde o arquivo será salvo
    local_file_path = f"{output_dir}/dados_previsao_tempo_{timestamp}.txt"

    # Escreve os dados transformados em um arquivo de texto local
    with open(local_file_path, 'w') as file:
        file.write(f"Aqui está a previsão do tempo para a cidade escolhida:\n")
        file.write(json.dumps(dicionario_dados_transformados) + '\n')

    # Nome do bucket no S3 (deve ser criado previamente)
    s3_bucket = "dub-bucket-awsapi-2"

    # Caminho/identificador do arquivo dentro do bucket
    s3_key = f"dados_previsao_tempo_{timestamp}.txt"

    # Inicializa um cliente do S3
    s3_client = boto3.client('s3')

    # Faz o upload do arquivo local para o bucket S3
    s3_client.upload_file(local_file_path, s3_bucket, s3_key)

# Cria a tarefa de extração de dados, vinculando à função correspondente
tarefa_extrai_dados = PythonOperator(
    task_id='dub_extraindo_dados',                    # Identificador único da tarefa no DAG
    python_callable=dub_extrai_dados,                 # Função que será executada
    dag=dub_dag                                       # DAG à qual a tarefa pertence
)

# Cria a tarefa de transformação, que depende da saída da tarefa anterior
tarefa_transforma_dados = PythonOperator(
    task_id='dub_transformando_dados',
    python_callable=dub_transforma_dados,             # Função de transformação
    op_args=[tarefa_extrai_dados.output],             # Passa como argumento a saída da tarefa de extração
    dag=dub_dag
)

# Cria a tarefa de carga, que depende da transformação
tarefa_carrega_dados = PythonOperator(
    task_id='dub_carregando_dados',
    python_callable=dub_carrega_dados,                # Função de carga
    op_args=[tarefa_transforma_dados.output],         # Passa a saída da transformação
    dag=dub_dag
)

# Define a ordem de execução das tarefas no pipeline:
# Primeiro extrai, depois transforma, por fim carrega
tarefa_extrai_dados >> tarefa_transforma_dados >> tarefa_carrega_dados



# Resumo:

# Bloco de Imports: Importação das bibliotecas necessárias para o DAG.
# default_args: Definição dos argumentos padrão para o DAG.
# dub_dag: Criação do DAG com o intervalo de agendamento especificado.
# dub_extrai_dados: Função que extrai dados da API.
# dub_transforma_dados: Função que transforma os dados extraídos.
# dub_carrega_dados: Função que carrega os dados transformados para um arquivo local e faz upload para o S3.
# tarefa_extrai_dados: Definição da tarefa para extrair dados.
# tarefa_transforma_dados: Definição da tarefa para transformar dados.
# tarefa_carrega_dados: Definição da tarefa para carregar dados.
# Sequência das Tarefas: Define a ordem de execução das tarefas no DAG.