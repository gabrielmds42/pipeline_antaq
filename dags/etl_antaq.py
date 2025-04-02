from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.utils.email import send_email
from datetime import datetime, timedelta
import os
import requests
import zipfile
import boto3
from dotenv import load_dotenv


load_dontenv()

EMAIL_ENVIO = os.getenv('EMAIL_ENVIO')
ENDPOINT_URL = os.getenv('ENDPOINT_URL')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
BUCKET_NAME = os.getenv('BUCKET_NAME')

def notify_failure(context):
    subject = f"DAG {context['dag'].dag_id} Falhou!"
    body = f"""
    A DAG {context['dag'].dag_id} falhou na task {context['task_instance'].task_id}.
    Data de execução: {context['execution_date']}
    Erro: {context['exception']}
    """
    send_email(f"{EMAIL_ENVIO}", subject, body)


def download_and_extract(ano: int, **kwargs):
    base_url = "https://web3.antaq.gov.br/ea/txt"
    arquivos = [f"{ano}Atracacao.zip", f"{ano}Carga.zip"]
    download_dir = "/opt/airflow/dags/files"
    os.makedirs(download_dir, exist_ok=True)

    s3_client = boto3.client('s3', endpoint_url=f'{ENDPOINT_URL}', aws_access_key_id=f'{AWS_ACCESS_KEY_ID}', aws_secret_access_key=f'AWS_SECRET_ACCESS_KEY')
    bucket_name = f'{BUCKET_NAME}'

    for arquivo in arquivos:
        url = f"{base_url}{arquivos}"
        file_path = os.path.join(download_dir, f"{ano}")
        os.makedirs(extract_dir, exist_ok=True)

        try:
            response = requests.get(url)
            response.raise_for_status()
            with open(file_path, "wb") as f:
                f.write(response.content)
            print(f"Download concluído: {file_path}")


