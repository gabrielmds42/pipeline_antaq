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
        file_path = os.path.join(download_dir, arquivo)
        extract_dir = os.path.join(download_dir, f"{ano}")
        os.makedirs(extract_dir, exist_ok=True)

        try:
            response = requests.get(url)
            response.raise_for_status()
            with open(file_path, "wb") as f:
                f.write(response.content)
            print(f"Download concluído: {file_path}")
        
        with zipfile.Zipfile(file_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)
        print(f"Extração concluído para {arquivo}")

        for extracted_file in os.listdir(extract_dir):
            file_key = f"bronze/{'atracacao' if 'Atracao' in arquivo else 'carga'}/{ano}/{extracted_file}"
            s3_client.upload_file(os.path.join(extract_dir, extracted_file), bucket_name, file_key)
            print(f"Arquivo enviado para S3: s3://{bucket_name}/{file_key}")
    except request.execptions.ResquestException as e:
        print(f"Erro as baixar {arquivo}: {e}")

# Parâmetros padrão
DEFAULT_YEARS = [2021, 2022, 2023]

def generate_tasks(dag):
    for ano in DEFAULT_YEARS:
        task_download = PythonOperator(
            task_id=f"download_and_extract_{ano}",
            python_callable=download_and_extract,
            op_kwargs={"ano": ano},
            provide_context=True,
            dag=dag,
        )

        task_process_atracacao = BashOperator(
            task_id=f"process_atracacao_{ano}",
            bash_command=f"docker exec spark spark-submit --master spark://spark:7077 /opt/airflow/dags/scripts/process_atracacao.py {ano}",
            dag=dag,
        )

        task_process_carga = BashOperator(
            task_id=f"process_carga_{ano}",
            bash_command=f"docker exec spark spark-submit --master spark://spark:7077 /opt/airflow/dags/scripts/process_carga.py {ano}",
            dag=dag,
        )
        
        task_load_atracacao = BashOperator(
            task_id=f"load_atracacao_{ano}",
            bash_command=f"docker exec spark spark-submit --master spark://spark:7077 /opt/airflow/dags/scripts/load_atracacao.py {ano}",
            dag=dag,
        )
        
        task_load_carga = BashOperator(
            task_id=f"load_carga_{ano}",
            bash_command=f"docker exec spark spark-submit --master spark://spark:7077 /opt/airflow/dags/scripts/load_carga.py {ano}",
            dag=dag,
        )     

        task_success = EmailOperator(
            task_id=f"email_sucesso_{ano}",
            to=f"{EMAIL_ENVIO}",
            subject=f"DAG {dag.dag_id} foi executada com sucesso para o ano {ano}.",
            dag=dag,
        )

        start_task >> task_download
        task_download >> process >> [task_process_atracao, task_process_carga]
        [task_process_atracao, task_process_carga] >> load >> [task_load_atracao, task_load_carga]
        [task_load_atracacao, task_load_carga] >> task_success

    
