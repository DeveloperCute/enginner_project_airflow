from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import dag, task
from datetime import datetime
import os


def upload_to_minio(filename, key):
    hook = S3Hook(aws_conn_id='minio_conn', verify=False)
    print(f"Subindo {filename} para camada bronze com a key {key}")
    #filename='../include/data_raw/Extrato conta corrente - 012026.xlsx'
    
    hook.load_file(
        filename=filename,
        key=key,
        bucket_name='bronze',
        replace=True
    )

@dag(
    dag_id='set_file_to_bronze', 
    schedule='@daily', 
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['bronze']
)
def set_file_to_bronze_main():

    @task
    def teste_minIO():
        hook = S3Hook(aws_conn_id='minio_conn', verify=False)
        if hook.get_bucket('bronze'):
            return 'Conexão bem sucedida! Bucket encontrado!'
        else:
            return 'Bucket não foi encontrado ou conexão foi perdida!'
        
    @task
    def extract_and_upload():
        PATH_BASE = '/usr/local/airflow/include'
        path_origem = os.path.join(PATH_BASE, 'data_raw/')
        
        if not os.path.exists(path_origem) or not os.listdir(path_origem):
            print('Nenhum arquivo está na pasta ou pasta não existe.')
        else:
            print("Arquivo encontrado na pasta")

        for arch in os.listdir(path_origem):
            file_path = os.path.join(path_origem, arch)
            print(f'Arquivo sendo processado: {arch}')
            print(f'Caminho do arquivo: {file_path}') 
            if os.path.isfile(file_path):
                versioner = datetime.now().strftime('%d-%m-%Y')
                key_ = f"{versioner}/{arch}" 
                upload_to_minio(file_path, key_)
        
    # Chamada da task dentro da DAG
    teste_minIO() >> extract_and_upload()

set_file_to_bronze_main()