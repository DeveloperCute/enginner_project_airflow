import duckdb
import os
DB_PATH = os.getenv('DB_PATH', '/usr/local/airflow/include/database/extrato.duckdb')
def get_connection():
    try:
        conn = duckdb.connect(DB_PATH)
        print('Conexão bem sucedidade.')
        return conn
    except Exception as e:
        print(f'Não foi possível estabelecer conexão com o banco de dados: {e}')