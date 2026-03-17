from airflow.decorators import dag, task
from datetime import datetime
import pandas as pd
import duckdb
import os
import time
import shutil
from include.database.connection import get_connection
from duckdb_provider.hooks.duckdb_hook import DuckDBHook


def rename_columns(df):
    rename_columns_dict = {'Data':'data',
        'Lançamento':'lancamento',
        'Detalhes':'detalhes',
        'Valor':'valor',
        'Tipo Lançamento':'tipo_lancamento'}
    df.rename(columns=rename_columns_dict, inplace=True)
    return df

#Drop columns dont be used in project
def drop_columns(df, columns_name):
    try:
        df.drop(columns={columns_name}, inplace=True)
        print(f'Colunas dropadas: {columns_name}')
        return df
    except Exception as e:
        print(f'Não foi possível excluir as colunas: {columns_name} || {e}')

#aplicando regex nas colunas 
def apply_regex(df):
    df['valor'] = (df['valor']
    .str.replace('.', '', regex=False)  # Remove ponto de milhar
    .str.replace(',', '.', regex=False)  # Troca vírgula por ponto
    .astype(float)                       # Converte para número
)
    df['lancamento'] = (df['lancamento'].str.lower().str.replace(" ", ""))
    df['detalhes'] = df['detalhes'].str.replace(r'[^a-zA-Zá-úÁ-Ú\s]', '', regex=True).str.lower()
    return df

#Separando os dataframes sem o saldododia e com o saldododia
def get_saldo(df):
    df_saldo_ = df[(df['lancamento'] != 'saldododia') & (df['lancamento'] != 'saldo') &
                                                (df['lancamento'] != 'saldoanterior')].reset_index(drop=True)
    return df_saldo_

#cast to date
def transform_date(df):
    df['data'] = pd.to_datetime(df['data'], format='%d/%m/%Y')
    df['mes_referencia'] = df['data'].dt.month_name(locale='pt_BR.utf8')
    return df


#Criando tabelas
def create_table(conn):
    try:
        conn.execute("""
    CREATE TABLE IF NOT EXISTS tb_extrato (
            data DATE,
            lancamento VARCHAR,
            detalhes VARCHAR,
            valor DOUBLE,
            tipo_lancamento VARCHAR,
            mes_referencia VARCHAR
            )
    """)
        print(f'Tabela criada!')
    except Exception as e:
        print(f'Não foi possível criar tabela: {e}')
    return conn
#Inserindo dados
def insert_data(conn, virtual_table):
    try:
        conn.execute(f"""
    INSERT INTO tb_extrato SELECT * FROM {virtual_table}
""")
        print("Registros inseridos com sucesso.")
    except Exception as e:
        print(f"Falha ao tentar inserir os registros: {e}" )
    return conn

@dag(
    schedule='@daily', 
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['financeiro', 'duckdb']
)

def pipeline_extrato_bancario():

    @task
    def extract():

        PATH_BASE = '/usr/local/airflow/include'

        path_origem = os.path.join(PATH_BASE, 'data_raw/')
        path_bkp = os.path.join(PATH_BASE, 'backup/')
        contas_ = list()
        if not os.listdir(path_origem):
            print('Nenhum arquivo está na pasta.')
        else:
            for arch in os.listdir(path_origem):
                file_name = os.path.join(path_origem, arch)
                file_name_bkp = os.path.join(path_bkp, arch)
                try:
                    conta_corrente_temp = pd.read_excel(file_name)
                    print('Quantidade de registros lidos:')
                    print(f'{conta_corrente_temp.shape[0]} linhas e {conta_corrente_temp.shape[1]} colunas.')
                    contas_.append(conta_corrente_temp)
                except Exception as e:
                    print(f'Não foi possível ler o arquivo: {e}.')
                try:
                    shutil.move(file_name, file_name_bkp)
                    print(f'Arquivo {arch} movido para a pasta {path_bkp}.')
                except Exception as e:
                    print('****FILE_NAME:'+file_name)
                    print('*****ARCH: '+ arch)
                    print('****FILE NAME BACKUP: '+ file_name_bkp)
                    print('****PATH_BKP: ' + path_bkp)
                    print(f'Não foi possível mover o arquivo para a pasta de backup.')
                    print(e)
                time.sleep(5)
        return contas_

    @task
    def transform(dfs_extratos):
        # Aqui você encadeia: apply_regex -> get_saldo -> transform_date
        # Retorna o DF processado
        if dfs_extratos:
            df_concat = pd.concat(dfs_extratos, ignore_index=True)
            df_rename = rename_columns(df_concat)
            df_drop = drop_columns(df_rename, 'N° documento')
            df_concat = apply_regex(df_drop)
            df_filter = get_saldo(df_concat)
            df_date_format = transform_date(df_filter)
            return df_date_format
        else:
            print("Nenhum dado encontrado")
    @task
    def load(df_final):
        conn = None
        hook = DuckDBHook(duckdb_conn_id='duckdb_default')
        conn = hook.get_conn()
        try:
            conn.register('df_memoria', df_final)
            create_table(conn)
            insert_data(conn, 'df_memoria')
            print('Carga finalizada com sucesso!')
        except Exception as e:
            print('Erro ao criar os arquivos.')
        finally:
            if conn:
                print('Conexão com duckDB fechada')
                conn.close()

    # Fluxo de execução
    dados = extract()
    dados_limpos = transform(dados)
    load(dados_limpos)

pipeline_extrato_bancario()