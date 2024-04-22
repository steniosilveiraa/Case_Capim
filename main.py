import logging
import pandas as pd
import psycopg2
import json
import schedule
import time
import os
from psycopg2.extras import execute_batch



# Configurando o logging
logging.basicConfig(
    #filename='case.log',  # Nome do arquivo de log
    #filemode='w',         # Modo de abertura do arquivo ('a' para adicionar, 'w' para sobrescrever)
    level=logging.INFO,   # Nível de log
    format='%(asctime)s - %(levelname)s - %(message)s'  # Formato da mensagem de log
)

# Leitura do arquivo
json_file_path = r"C:\Users\stenio.da.s.alves\PycharmProjects\CaseCapim\json_for_case_delta.json"

# Acessa a variável de ambiente e armazena em uma variável Python
password1 = os.getenv('DATABASE_PASSWORD')

def connect_to_db():
    """ Estabelece conexão com o banco de dados PostgreSQL"""
    try:
        conn = psycopg2.connect(
            dbname='postgres',
            user='postgres',
            password='123',
            host='localhost',
            port='5432'
        )
        logging.info("Conexão com o banco de dados estabelecida com sucesso.")
        return conn
    except Exception as e:
        logging.error(f"Falha ao conectar ao banco de dados: {e}")
        raise


def load_json_to_df(json_file_path):
    """ Carrega dados do arquivo JSON e retorna um DataFrame normalizado. """
    try:
        with open(json_file_path, 'r', encoding='utf-8') as json_file:
            data = json.load(json_file)
            normalized_data = pd.json_normalize(data)
            df = pd.DataFrame(normalized_data)
            return df
    except Exception as e:
        logging.error(f"Erro ao carregar e normalizar JSON: {e}")
        raise

def get_existing_ids(cursor):
    """ Retorna um conjunto de IDs já existentes na tabela 'tb_cliente' do banco. """
    cursor.execute("SELECT id FROM tb_cliente")
    ids_table = set(id[0] for id in cursor.fetchall())
    return ids_table

def filter_new_records(df, existing_ids):
    """ Filtra e retorna registros do DataFrame que não contêm IDs existentes na tabela. """
    filtered_df = df[~df['id'].isin(existing_ids)]
    return filtered_df

def drop_dupl(new_records):
    """ Remove dados duplicados do DataFrame baseado na coluna 'id'. """
    new_records1 = new_records.drop_duplicates(subset=['id'], keep='first')  # Ajuste o subset conforme as colunas desejadas
    logging.info("Processo de remoção de dados duplicados aplicado.")
    return new_records1

def insert_data(new_records1):
    """ Insere registros no banco de dados na tabela 'tb_cliente'. """
    columns = ['id', 'nome', 'idade', 'email', 'telefone', 'endereco.logradouro', 'endereco.numero', 'endereco.bairro','endereco.cidade', 'endereco.estado', 'endereco.cep']
    data_tuples = [tuple(x) for x in new_records1[columns].values]
    insert_query = 'INSERT INTO tb_cliente ( id, nome, idade, email, telefone, logradouro, numero, bairro, cidade, estado, cep) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
    conn = connect_to_db()
    cur = conn.cursor()
    try:
        execute_batch(cur, insert_query, data_tuples)  # Usa execute_batch para eficiência ####
        conn.commit()  # Confirma as transações
        logging.info("Novos registros inseridos com sucesso.")
    except Exception as e:
        logging.error(f"Erro ao inserir dados: {e}")
        conn.rollback()  # Reverte todas as transações desde o último commit se houver erro
    finally:
        cur.close()
        conn.close()

def main():
    """
    Processo principal que gerencia a carga de dados do arquivo JSON para o banco de dados.
    Este script pode ser acionado de duas maneiras diferentes:
    1. Manualmente, executando a função main diretamente (comentada por padrão).
    2. Automaticamente, via agendamento, que executa a função main a cada minuto.
    """

    logging.info("Início do processo de carga de dados.")
    try:
        # Conexão com banco de dados
        conn = connect_to_db()  # Cria uma conexão com o banco
        cursor = conn.cursor()  # Cria um cursor a partir dessa conexão

        # Criação do dataframe
        df = load_json_to_df(json_file_path)
        logging.info("Data frame criado com sucesso.")

        # Filtro para carregar apenas novos registros
        existing_ids = get_existing_ids(cursor)
        new_records = filter_new_records(df, existing_ids)

        if not new_records.empty:
            new_records1 = drop_dupl(new_records)
            logging.info("Iniciando o insert na tabela.")
            insert_data(new_records1)
        else:
            logging.info("Nenhum novo registro encontrado para inserção.")
    except Exception as e:
        logging.error(f"Erro durante a execução: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# Opção para schedular - Descomente as linhas abaixo para ativar a execução agendada.
"""
schedule.every(1).minutes.do(main)

while True:
    schedule.run_pending()
    time.sleep(1)
"""

# Opção para execução manual - Descomente a linha abaixo para permitir a execução direta quando o script é chamado.

if __name__ == '__main__':
    main()
