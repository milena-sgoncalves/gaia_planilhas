import os
import sys
import pandas as pd
import sqlite3
from dotenv import load_dotenv
import pydot
from sqlalchemy import create_engine, MetaData
from sqlalchemy_schemadisplay import create_schema_graph
from mover_arquivos import mover_arquivos_excel
import shutil

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()
ROOT = os.getenv('ROOT')

# Caminho do diretório contendo as planilhas
GAIA_UP = os.path.abspath(os.path.join(ROOT, 'GAIA_up'))

# Caminho do banco de dados SQLite
db_up = os.path.join(ROOT, 'GAIA_up', 'db_gaia.db')

def gerar_db_sqlite():

    # Conectar ao banco de dados SQLite
    conn = sqlite3.connect(db_up)
    cursor = conn.cursor()

    # Iterar sobre os arquivos no diretório
    for filename in os.listdir(GAIA_UP):

        if filename.endswith(".xlsx"):
            filepath = os.path.join(GAIA_UP, filename)
            # Ler a planilha
            df = pd.read_excel(filepath)
            # Nome da tabela no banco de dados será o nome do arquivo sem a extensão
            table_name = os.path.splitext(filename)[0]
            # Sanitizar nomes de colunas
            df.columns = [col.encode('ascii', 'ignore').decode('ascii') for col in df.columns]
            # Limpar dados da tabela antes de inserir novos dados
            #clear_table(table_name, cursor, conn)
            # Inserir os dados no banco de dados
            df.to_sql(table_name, conn, if_exists='append', index=False)
            
    # Fechar a conexão com o banco de dados
    conn.close()

    #gerar_der()


# def gerar_der():
#     # Conectar ao banco de dados SQLite usando SQLAlchemy
#     engine = create_engine(f'sqlite:///{db_up}')
#     metadata = MetaData()
#     metadata.reflect(bind=engine)

#     # Criar o diagrama ER
#     graph = create_schema_graph(metadata=metadata, engine=engine)
#     graph.write_png(der_path)


# Função para limpar dados da tabela
def clear_table(table_name, cursor, conn):
    cursor.execute(f'DELETE FROM "{table_name}"')
    conn.commit()
