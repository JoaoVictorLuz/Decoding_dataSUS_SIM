import snowflake.connector
import os
from db import get_connection
from dotenv import load_dotenv
load_dotenv()
# Configurações
DIRETORIO_LOCAL = "Csv_files"  # diretório local com os arquivos .csv
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_STAGE = f"{SNOWFLAKE_SCHEMA}.DO_DATA"
FILE_FORMAT = f"{SNOWFLAKE_SCHEMA}.CSV_COMMASEP"
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")

# Conexão Snowflake
conn = get_connection()
cur = conn.cursor()



# Testa conexão
try:
    cur.execute("SELECT CURRENT_VERSION()")
    version = cur.fetchone()[0]
    print(f"Conexão OK! Versão do Snowflake: {version}")
except Exception as e:
    print("Erro na conexão:", e)

# Lista arquivos .csv no diretório local
arquivos_csv = [f for f in os.listdir(DIRETORIO_LOCAL) if f.lower().endswith(".csv")]
print("Arquivos locais encontrados:", arquivos_csv)

# Executa COPY INTO para cada arquivo correspondente no stage
for arquivo_csv in arquivos_csv:
    # Nome base do arquivo (sem extensão .csv)
    nome_base = os.path.splitext(arquivo_csv)[0]
    
    # Nome do arquivo no stage (adiciona .csv.gz)
    arquivo_stage = f"{nome_base}.csv.gz"
    
    # Nome da tabela
    nome_tabela = nome_base.upper()
    
    query = f"""
    COPY INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{nome_tabela}
    FROM @{SNOWFLAKE_STAGE}
    FILES = ('{arquivo_stage}')
    FILE_FORMAT = ( FORMAT_NAME = {FILE_FORMAT} );
    """
    
    try:
        cur.execute(query)
        print(f"Tabela '{nome_tabela}' carregada com sucesso a partir do arquivo '{arquivo_stage}' no stage")
    except Exception as e:
        print(f"Erro ao carregar tabela '{nome_tabela}': {e}")

cur.close()
conn.close()

