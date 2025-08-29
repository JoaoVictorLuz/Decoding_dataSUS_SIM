import pandas as pd
import os

# Diretório onde estão os CSVs
diretorio = "Csv_files"

# Lista todos os arquivos CSV do diretório
arquivos_csv = [f for f in os.listdir(diretorio) if f.lower().endswith(".csv")]

# Lista para armazenar os DataFrames
lista_dfs = []



# Lê todos os CSVs
for arquivo in arquivos_csv:
    caminho_arquivo = os.path.join(diretorio, arquivo)
    df = pd.read_csv(caminho_arquivo, low_memory=False)  # evita DtypeWarning
    lista_dfs.append((arquivo, df))
    print('appendou um aaaahhhhh')


# Colunas de referência = colunas do primeiro CSV
colunas_referencia = set(lista_dfs[0][1].columns)

# Verificação
for nome_arquivo, df in lista_dfs:
    if set(df.columns) == colunas_referencia:
        print(f"{nome_arquivo}: OK")
    else:
        print(f"{nome_arquivo}: Colunas diferentes -> {df.columns.tolist()}")
