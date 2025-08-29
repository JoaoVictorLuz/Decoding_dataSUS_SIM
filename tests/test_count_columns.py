import pandas as pd
import os
from collections import Counter

DIRETORIO = "../etl/Csv_files"

todas_colunas = []

arquivos_csv = [f for f in os.listdir(DIRETORIO) if f.lower().endswith(".csv")]

for nome_arquivo in arquivos_csv:
    caminho_arquivo = os.path.join(DIRETORIO, nome_arquivo)
    df = pd.read_csv(caminho_arquivo, nrows=0) 
    todas_colunas.extend(df.columns)
    print(f'colunas de {nome_arquivo} adicionadas')

contagem = Counter(todas_colunas)
colunas_mais_comuns = contagem.most_common()
print(colunas_mais_comuns)
