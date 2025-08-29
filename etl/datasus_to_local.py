import pandas as pd
import os
from pysus import SIM
from pysus.preprocessing.decoders import translate_variables_SIM
#from pysus.preprocessing.decoders import decodifica_sim
from pysus.online_data.SIM import get_CID9_table, get_CID10_table, get_municipios, get_ocupations
#from pysus.online_data.SIM import ge
sim = SIM().load()



lista_de_arquivos = sim.get_files("CID10", uf="BR")
print(len(lista_de_arquivos))
total_de_arquivos_baixados = 0

os.makedirs('Parquet_files', exist_ok=True)

for ftp_file in lista_de_arquivos:
    try:
        sim.download(ftp_file, local_dir='Parquet_files')
        total_de_arquivos_baixados += 1
    except Exception as e:
        print(f"Erro ao baixar {ftp_file}: {e}")
        continue

print(f"Total de arquivos: {len(lista_de_arquivos)}")
print(f"Total de arquivos baixados: {total_de_arquivos_baixados}")

