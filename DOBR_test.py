import pandas as pd
import os

#Vou pegar um DOBR e comparar com outros DO(outros estados) para ver se eles contém as mesmas informações 

# Lê o arquivo parquet e transforma em DataFrame

df_BR_2000 = pd.read_parquet('Parquet_files/DOBR2000.parquet')
df_AC_2000 = pd.read_parquet('Parquet_files_test/DOAC2000.parquet')
df_AL_2000 = pd.read_parquet('Parquet_files_test/DOAL2000.parquet')

# teste com acre 
print(df_AC_2000.head().iloc[:, :20])
print(df_BR_2000[

    (df_BR_2000["DTNASC"] == '09011909') &
    (df_BR_2000["DTOBITO"] == '27022000') 
    #(df_BR_2000["DTNASC"] == '1200609  ')
].iloc[:, :20])

print(df_AL_2000.head().iloc[:, :20])
print(df_BR_2000[
    (df_BR_2000["DTNASC"] == '26111919') &
    (df_BR_2000["DTOBITO"] == '13012000')
    #(df_BR_2000[""] == )
].iloc[:, :20])
