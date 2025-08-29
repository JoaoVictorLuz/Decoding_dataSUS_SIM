import os
import pandas as pd

input_dir = 'Parquet_files'
output_dir = 'Csv_files'

for file in os.listdir(input_dir):
    if file.endswith('.parquet'):
        parquet_path = os.path.join(input_dir, file)
        csv_path = os.path.join(output_dir, file.replace('.parquet', '.csv'))

        if os.path.exists(csv_path):
            print(f'Arquivo CSV já existe: {csv_path}, pulando conversão.')
            continue

        else:
            df = pd.read_parquet(parquet_path)
            df.to_csv(csv_path, index=False)
            print(f'Convertido {file} para CSV')