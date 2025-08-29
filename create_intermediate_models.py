import os

# Caminho da pasta
pasta = "/home/joao/pysus/projeto/projeto_dbt/models/intermediate"

# Cria a pasta caso não exista
os.makedirs(pasta, exist_ok=True)

# Loop para criar arquivos de 1996 até 2023
for ano in range(1996, 2024):
    nome_arquivo = f"int_{ano}.sql"
    caminho_arquivo = os.path.join(pasta, nome_arquivo)
    
    # Conteúdo inicial com CTE
    conteudo = f"""with do_{ano} as (
    select *
    from {{{{ ref('stg_{ano}') }}}}
)

select *
from do_{ano}
"""
    
    # Cria o arquivo (ou substitui se já existir)
    with open(caminho_arquivo, "w", encoding="utf-8") as f:
        f.write(conteudo)
    
    print(f"Arquivo criado: {caminho_arquivo}")
