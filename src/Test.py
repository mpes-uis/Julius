import requests
import pandas as pd
import sqlite3

# Configurações da API
url = "https://baixoguandu-es.portaltp.com.br/api/Cmcb/PostContratos"
headers = {
    "Content-Type": "application/json",
    # "Authorization": "Bearer SEU_TOKEN"  # Se necessário
}

# Body corrigido (lista vazia ou com parâmetros de filtro)
payload = {
    "contratos": [
        # Se a API permitir filtros, adicione aqui. Exemplo:
        # {
        #    "ExercicioContracto": 2023,
        #    "Formecador": "NOME_DO_FORNECEDOR"
        # }
    ]
}

# Requisição POST
response = requests.post(url, json=payload, headers=headers)

# Verificação
if response.status_code == 200:
    print("Dados recebidos!")
    dados = response.json()
    
    # Converter para DataFrame
    df = pd.DataFrame(dados)  # Ajuste se a estrutura for diferente (ex: dados["contratos"])
    
    # Salvar no SQLite
    conn = sqlite3.connect("contratos.db")
    df.to_sql("contratos", conn, if_exists="replace", index=False)
    conn.close()
    
    print("Dados salvos no SQLite!")
else:
    print(f"Erro {response.status_code}: {response.text}")