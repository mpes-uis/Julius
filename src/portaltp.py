import requests
import pandas as pd
from time import sleep

# Configurações
ano = 2024
meses = range(1, 13)  # De 1 (janeiro) a 12 (dezembro)
base_url = "https://santateresa-es.portaltp.com.br/api/Compras"
delay = 1  # segundos entre requisições

# DataFrames finais
df_licitacoes = pd.DataFrame()
df_contratos = pd.DataFrame()

def get_api_data(endpoint, ano, mes):
    """Função para fazer requisição à API e retornar DataFrame"""
    url = f"{base_url}/{endpoint}?ano={ano}&mes={mes:02d}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return pd.DataFrame(response.json())
    except Exception as e:
        print(f"Erro ao obter {endpoint} para {mes:02d}/{ano}: {str(e)}")
        return pd.DataFrame()

for mes in meses:
    print(f"\nProcessando mês {mes:02d}/{ano}...")
    
    # Licitações
    df_mes_lic = get_api_data("GetLicitacoes", ano, mes)
    if not df_mes_lic.empty:
        df_mes_lic['ano'] = ano
        df_mes_lic['mes'] = mes
        df_licitacoes = pd.concat([df_licitacoes, df_mes_lic], ignore_index=True)
        print(f"Licitações: +{len(df_mes_lic)} registros")
    
    # Contratos/Aditivos
    df_mes_con = get_api_data("GetContratosAditivos", ano, mes)
    if not df_mes_con.empty:
        df_mes_con['ano'] = ano
        df_mes_con['mes'] = mes
        df_contratos = pd.concat([df_contratos, df_mes_con], ignore_index=True)
        print(f"Contratos: +{len(df_mes_con)} registros")
    
    sleep(delay)

# Salvando os resultados
if not df_licitacoes.empty:
    lic_file = f"licitacoes_consolidadas_{ano}.csv"
    df_licitacoes.to_csv(lic_file, index=False, encoding='utf-8-sig')
    print(f"\nLicitações salvas em '{lic_file}'")
    print(f"Total de licitações: {len(df_licitacoes)}")
else:
    print("\nNenhuma licitação encontrada")

if not df_contratos.empty:
    con_file = f"contratos_aditivos_{ano}.csv"
    df_contratos.to_csv(con_file, index=False, encoding='utf-8-sig')
    print(f"Contratos/Aditivos salvos em '{con_file}'")
    print(f"Total de contratos/aditivos: {len(df_contratos)}")
else:
    print("Nenhum contrato/aditivo encontrado")

# Mostrando amostras dos dados
if not df_licitacoes.empty:
    print("\nAmostra das licitações:")
    print(df_licitacoes.head(2))

if not df_contratos.empty:
    print("\nAmostra dos contratos/aditivos:")
    print(df_contratos.head(2))