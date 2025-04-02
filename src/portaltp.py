import requests
import pandas as pd
from time import sleep
import os

def main():
    # Configurações
    anos = [2023, 2024]  # Lista de anos
    meses = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]  
    base_url = "https://santateresa-es.portaltp.com.br/api"
    delay = 1  # segundos entre requisições
    endpoints_file = "endpoints_portaltp.txt"  # Arquivo com os endpoints

    # Verifica se o arquivo de endpoints existe antes de continuar
    if not os.path.exists(endpoints_file):
        print(f"\nErro: Arquivo '{endpoints_file}' não encontrado no diretório atual.")
        print(f"Diretório atual: {os.getcwd()}")
        return

    # Carrega endpoints
    endpoints = load_endpoints(endpoints_file)
    
    if not endpoints:
        print("\nNenhum endpoint válido encontrado no arquivo. Verifique o conteúdo de endpoints.txt")
        return

    # Processa os endpoints e obtém o DataFrame consolidado
    df_consolidado = process_endpoints(endpoints, anos, meses, base_url, delay)

    # Exibe resumo final
    print("\nProcessamento concluído!")
    print(f"- Total de registros: {len(df_consolidado)}")
    print("- Amostra dos dados:")
    print(df_consolidado.head())
    
    return df_consolidado

def get_api_data(endpoint, ano, mes, base_url):
    """Função para fazer requisição à API e retornar DataFrame"""
    url = f"{base_url}/{endpoint}?ano={ano}&mes={mes:02d}"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return pd.DataFrame(response.json())
    except requests.exceptions.RequestException as e:
        print(f"Erro ao obter {endpoint} para {mes:02d}/{ano}: {str(e)}")
        return pd.DataFrame()

def load_endpoints(filename):
    """Carrega os endpoints de um arquivo .txt"""
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            return [line.strip() for line in file if line.strip()]
    except Exception as e:
        print(f"\nErro ao ler arquivo de endpoints: {str(e)}")
        return []

def process_endpoints(endpoints, anos, meses, base_url, delay):
    """Processa endpoints e retorna um único DataFrame consolidado"""
    df_consolidado = pd.DataFrame()
    
    for endpoint in endpoints:
        print(f"\n{'='*50}")
        print(f"Processando: {endpoint}")
        endpoint_name = endpoint.split('/')[-1]
        
        for ano in anos:
            for mes in meses:
                print(f"{mes:02d}/{ano}", end=' ', flush=True)
                
                # Obtém dados da API
                df_mes = get_api_data(endpoint, ano, mes, base_url)
                
                if not df_mes.empty:
                    # Adiciona colunas de ano, mes e endpoint
                    df_mes['ano'] = ano
                    df_mes['mes'] = mes
                    df_mes['endpoint'] = endpoint_name
                    # Concatena ao DataFrame principal
                    df_consolidado = pd.concat([df_consolidado, df_mes], ignore_index=True)
                
                sleep(delay)
        
        print(f"\nTotal acumulado: {len(df_consolidado)} registros")
    
    return df_consolidado

if __name__ == "__main__":
    # Executa o script e obtém o DataFrame consolidado
    df_final = main()
    
    # Exemplo de como acessar o DataFrame depois:
    if not df_final.empty:
        print("\nExemplo de acesso ao DataFrame consolidado:")
        print(f"Colunas disponíveis: {list(df_final.columns)}")
        print(f"Primeiros registros:\n{df_final.head()}")