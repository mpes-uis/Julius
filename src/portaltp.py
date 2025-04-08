import requests
import pandas as pd
import sqlite3
from time import sleep
import os

def main():
    # Configurações
    anos = [2023, 2024]
    meses = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]  
    url_template = "https://{municipio}-es.portaltp.com.br/api"
    delay = 1
    endpoints_file = "endpoints_portaltp.txt"
    prefeituras_file = "prefeituras.csv"
    db_file = "dados_transparencia_portaltp.db"

    # Verifica arquivos necessários
    if not all(os.path.exists(f) for f in [endpoints_file, prefeituras_file]):
        print("\nErro: Arquivos necessários não encontrados.")
        return

    # Conecta ao SQLite (cria o banco se não existir)
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    
    # Cria tabela se não existir
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS transparencia (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        municipio TEXT,
        endpoint TEXT,
        ano INTEGER,
        mes INTEGER,
        dados TEXT,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    conn.commit()

    # Carrega dados
    endpoints = load_endpoints(endpoints_file)
    prefeituras = load_prefeituras(prefeituras_file)
    prefeituras_portaltp = prefeituras[prefeituras['empresa'] == 'portaltp']

    if prefeituras_portaltp.empty:
        print("\nNenhuma prefeitura com empresa 'portaltp' encontrada.")
        conn.close()
        return

    # Processa cada prefeitura
    for _, prefeitura in prefeituras_portaltp.iterrows():
        municipio = prefeitura['municipio']
        base_url = url_template.format(municipio=municipio)
        
        print(f"\n{'='*50}")
        print(f"Processando: {prefeitura['prefeitura']} ({municipio})")
        
        for endpoint in endpoints:
            endpoint_name = endpoint.split('/')[-1]
            print(f"\nEndpoint: {endpoint_name}")
            
            for ano in anos:
                for mes in meses:
                    print(f"{mes:02d}/{ano}", end=' ', flush=True)
                    
                    # Obtém dados da API
                    url = f"{base_url}/{endpoint}?ano={ano}&mes={mes:02d}"
                    try:
                        response = requests.get(url, timeout=10)
                        response.raise_for_status()
                        dados = response.json()
                        
                        # Insere no SQLite
                        cursor.execute('''
                        INSERT INTO transparencia 
                        (municipio, endpoint, ano, mes, dados) 
                        VALUES (?, ?, ?, ?, ?)
                        ''', (municipio, endpoint_name, ano, mes, str(dados)))
                        
                        conn.commit()
                        
                    except Exception as e:
                        print(f" [Erro: {str(e)}]", end=' ')
                    
                    sleep(delay)

    print("\n\nProcessamento concluído!")
    print(f"Dados salvos em: {db_file}")
    
    # Exemplo de consulta
    print("\nExemplo de dados armazenados:")
    cursor.execute("SELECT municipio, endpoint, COUNT(*) as registros FROM transparencia GROUP BY municipio, endpoint")
    for row in cursor.fetchmany(5):
        print(row)
    
    conn.close()

def load_prefeituras(filename):
    try:
        return pd.read_csv(filename)
    except Exception as e:
        print(f"\nErro ao ler arquivo de prefeituras: {str(e)}")
        return pd.DataFrame()

def load_endpoints(filename):
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            return [line.strip() for line in file if line.strip()]
    except Exception as e:
        print(f"\nErro ao ler arquivo de endpoints: {str(e)}")
        return []

if __name__ == "__main__":
    main()