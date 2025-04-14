import requests
import pandas as pd
import sqlite3
from time import sleep
import os

def main():
    # Configurações
    anos = [2023, 2024]
    meses = [1, 2]  
    delay = 1
    endpoints_file = os.path.join("..", "data", "endpoints_portaltp.txt")  
    prefeituras_file = os.path.join("..", "data", "prefeituras.csv")      
    db_folder = os.path.join("..", "bds", "dados_transparencia_portaltp") 

    # Verifica arquivos necessários
    if not all(os.path.exists(f) for f in [endpoints_file, prefeituras_file]):
        print("\nErro: Arquivos necessários não encontrados.")
        return

    # Cria pasta para os bancos de dados se não existir
    os.makedirs(db_folder, exist_ok=True)

    # Carrega dados
    endpoints = load_endpoints(endpoints_file)
    prefeituras = load_prefeituras(prefeituras_file)
    prefeituras_portaltp = prefeituras[prefeituras['empresa'] == 'portaltp']

    if prefeituras_portaltp.empty:
        print("\nNenhuma prefeitura com empresa 'portaltp' encontrada.")
        return

    # Processa por endpoint
    for endpoint in endpoints:
        endpoint_name = endpoint.split('/')[-1].replace('Get', '')
        db_file = os.path.join(db_folder, f"{endpoint_name}_portaltp.db")
        
        print(f"\n{'='*50}")
        print(f"Processando endpoint: {endpoint_name}")
        
        # Cria conexão com o banco de dados específico para este endpoint
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        
        # Cria tabela se não existir (sem timestamp)
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS transparencia (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            municipio TEXT,
            prefeitura TEXT,
            ano INTEGER,
            mes INTEGER,
            dados TEXT
        )
        ''')
        conn.commit()

        # Processa todas as prefeituras para este endpoint
        for _, prefeitura in prefeituras_portaltp.iterrows():
            municipio = prefeitura['municipio']  # Nome do município da coluna 'municipio'
            prefeitura_nome = prefeitura['prefeitura']
            base_url = prefeitura['url']
            
            print(f"\nPrefeitura: {prefeitura_nome} ({municipio})")
            
            for ano in anos:
                for mes in meses:
                    print(f"{mes:02d}/{ano}", end=' ', flush=True)
                    
                    # Obtém dados da API
                    url = f"{base_url}/{endpoint}?ano={ano}&mes={mes:02d}"
                    try:
                        response = requests.get(url, timeout=10)
                        response.raise_for_status()
                        dados = response.json()
                        
                        # Insere no SQLite com o nome do município
                        cursor.execute('''
                        INSERT INTO transparencia 
                        (municipio, prefeitura, ano, mes, dados) 
                        VALUES (?, ?, ?, ?, ?)
                        ''', (municipio, prefeitura_nome, ano, mes, str(dados)))
                        
                        conn.commit()
                        
                    except Exception as e:
                        print(f" [Erro: {str(e)}]", end=' ')
                    
                    sleep(delay)
        
        # Fecha conexão com o banco deste endpoint
        conn.close()

    print("\n\nProcessamento concluído!")
    print(f"Dados salvos na pasta: {db_folder}")
    
    # Exemplo de consulta (mostra o primeiro banco de dados encontrado)
    if endpoints:
        first_endpoint = endpoints[0].split('/')[-1].replace('Get', '')
        db_file = os.path.join(db_folder, f"{first_endpoint}_portaltp.db")
        if os.path.exists(db_file):
            print(f"\nExemplo de dados armazenados em {first_endpoint}_portaltp.db:")
            conn = sqlite3.connect(db_file)
            cursor = conn.cursor()
            cursor.execute("SELECT municipio, prefeitura, ano, mes, COUNT(*) as registros FROM transparencia GROUP BY municipio, prefeitura, ano, mes")
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