import requests
import pandas as pd
import sqlite3
from time import sleep
import os

def main():
    # Configurações
    anos = [2024, 2025]
    meses = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]  
    delay = 1
    endpoints_file = os.path.join("..", "data", "endpoints_portaltp.txt")  
    prefeituras_file = os.path.join("..", "data", "prefeituras.csv")      
    db_file = os.path.join("..", "bds", "portaltp.db") 

    # Verifica arquivos necessários
    if not all(os.path.exists(f) for f in [endpoints_file, prefeituras_file]):
        print("\nErro: Arquivos necessários não encontrados.")
        return

    # Cria pasta para o banco de dados se não existir
    os.makedirs(os.path.dirname(db_file), exist_ok=True)

    # Carrega dados
    endpoints = load_endpoints(endpoints_file)
    prefeituras = load_prefeituras(prefeituras_file)
    prefeituras_portaltp = prefeituras[prefeituras['empresa'] == 'portaltp']

    if prefeituras_portaltp.empty:
        print("\nNenhuma prefeitura com empresa 'portaltp' encontrada.")
        return

    # Cria conexão com o único banco de dados
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Processa por endpoint 
    for endpoint in endpoints:
        endpoint_name = endpoint.split('/')[-1].replace('Get', '').lower()
        
        print(f"\n{'='*50}")
        print(f"Processando endpoint: {endpoint_name}")
        
        # Cria tabela para este endpoint se não existir 
        cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS {endpoint_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT
            -- As colunas serão adicionadas dinamicamente conforme os dados
        )
        ''')
        conn.commit()

        # Processa todas as prefeituras para este endpoint
        for _, prefeitura in prefeituras_portaltp.iterrows():
            municipio = prefeitura['municipio']
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
                        
                        # Converte JSON para DataFrame
                        df = pd.DataFrame(dados)
                        
                        if not df.empty:
                            # Adiciona apenas a coluna de município (se necessário)
                            if 'municipio' not in df.columns:
                                df['municipio'] = municipio
                            
                            # Adiciona coluna de prefeitura (se necessário)
                            if 'prefeitura' not in df.columns:
                                df['prefeitura'] = prefeitura_nome
                            
                            # Verifica e adiciona novas colunas se necessário
                            cursor.execute(f"PRAGMA table_info({endpoint_name})")
                            existing_columns = [col[1] for col in cursor.fetchall()]
                            
                            for column in df.columns:
                                if column not in existing_columns and column != 'id':
                                    # Determina o tipo da coluna
                                    col_type = 'TEXT'  # padrão
                                    if pd.api.types.is_numeric_dtype(df[column]):
                                        col_type = 'REAL'
                                    elif pd.api.types.is_integer_dtype(df[column]):
                                        col_type = 'INTEGER'
                                    
                                    cursor.execute(f"ALTER TABLE {endpoint_name} ADD COLUMN {column} {col_type}")
                                    conn.commit()
                            
                            # Insere os dados no banco (sem índice)
                            df.to_sql(endpoint_name, conn, if_exists='append', index=False)
                            
                    except Exception as e:
                        print(f" [Erro: {str(e)}]", end=' ')
                    
                    sleep(delay)
    
    # Fecha conexão com o banco de dados
    conn.close()

    print("\n\nProcessamento concluído!")
    print(f"Dados salvos no arquivo: {db_file}")
    
    # Exemplo de consulta (mostra as tabelas criadas)
    if endpoints:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        
        print("\nTabelas criadas no banco de dados:")
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        for table in cursor.fetchall():
            print(f"\nTabela: {table[0]}")
            cursor.execute(f"SELECT * FROM {table[0]} LIMIT 1")
            columns = [description[0] for description in cursor.description]
            print("Colunas:", columns)
        
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