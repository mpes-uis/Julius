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
    error_log_file = os.path.join("..", "logs", "portaltp_errors.log")

    if not all(os.path.exists(f) for f in [endpoints_file, prefeituras_file]):
        print("\n🔴 ERRO: Arquivos necessários não encontrados.")
        return

    os.makedirs(os.path.dirname(db_file), exist_ok=True)
    os.makedirs(os.path.dirname(error_log_file), exist_ok=True)

    endpoints = load_endpoints(endpoints_file)
    prefeituras = load_prefeituras(prefeituras_file)
    prefeituras_portaltp = prefeituras[prefeituras['empresa'] == 'portaltp']

    if prefeituras_portaltp.empty:
        print("\n🔴 Nenhuma prefeitura com empresa 'portaltp' encontrada.")
        return

    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    for endpoint in endpoints:
        endpoint_name = endpoint.split('/')[-1].replace('Get', '').lower()
        
        print(f"\n{'='*50}")
        print(f"🔧 Processando endpoint: {endpoint_name}")
        
        cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS {endpoint_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT
        )
        ''')
        conn.commit()

        for _, prefeitura in prefeituras_portaltp.iterrows():
            municipio = prefeitura['municipio']
            prefeitura_nome = prefeitura['prefeitura']
            base_url = prefeitura['url']
            
            print(f"\n🏛️ Prefeitura: {prefeitura_nome} ({municipio})")
            
            for ano in anos:
                for mes in meses:
                    print(f"📅 {mes:02d}/{ano}", end=' ', flush=True)
                    url = f"{base_url}/{endpoint}?ano={ano}&mes={mes:02d}"
                    
                    try:
                        response = requests.get(url, timeout=10)
                        response.raise_for_status()
                        dados = response.json()
                        
                        df = pd.DataFrame(dados)
                        
                        if not df.empty:
                            if 'municipio' not in df.columns:
                                df['municipio'] = municipio
                            
                            if 'prefeitura' not in df.columns:
                                df['prefeitura'] = prefeitura_nome
                            
                            cursor.execute(f"PRAGMA table_info({endpoint_name})")
                            existing_columns = [col[1] for col in cursor.fetchall()]
                            
                            for column in df.columns:
                                if column not in existing_columns and column != 'id':
                                    col_type = 'TEXT'
                                    if pd.api.types.is_numeric_dtype(df[column]):
                                        col_type = 'REAL'
                                    elif pd.api.types.is_integer_dtype(df[column]):
                                        col_type = 'INTEGER'
                                    
                                    cursor.execute(f"ALTER TABLE {endpoint_name} ADD COLUMN {column} {col_type}")
                                    conn.commit()
                            
                            df.to_sql(endpoint_name, conn, if_exists='append', index=False)
                            
                    except Exception as e:
                        print(f"🔴 ERRO: {str(e)}", end=' ')
                        with open(error_log_file, 'a') as f:
                            f.write(f"{url}|{str(e)}\n")
                    
                    sleep(delay)
    
    conn.close()

    print("\n\n✅ PROCESSAMENTO CONCLUÍDO!")
    print(f"💾 Dados salvos no arquivo: {db_file}")
    
    if endpoints:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        
        print("\n📊 Tabelas criadas no banco de dados:")
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        for table in cursor.fetchall():
            print(f"\n📋 Tabela: {table[0]}")
            cursor.execute(f"SELECT * FROM {table[0]} LIMIT 1")
            columns = [description[0] for description in cursor.description]
            print("🔡 Colunas:", columns)
        
        conn.close()

def load_prefeituras(filename):
    try:
        return pd.read_csv(filename)
    except Exception as e:
        print(f"\n🔴 ERRO ao ler arquivo de prefeituras: {str(e)}")
        return pd.DataFrame()

def load_endpoints(filename):
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            return [line.strip() for line in file if line.strip()]
    except Exception as e:
        print(f"\n🔴 ERRO ao ler arquivo de endpoints: {str(e)}")
        return []

if __name__ == "__main__":
    main()