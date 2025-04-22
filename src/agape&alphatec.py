import requests
import pandas as pd
import sqlite3
from time import sleep
import os
import json

def main():
    anos = [2023, 2024]
    meses = [1, 2]  
    delay = 1

    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    endpoints_file = os.path.join(base_dir, "data", "endpoints_agape.txt")  
    prefeituras_file = os.path.join(base_dir, "data", "prefeituras.csv")
    db_file = os.path.join(base_dir, "bds", "agape&alphatec.db")  

    if not all(os.path.exists(f) for f in [endpoints_file, prefeituras_file]):
        print("\nErro: Arquivos necessários não encontrados.")
        print(f"Procurando em: {endpoints_file}")
        print(f"Procurando em: {prefeituras_file}")
        return

    os.makedirs(os.path.dirname(db_file), exist_ok=True)

    endpoints = load_endpoints(endpoints_file)
    prefeituras = load_prefeituras(prefeituras_file)

    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    for endpoint in endpoints:
        endpoint_name = endpoint.split('/')[-1].replace('Get', '').lower()

        print(f"\n{'='*50}")
        print(f"Processando endpoint: {endpoint_name}")

        cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS {endpoint_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT
        )
        ''')
        conn.commit()

        processar_empresa(conn, cursor, prefeituras, endpoint, endpoint_name, 'Agape', anos, meses, delay)
        processar_empresa(conn, cursor, prefeituras, endpoint, endpoint_name, 'Alphatec', anos, meses, delay)

    conn.close()

    print("\n\nProcessamento concluído!")
    print(f"Dados salvos no arquivo: {db_file}")

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

def processar_empresa(conn, cursor, prefeituras, endpoint, endpoint_name, empresa, anos, meses, delay):
    prefeituras_empresa = prefeituras[prefeituras['empresa'] == empresa]  

    if prefeituras_empresa.empty:
        return

    for _, prefeitura in prefeituras_empresa.iterrows():
        municipio = prefeitura['municipio']
        prefeitura_nome = prefeitura['prefeitura']
        base_url = prefeitura['url'].rstrip('/')  

        print(f"\nPrefeitura: {prefeitura_nome} ({municipio}) - {empresa}")

        for ano in anos:
            for mes in meses:
                print(f"{mes:02d}/{ano}", end=' ', flush=True)
                url = f"{base_url}/{endpoint}?ano={ano}&mes={mes:02d}"

                try:
                    response = requests.get(url, timeout=10)
                    response.raise_for_status()

                    try:
                        dados = response.json()
                    except Exception:
                        dados = json.loads(response.content.decode('utf-8-sig'))

                    if isinstance(dados, dict):
                        if all(isinstance(v, list) for v in dados.values()):
                            lengths = [len(v) for v in dados.values()]
                            if len(set(lengths)) > 1:
                                raise ValueError("All arrays must be of the same length")
                            dados = [dict(zip(dados.keys(), values)) for values in zip(*dados.values())]
                        else:
                            dados = [dados]

                    if not isinstance(dados, list):
                        raise ValueError("Formato inesperado (não é lista)")

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

                        for col in df.columns:
                            df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x)

                        df.to_sql(endpoint_name, conn, if_exists='append', index=False)

                except Exception as e:
                    print(f" [Erro: {str(e)}]", end=' ')

                sleep(delay)

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
