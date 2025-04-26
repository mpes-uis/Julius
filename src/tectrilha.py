import requests
import pandas as pd
import sqlite3
import time
import json
from datetime import datetime
import sys
import os

REQUEST_TIMEOUT = 60
DELAY_BETWEEN_REQUESTS = 2
MAX_RETRIES = 5
RETRY_DELAY = 10

def main():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    prefeituras_file = os.path.join(base_dir, "data", "prefeituras.csv")
    assuntos_file = os.path.join(base_dir, "data", "assuntos_tectrilha.csv")
    db_file = os.path.join(base_dir, "bds", "tectrilha.db")
    error_log_file = os.path.join(base_dir, "logs", "tectrilha_errors.log")

    if not all(os.path.exists(f) for f in [prefeituras_file, assuntos_file]):
        print("\n🔴 ERRO: Arquivos necessários não encontrados.")
        print(f"🔍 Procurando em: {prefeituras_file}")
        print(f"🔍 Procurando em: {assuntos_file}")
        return

    os.makedirs(os.path.dirname(db_file), exist_ok=True)
    os.makedirs(os.path.dirname(error_log_file), exist_ok=True)

    print("🚀 Iniciando processo de extração de dados Tectrilha")
    
    prefeituras, assuntos = load_data(prefeituras_file, assuntos_file)
    conn = setup_database(db_file)
    
    tectrilha_prefs = prefeituras[prefeituras['empresa'] == 'Tectrilha']
    for _, prefeitura in tectrilha_prefs.iterrows():
        process_prefeitura(prefeitura, assuntos, conn, error_log_file)
    
    conn.close()
    
    print("\n🎉 PROCESSAMENTO CONCLUÍDO!")
    print(f"💾 Banco de dados disponível em: {db_file}")

def load_data(prefeituras_file, assuntos_file):
    try:
        prefeituras = pd.read_csv(prefeituras_file)
        assuntos = pd.read_csv(assuntos_file)
        prefeituras['unidadegestora'] = prefeituras['unidadegestora'].fillna(0)
        return prefeituras, assuntos
    except Exception as e:
        print(f"🔴 ERRO ao carregar arquivos CSV: {str(e)}")
        sys.exit(1)

def setup_database(db_file):
    return sqlite3.connect(db_file)

def build_url(base_url, assunto, parametros, unidadegestora, ano, periodo=None):
    if pd.isna(parametros) or not parametros.strip():
        return f"{base_url.rstrip('/')}/{assunto}"
    
    try:
        ug_id = int(float(unidadegestora)) if not pd.isna(unidadegestora) else 0
    except (ValueError, TypeError):
        ug_id = 0
    
    params = parametros
    params = params.replace("{unidadeGestoraId}", str(ug_id))
    params = params.replace("{exercicio}", str(ano))
    
    if periodo and ('{periodo}' in params):
        params = params.replace("{periodo}", str(periodo))
    
    return f"{base_url.rstrip('/')}/{assunto}{params}"

def process_prefeitura(prefeitura, assuntos, conn, error_log_file):
    prefeitura_nome = prefeitura['prefeitura']
    municipio = prefeitura['municipio']
    print(f"\n🏛️ Processando prefeitura: {prefeitura_nome} ({municipio})")
    
    for _, assunto in assuntos.iterrows():
        assunto_nome = assunto['assunto']
        print(f"\n📌 Assunto: {assunto_nome}")
        
        if assunto_nome in ["despesa", "captacoes", "bensmoveis", "receitas", "diarias", "convenios", "passagens", "contratos"]:
            years = [2023, 2024]
            periods = [None]
        elif assunto_nome == "bensimoveis":
            years = [0]
            periods = [None]
        elif assunto_nome == "pessoal":
            years = [2023, 2024]
            periods = range(1, 13)
        else:
            continue
        
        for ano in years:
            for periodo in periods:
                if (ano == datetime.now().year and 
                    periodo and periodo > datetime.now().month and
                    assunto_nome == "pessoal"):
                    continue
                
                url = build_url(
                    prefeitura['url'],
                    assunto_nome,
                    assunto['parametros'],
                    prefeitura['unidadegestora'],
                    ano,
                    periodo
                )
                
                print(f"🌐 URL: {url}")
                
                success, data = make_request(url)
                
                if not success:
                    with open(error_log_file, 'a') as f:
                        f.write(f"{url}|Falha após {MAX_RETRIES} tentativas\n")
                    continue
                
                if data:
                    df = transform_json_to_dataframe(data, assunto_nome)
                    
                    if df is not None and not df.empty:
                        df['prefeitura'] = prefeitura_nome
                        df['municipio'] = municipio
                        df['ano'] = ano
                        if periodo:
                            df['mes'] = periodo
                        
                        store_dataframe(conn, df, assunto_nome)
                
                time.sleep(DELAY_BETWEEN_REQUESTS)

def make_request(url):
    headers = {'User-Agent': 'Mozilla/5.0'}
    last_error = None
    
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return True, response.json()
            
        except requests.exceptions.RequestException as e:
            last_error = str(e)
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))
            continue
        except json.JSONDecodeError as e:
            last_error = f"Erro de JSON: {str(e)}"
            continue
        except Exception as e:
            last_error = f"Erro inesperado: {str(e)}"
            continue
    
    print(f"🔴 Falha após {MAX_RETRIES} tentativas: {last_error}")
    return False, None

def transform_json_to_dataframe(data, assunto_nome):
    try:
        if isinstance(data, list):
            return pd.DataFrame(data)
        
        if isinstance(data, dict) and all(isinstance(v, list) for v in data.values()):
            lengths = [len(v) for v in data.values()]
            if len(set(lengths)) == 1:
                return pd.DataFrame({k: pd.Series(v) for k, v in data.items()})
        
        if isinstance(data, dict):
            return pd.DataFrame([data])
        
        print(f"⚠️ Formato inesperado para o assunto {assunto_nome}")
        return None
        
    except Exception as e:
        print(f"🔴 ERRO ao transformar JSON em DataFrame: {str(e)}")
        return None

def store_dataframe(conn, df, table_name):
    try:
        for col in ['ano', 'mes', 'prefeitura', 'municipio']:
            if col in df.columns:
                df = df.drop(columns=[col])
        
        for col in df.columns:
            if df[col].apply(lambda x: isinstance(x, (list, dict))).any():
                df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x)
        
        cursor = conn.cursor()
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
        table_exists = cursor.fetchone() is not None
        
        if not table_exists:
            df.to_sql(name=table_name, con=conn, if_exists='fail', index=False)
        else:
            cursor.execute(f"PRAGMA table_info({table_name})")
            existing_columns = [column[1] for column in cursor.fetchall()]
            
            for column in df.columns:
                if column not in existing_columns:
                    try:
                        col_type = 'TEXT'
                        if pd.api.types.is_numeric_dtype(df[column]):
                            col_type = 'REAL'
                        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column} {col_type}")
                    except sqlite3.OperationalError as e:
                        if "duplicate column" not in str(e):
                            raise
        
        df.to_sql(name=table_name, con=conn, if_exists='append', index=False)
        print(f"💾 Dados armazenados na tabela '{table_name}'")

    except Exception as e:
        print(f"🔴 ERRO ao armazenar dados: {str(e)}")
        print(f"🔡 Colunas problemáticas: {df.columns.tolist()}")

if __name__ == "__main__":
    main()