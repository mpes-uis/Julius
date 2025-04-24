import requests
import pandas as pd
import sqlite3
import time
import json
from datetime import datetime
import sys
import os

# Configurações
REQUEST_TIMEOUT = 60
DELAY_BETWEEN_REQUESTS = 2
MAX_RETRIES = 5
RETRY_DELAY = 10

def main():
    # Configura caminhos baseado na estrutura de pastas
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    prefeituras_file = os.path.join(base_dir, "data", "prefeituras.csv")
    assuntos_file = os.path.join(base_dir, "data", "assuntos_tectrilha.csv")
    db_file = os.path.join(base_dir, "bds", "tectrilha.db")

    # Verifica arquivos necessários
    if not all(os.path.exists(f) for f in [prefeituras_file, assuntos_file]):
        print("\nErro: Arquivos necessários não encontrados.")
        print(f"Procurando em: {prefeituras_file}")
        print(f"Procurando em: {assuntos_file}")
        return

    # Cria pasta para o banco de dados se não existir
    os.makedirs(os.path.dirname(db_file), exist_ok=True)

    print("🚀 Iniciando processo de extração de dados Tectrilha")
    
    # Carrega dados
    prefeituras, assuntos = load_data(prefeituras_file, assuntos_file)
    
    # Configura banco de dados
    conn = setup_database(db_file)
    
    # Processa prefeituras Tectrilha
    tectrilha_prefs = prefeituras[prefeituras['empresa'] == 'Tectrilha']
    for _, prefeitura in tectrilha_prefs.iterrows():
        process_prefeitura(prefeitura, assuntos, conn)
    
    conn.close()
    
    print("\n🎉 Processo concluído com sucesso!")
    print(f"📊 Banco de dados disponível em: {db_file}")

def load_data(prefeituras_file, assuntos_file):
    try:
        prefeituras = pd.read_csv(prefeituras_file)
        assuntos = pd.read_csv(assuntos_file)
        
        # Preencher NaN na coluna unidadegestora com 0
        prefeituras['unidadegestora'] = prefeituras['unidadegestora'].fillna(0)
        
        return prefeituras, assuntos
    except Exception as e:
        print(f"❌ Erro ao carregar arquivos CSV: {str(e)}")
        sys.exit(1)

def setup_database(db_file):
    conn = sqlite3.connect(db_file)
    return conn

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

def process_prefeitura(prefeitura, assuntos, conn):
    prefeitura_nome = prefeitura['prefeitura']
    municipio = prefeitura['municipio']
    print(f"\n🔍 Processando prefeitura: {prefeitura_nome} ({municipio})")
    
    for _, assunto in assuntos.iterrows():
        assunto_nome = assunto['assunto']
        print(f"\n📌 Assunto: {assunto_nome}")
        
        # Define anos e períodos conforme o assunto
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
                
                if success and data:
                    # Transforma o JSON em DataFrame
                    df = transform_json_to_dataframe(data, assunto_nome)
                    
                    if df is not None and not df.empty:
                        # Adiciona colunas de identificação
                        df['prefeitura'] = prefeitura_nome
                        df['municipio'] = municipio
                        df['ano'] = ano
                        if periodo:
                            df['mes'] = periodo
                        
                        # Armazena no banco de dados
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
    
    print(f"❌ Falha após {MAX_RETRIES} tentativas: {last_error}")
    return False, None

def transform_json_to_dataframe(data, assunto_nome):
    try:
        # Se for uma lista de objetos, cria DataFrame diretamente
        if isinstance(data, list):
            return pd.DataFrame(data)
        
        # Se for um objeto com arrays de mesmo tamanho
        if isinstance(data, dict) and all(isinstance(v, list) for v in data.values()):
            lengths = [len(v) for v in data.values()]
            if len(set(lengths)) == 1:  # Todos os arrays têm o mesmo tamanho
                return pd.DataFrame({k: pd.Series(v) for k, v in data.items()})
        
        # Se for um único objeto, coloca em uma lista
        if isinstance(data, dict):
            return pd.DataFrame([data])
        
        print(f"⚠️ Formato inesperado para o assunto {assunto_nome}")
        return None
        
    except Exception as e:
        print(f"❌ Erro ao transformar JSON em DataFrame: {str(e)}")
        return None

def store_dataframe(conn, df, table_name, prefeitura_nome=None, municipio=None, ano=None, periodo=None):
    """
    Armazena DataFrame no SQLite SEM adicionar colunas de metadados
    Args:
        prefeitura_nome, municipio, ano, periodo: usados apenas para lógica interna (não armazenados)
    """
    try:
        # Remove colunas de metadados se existirem
        for col in ['ano', 'mes', 'prefeitura', 'municipio']:
            if col in df.columns:
                df = df.drop(columns=[col])
        
        # Converte listas/dicionários para JSON
        for col in df.columns:
            if df[col].apply(lambda x: isinstance(x, (list, dict))).any():
                df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x)
        
        # Verifica e adapta estrutura da tabela
        cursor = conn.cursor()
        
        # 1. Verifica se a tabela existe
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
        table_exists = cursor.fetchone() is not None
        
        if not table_exists:
            # Cria nova tabela com a estrutura do DataFrame
            df.to_sql(
                name=table_name,
                con=conn,
                if_exists='fail',
                index=False
            )
        else:
            # Adiciona colunas faltantes
            cursor.execute(f"PRAGMA table_info({table_name})")
            existing_columns = [column[1] for column in cursor.fetchall()]
            
            for column in df.columns:
                if column not in existing_columns:
                    try:
                        col_type = 'TEXT'  # Tipo padrão
                        if pd.api.types.is_numeric_dtype(df[column]):
                            col_type = 'REAL'
                        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column} {col_type}")
                    except sqlite3.OperationalError as e:
                        if "duplicate column" not in str(e):
                            raise
        
        # Insere os dados
        df.to_sql(
            name=table_name,
            con=conn,
            if_exists='append',
            index=False
        )
        print(f"💾 Dados armazenados na tabela '{table_name}'")

    except Exception as e:
        print(f"❌ Erro ao armazenar dados: {str(e)}")
        print(f"Colunas problemáticas: {df.columns.tolist()}")
def clean_column_name(col):
    # Remove caracteres especiais e substitui espaços
    return ''.join(c if c.isalnum() else '_' for c in str(col))

def get_sql_type(series):
    if pd.api.types.is_integer_dtype(series):
        return 'INTEGER'
    elif pd.api.types.is_numeric_dtype(series):
        return 'REAL'
    return 'TEXT'

if __name__ == "__main__":
    main()