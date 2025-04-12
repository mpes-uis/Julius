import requests
import pandas as pd
import sqlite3
import time
import json
from datetime import datetime
import sys

# Configurações
DB_PATH = 'transparencia_tectrilha.db'
REQUEST_TIMEOUT = 60
DELAY_BETWEEN_REQUESTS = 2
MAX_RETRIES = 5
RETRY_DELAY = 10

def load_data():
    try:
        prefeituras = pd.read_csv('prefeituras.csv')
        assuntos = pd.read_csv('assuntos_tectrilha.csv')
        
        # Preencher NaN na coluna unidadegestora com 0
        prefeituras['unidadegestora'] = prefeituras['unidadegestora'].fillna(0)
        
        return prefeituras, assuntos
    except Exception as e:
        print(f"❌ Erro ao carregar arquivos CSV: {str(e)}")
        sys.exit(1)

def setup_database():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS prefeituras (
        id INTEGER PRIMARY KEY,
        nome TEXT,
        municipio TEXT,
        url TEXT,
        unidade_gestora INTEGER
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS assuntos (
        id INTEGER PRIMARY KEY,
        nome TEXT,
        parametros TEXT
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS requisicoes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        prefeitura_id INTEGER,
        assunto_id INTEGER,
        exercicio INTEGER,
        periodo TEXT,
        url TEXT UNIQUE,
        dados TEXT,
        status TEXT,
        data_hora TEXT,
        tentativas INTEGER DEFAULT 0,
        FOREIGN KEY (prefeitura_id) REFERENCES prefeituras (id),
        FOREIGN KEY (assunto_id) REFERENCES assuntos (id)
    )
    ''')
    
    conn.commit()
    return conn

def populate_basic_tables(conn, prefeituras, assuntos):
    cursor = conn.cursor()
    
    tectrilha_prefs = prefeituras[prefeituras['empresa'] == 'Tectrilha']
    for _, row in tectrilha_prefs.iterrows():
        cursor.execute('''
        INSERT OR IGNORE INTO prefeituras (id, nome, municipio, url, unidade_gestora)
        VALUES (?, ?, ?, ?, ?)
        ''', (row['id'], row['prefeitura'], row['municipio'], row['url'], int(row['unidadegestora'])))
    
    for _, row in assuntos.iterrows():
        cursor.execute('''
        INSERT OR IGNORE INTO assuntos (id, nome, parametros)
        VALUES (?, ?, ?)
        ''', (row['id'], row['assunto'], row['parametros']))
    
    conn.commit()

def build_url(base_url, assunto, parametros, unidadegestora, ano, periodo=None):
    if pd.isna(parametros) or not parametros.strip():
        return f"{base_url.rstrip('/')}/{assunto}"
    
    # Converter unidadegestora para int, tratando NaN como 0
    try:
        ug_id = int(float(unidadegestora)) if not pd.isna(unidadegestora) else 0
    except (ValueError, TypeError):
        ug_id = 0
    
    params = parametros
    params = params.replace("{unidadeGestoraId}", str(ug_id))
    params = params.replace("{exercicio}", str(ano))
    
    if periodo and ('{periodo}' in params or '{periodo}' in params):
        params = params.replace("{periodo}", str(periodo))
        params = params.replace("{periodo}", str(periodo))
    
    return f"{base_url.rstrip('/')}/{assunto}{params}"

def make_request(url, prefeitura_id, assunto_id, exercicio, periodo=None):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('SELECT status FROM requisicoes WHERE url = ?', (url,))
    result = cursor.fetchone()
    if result and result[0] == 'sucesso':
        conn.close()
        return True
    
    headers = {'User-Agent': 'Mozilla/5.0'}
    last_error = None
    
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            
            data = response.json()
            dados_json = json.dumps(data, ensure_ascii=False)
            
            now = datetime.now().isoformat()
            cursor.execute('''
            INSERT OR REPLACE INTO requisicoes 
            (prefeitura_id, assunto_id, exercicio, periodo, url, dados, status, data_hora, tentativas)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                prefeitura_id,
                assunto_id,
                exercicio,
                periodo,
                url,
                dados_json,
                'sucesso',
                now,
                attempt + 1
            ))
            conn.commit()
            conn.close()
            return True
            
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
    
    now = datetime.now().isoformat()
    cursor.execute('''
    INSERT OR REPLACE INTO requisicoes 
    (prefeitura_id, assunto_id, exercicio, periodo, url, dados, status, data_hora, tentativas)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        prefeitura_id,
        assunto_id,
        exercicio,
        periodo,
        url,
        None,
        f'erro: {last_error}',
        now,
        MAX_RETRIES
    ))
    conn.commit()
    conn.close()
    return False

def process_prefeitura(prefeitura, assuntos, conn):
    prefeitura_nome = prefeitura['prefeitura']
    print(f"\n🔍 Processando prefeitura: {prefeitura_nome}")
    
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
                
                success = make_request(
                    url,
                    prefeitura['id'],
                    assunto['id'],
                    ano,
                    periodo
                )
                
                status = "✅ Sucesso" if success else "❌ Falha"
                print(f"{status} - Tentativa concluída")
                time.sleep(DELAY_BETWEEN_REQUESTS)

def main():
    print("🚀 Iniciando processo de extração de dados Tectrilha")
    
    prefeituras, assuntos = load_data()
    
    conn = setup_database()
    populate_basic_tables(conn, prefeituras, assuntos)
    conn.close()
    
    tectrilha_prefs = prefeituras[prefeituras['empresa'] == 'Tectrilha']
    
    for _, prefeitura in tectrilha_prefs.iterrows():
        process_prefeitura(prefeitura, assuntos, sqlite3.connect(DB_PATH))
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_requisicoes_prefeitura ON requisicoes(prefeitura_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_requisicoes_assunto ON requisicoes(assunto_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_requisicoes_exercicio ON requisicoes(exercicio)')
    conn.commit()
    conn.close()
    
    print("\n🎉 Processo concluído com sucesso!")
    print(f"📊 Banco de dados disponível em: {DB_PATH}")

if __name__ == "__main__":
    main()