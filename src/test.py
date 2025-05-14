import requests
import pandas as pd
import sqlite3
from time import sleep, time
import os
from datetime import datetime
from urllib.parse import urlparse
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import re

# Configurações de diretórios
def setup_directories():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(base_dir, 'data')
    bds_dir = os.path.join(base_dir, 'bds')
    logs_dir = os.path.join(base_dir, 'logs')
    
    os.makedirs(data_dir, exist_ok=True)
    os.makedirs(bds_dir, exist_ok=True)
    os.makedirs(logs_dir, exist_ok=True)
    
    return {
        'base_dir': base_dir,
        'data_dir': data_dir,
        'bds_dir': bds_dir,
        'logs_dir': logs_dir,
        'prefeituras_file': os.path.join(data_dir, 'prefeituras.csv'),
        'assuntos_file': os.path.join(data_dir, 'assuntos_tectrilha.csv'),
        'db_file': os.path.join(bds_dir, 'tectrilha.db'),
        'error_log_file': os.path.join(logs_dir, 'tectrilha_errors.log'),
        'execution_log_file': os.path.join(logs_dir, 'tectrilha_execution.log'),
        'last_run_file': os.path.join(logs_dir, 'tectrilha_last_run.txt')
    }

# Função para normalizar URLs
def normalizar_url(url):
    """Garante que a URL base esteja no formato correto sem duplicações"""
    if not url:
        return ''
    
    url = url.strip()
    # Remover espaços e caracteres especiais
    url = re.sub(r'\s+', '', url)
    # Remover todas as ocorrências de /api no final
    url = re.sub(r'/api/?$', '', url)
    # Adicionar /api apenas uma vez no final
    url = url.rstrip('/') + '/api'
    return url

# Função para obter período do usuário
def get_periodo_usuario():
    print("\n" + "="*50)
    print("DEFINIR PERÍODO DE EXTRAÇÃO")
    print("="*50)

    while True:
        try:
            inicio = input("Data inicial (MM/AAAA): ").split('/')
            mes_inicio = int(inicio[0])
            ano_inicio = int(inicio[1])

            fim = input("Data final (MM/AAAA): ").split('/')
            mes_fim = int(fim[0])
            ano_fim = int(fim[1])

            if (ano_inicio > ano_fim) or (ano_inicio == ano_fim and mes_inicio > mes_fim):
                print("🔴 Data inicial deve ser anterior à data final. Tente novamente.")
                continue

            if mes_inicio < 1 or mes_inicio > 12 or mes_fim < 1 or mes_fim > 12:
                print("🔴 Mês inválido. Deve ser entre 1 e 12. Tente novamente.")
                continue

            return (ano_inicio, mes_inicio), (ano_fim, mes_fim)

        except (ValueError, IndexError):
            print("🔴 Formato inválido. Use MM/AAAA (ex: 01/2024). Tente novamente.")

# Função para construir URLs
def build_tectrilha_urls(ano_inicio, mes_inicio, ano_fim, mes_fim, prefeituras_file, assuntos_file):
    try:
        prefeituras_df = pd.read_csv(prefeituras_file)
        assuntos_df = pd.read_csv(assuntos_file)
    except Exception as e:
        print(f"🔴 Erro ao ler arquivos CSV: {str(e)}")
        return None

    prefeituras_tectrilha = prefeituras_df[prefeituras_df['empresa'] == 'Tectrilha']
    
    urls = {}
    meses_periodo = generate_months_range((ano_inicio, mes_inicio), (ano_fim, mes_fim))
    
    for _, prefeitura in prefeituras_tectrilha.iterrows():
        prefeitura_nome = prefeitura['prefeitura']
        base_url = normalizar_url(prefeitura['url'])
        unidade_gestora = str(prefeitura['unidadegestora']).split('.')[0]  # Remove .0 se existir
        
        urls[prefeitura_nome] = {
            'municipio': prefeitura['municipio'],
            'base_url': base_url,
            'unidade_gestora': unidade_gestora,
            'urls': {}
        }
        
        for _, assunto in assuntos_df.iterrows():
            assunto_nome = assunto['assunto'].strip()
            parametros_template = assunto['parametros']
            urls_assunto = []
            
            for ano, mes in meses_periodo:
                # Construir URL corretamente evitando duplicação
                if base_url.endswith('/api'):
                    url_completa = f"{base_url}/{assunto_nome}"
                else:
                    url_completa = f"{base_url}api/{assunto_nome}"
                
                if pd.notna(parametros_template):
                    parametros = parametros_template.strip()
                    if '{unidadeGestoraId}' in parametros:
                        parametros = parametros.replace('{unidadeGestoraId}', unidade_gestora)
                    if '{exercicio}' in parametros:
                        parametros = parametros.replace('{exercicio}', str(ano))
                    if '{periodo}' in parametros:
                        parametros = parametros.replace('{periodo}', str(mes))
                    
                    if parametros:
                        url_completa += parametros
                
                # Garantir que não há barras duplas
                url_completa = re.sub(r'(?<!:)//+', '/', url_completa)
                
                urls_assunto.append({
                    'ano': ano,
                    'mes': mes,
                    'url': url_completa,
                    'assunto': assunto_nome
                })
            
            urls[prefeitura_nome]['urls'][assunto_nome] = urls_assunto
    
    return urls

# Função para gerar range de meses
def generate_months_range(data_inicio, data_fim):
    meses = []
    ano_inicio, mes_inicio = data_inicio
    ano_fim, mes_fim = data_fim
    ano_atual, mes_atual = ano_inicio, mes_inicio
    
    while (ano_atual < ano_fim) or (ano_atual == ano_fim and mes_atual <= mes_fim):
        meses.append((ano_atual, mes_atual))
        mes_atual += 1
        if mes_atual > 12:
            mes_atual = 1
            ano_atual += 1
    
    return meses

# Função para criar sessão com retry
def get_retry_session():
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[408, 429, 500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

# Função para criar tabela no banco de dados
def create_table_if_not_exists(conn, table_name, df):
    cursor = conn.cursor()
    
    # Colunas básicas que todas as tabelas devem ter
    base_columns = {
        'id': 'INTEGER PRIMARY KEY AUTOINCREMENT',
        'municipio': 'TEXT',
        'prefeitura': 'TEXT',
        'ano': 'INTEGER',
        'mes': 'INTEGER',
        'data_importacao': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP'
    }
    
    # Criar tabela com colunas básicas
    columns_def = ', '.join([f"{col} {typ}" for col, typ in base_columns.items()])
    cursor.execute(f'CREATE TABLE IF NOT EXISTS {table_name} ({columns_def})')
    
    # Verificar colunas existentes
    cursor.execute(f"PRAGMA table_info({table_name})")
    existing_columns = [col[1] for col in cursor.fetchall()]
    
    # Adicionar colunas do DataFrame que faltam
    for column in df.columns:
        if column not in existing_columns and column not in base_columns:
            col_type = 'TEXT'  # Default
            if pd.api.types.is_numeric_dtype(df[column]):
                col_type = 'REAL'
                if pd.api.types.is_integer_dtype(df[column]):
                    col_type = 'INTEGER'
            elif pd.api.types.is_datetime64_any_dtype(df[column]):
                col_type = 'TIMESTAMP'
            
            try:
                cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column} {col_type}")
            except sqlite3.OperationalError as e:
                if "duplicate column name" not in str(e):
                    raise e
    
    conn.commit()
    
    # Verificar colunas existentes
    cursor.execute(f"PRAGMA table_info({table_name})")
    existing_columns = [col[1] for col in cursor.fetchall()]
    
    # Adicionar colunas que faltam
    for column in df.columns:
        if column not in existing_columns and column != 'id':
            col_type = 'TEXT'  # Default
            if pd.api.types.is_numeric_dtype(df[column]):
                col_type = 'REAL'
                if pd.api.types.is_integer_dtype(df[column]):
                    col_type = 'INTEGER'
            elif pd.api.types.is_datetime64_any_dtype(df[column]):
                col_type = 'TIMESTAMP'
            
            try:
                cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column} {col_type}")
            except sqlite3.OperationalError as e:
                if "duplicate column name" not in str(e):
                    raise e
    
    conn.commit()

# Função principal de extração
def run_extraction(urls_dict, db_file, error_log_file):
    if not urls_dict:
        print("🔴 Nenhuma URL para processar.")
        return
    
    session = get_retry_session()
    conn = sqlite3.connect(db_file)
    
    total_urls = sum(len(assunto) for pref in urls_dict.values() 
                   for assunto in pref['urls'].values())
    processed = 0
    
    for prefeitura_nome, prefeitura_data in urls_dict.items():
        municipio = prefeitura_data['municipio']
        print(f"\n🏛️ Prefeitura: {prefeitura_nome} ({municipio})")
        
        for assunto_nome, urls_assunto in prefeitura_data['urls'].items():
            print(f"\n🔧 Processando assunto: {assunto_nome}")
            
            # Criar tabela com colunas básicas antes de processar URLs
            df_template = pd.DataFrame({
                'municipio': [municipio],
                'prefeitura': [prefeitura_nome],
                'ano': [0],
                'mes': [0]
            })
            create_table_if_not_exists(conn, assunto_nome, df_template)
            
            for url_data in urls_assunto:
                ano = url_data['ano']
                mes = url_data['mes']
                url = url_data['url']
                processed += 1
                
                print(f"📅 {mes:02d}/{ano} ({processed}/{total_urls}) - {url[:80]}...", end=' ', flush=True)
                
                # Verificar se já existe no BD
                cursor = conn.cursor()
                cursor.execute(f'''
                    SELECT 1 FROM {assunto_nome} 
                    WHERE municipio = ? AND prefeitura = ? AND ano = ? AND mes = ?
                    LIMIT 1
                ''', (municipio, prefeitura_nome, ano, mes))
                
                if cursor.fetchone():
                    print("✅ Já existe no BD")
                    continue
                
                try:
                    response = session.get(url, timeout=30)
                    response.raise_for_status()
                    
                    if not response.content.strip():
                        print("🟡 Resposta vazia. Ignorando.")
                        continue
                    
                    dados = response.json()
                    
                    if not dados:
                        print("🟡 Nenhum dado retornado.")
                        continue
                    
                    df = pd.DataFrame(dados)
                    
                    if df.empty:
                        print("🟡 DataFrame vazio.")
                        continue
                    
                    # Adicionar metadados
                    df['municipio'] = municipio
                    df['prefeitura'] = prefeitura_nome
                    df['ano'] = ano
                    df['mes'] = mes
                    
                    # Atualizar schema da tabela se necessário
                    create_table_if_not_exists(conn, assunto_nome, df)
                    
                    # Salvar dados
                    df.to_sql(assunto_nome, conn, if_exists='append', index=False)
                    print("✅ Dados salvos")
                
                except requests.exceptions.RequestException as e:
                    print(f"🔴 Erro na requisição: {str(e)}")
                    log_error(error_log_file, url, e)
                except ValueError as e:
                    print(f"🔴 Erro no JSON: {str(e)}")
                    log_error(error_log_file, url, e)
                except Exception as e:
                    print(f"🔴 Erro inesperado: {str(e)}")
                    log_error(error_log_file, url, e)
                
                sleep(0.5)  # Delay entre requisições
    
    conn.close()
    print("\n\n✅ EXTRAÇÃO CONCLUÍDA!")

# Função para log de erros
def log_error(error_log_file, url, error):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(error_log_file, 'a', encoding='utf-8') as f:
        f.write(f"{timestamp}|{url}|{type(error).__name__}|{str(error)}\n")

# Função para reprocessar URLs com erro
def run_failed_urls(error_log_file, db_file):
    if not os.path.exists(error_log_file):
        print("\n🔴 Nenhum arquivo de log de erros encontrado.")
        return
    
    # Ler URLs com erro
    failed_urls = []
    with open(error_log_file, 'r', encoding='utf-8') as f:
        for line in f:
            partes = line.strip().split('|')
            if len(partes) >= 2:
                failed_urls.append({
                    'url': partes[1],
                    'error': '|'.join(partes[2:]) if len(partes) > 2 else 'Erro desconhecido'
                })
    
    if not failed_urls:
        print("\n✅ Nenhuma URL com erro para reprocessar.")
        return
    
    print(f"\n🔧 Reprocessando {len(failed_urls)} URLs com erro")
    session = get_retry_session()
    conn = sqlite3.connect(db_file)
    success_count = 0
    temp_error_file = error_log_file + ".temp"
    
    for i, url_data in enumerate(failed_urls, 1):
        url = url_data['url']
        print(f"\n🔁 [{i}/{len(failed_urls)}] Tentando novamente: {url[:100]}...", end=' ', flush=True)
        
        try:
            # Extrair assunto da URL (último path antes dos parâmetros)
            parsed = urlparse(url)
            path_parts = parsed.path.split('/')
            assunto_nome = path_parts[-1].lower() if path_parts else 'dados'
            
            # Extrair metadados da URL
            params = {}
            if parsed.query:
                params = dict(p.split('=') for p in parsed.query.split('&') if '=' in p)
            
            response = session.get(url, timeout=60)
            response.raise_for_status()
            
            if not response.content.strip():
                print("🟡 Resposta vazia. Ignorando.")
                continue
            
            dados = response.json()
            
            if not dados:
                print("🟡 Nenhum dado retornado.")
                continue
            
            df = pd.DataFrame(dados)
            
            if df.empty:
                print("🟡 DataFrame vazio.")
                continue
            
            # Adicionar metadados básicos (precisa ser adaptado conforme sua estrutura)
            if 'unidadeGestoraId' in params:
                df['unidade_gestora'] = params['unidadeGestoraId']
            if 'exercicio' in params:
                df['ano'] = int(params['exercicio'])
            if 'periodo' in params:
                df['mes'] = int(params['periodo'])
            
            # Criar/atualizar tabela
            create_table_if_not_exists(conn, assunto_nome, df)
            
            # Salvar dados
            df.to_sql(assunto_nome, conn, if_exists='append', index=False)
            success_count += 1
            print("✅ Sucesso")
        
        except Exception as e:
            print(f"🔴 Falhou novamente: {str(e)}")
            log_error(temp_error_file, url, e)
        
        sleep(1)
    
    conn.close()
    
    # Atualizar arquivo de erros
    if os.path.exists(temp_error_file):
        os.replace(temp_error_file, error_log_file)
    else:
        open(error_log_file, 'w').close()  # Limpa arquivo se todos foram reprocessados
    
    print(f"\n✅ Concluído! {success_count}/{len(failed_urls)} URLs reprocessadas com sucesso.")

# Funções para gerenciar última execução
def save_last_run(last_run_file, data_fim):
    with open(last_run_file, 'w') as f:
        f.write(f"{data_fim[0]},{data_fim[1]}")

def get_last_run(last_run_file):
    if not os.path.exists(last_run_file):
        return None
    
    try:
        with open(last_run_file, 'r') as f:
            content = f.read().strip()
            if not content:
                return None
            ano, mes = map(int, content.split(','))
            return (ano, mes)
    except Exception:
        return None

# Funções de log
def log_execution(log_file, message):
    timestamp = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(f"[{timestamp}] {message}\n")

def log_execution_time(log_file, start_time):
    elapsed = time() - start_time
    minutes, seconds = divmod(elapsed, 60)
    timestamp = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(f"[{timestamp}] Tempo de execução: {int(minutes)} minutos e {int(seconds)} segundos\n\n")

# Função principal
def main():
    paths = setup_directories()
    
    # Verificar se arquivos necessários existem
    if not os.path.exists(paths['prefeituras_file']):
        print(f"🔴 Arquivo de prefeituras não encontrado: {paths['prefeituras_file']}")
        return
    
    if not os.path.exists(paths['assuntos_file']):
        print(f"🔴 Arquivo de assuntos não encontrado: {paths['assuntos_file']}")
        return
    
    while True:
        print("\n" + "="*50)
        print("MENU PRINCIPAL - TECTRILHA DATA EXTRACTOR")
        print("="*50)
        print("1. Rodar código para um período específico")
        print("2. Rodar URLs que falharam (do arquivo de log)")
        print("3. Continuar extração desde a última data")
        print("4. Sair")

        choice = input("\nEscolha uma opção (1-4): ")

        if choice == '1':
            start_time = time()
            log_execution(paths['execution_log_file'], "Opção 1: Rodar código para período específico")
            
            data_inicio, data_fim = get_periodo_usuario()
            if not data_inicio or not data_fim:
                continue
            
            urls = build_tectrilha_urls(
                data_inicio[0], data_inicio[1], 
                data_fim[0], data_fim[1],
                paths['prefeituras_file'],
                paths['assuntos_file']
            )
            
            if urls:
                run_extraction(urls, paths['db_file'], paths['error_log_file'])
                save_last_run(paths['last_run_file'], data_fim)
            
            log_execution_time(paths['execution_log_file'], start_time)

        elif choice == '2':
            start_time = time()
            log_execution(paths['execution_log_file'], "Opção 2: Rodar URLs que falharam")
            run_failed_urls(paths['error_log_file'], paths['db_file'])
            log_execution_time(paths['execution_log_file'], start_time)

        elif choice == '3':
            start_time = time()
            log_execution(paths['execution_log_file'], "Opção 3: Continuar desde última data")
            
            data_inicio = get_last_run(paths['last_run_file'])
            if data_inicio is None:
                print("\n🔴 Nenhuma execução anterior encontrada. Use a opção 1 primeiro.")
                continue

            data_fim = (datetime.now().year, datetime.now().month)
            
            urls = build_tectrilha_urls(
                data_inicio[0], data_inicio[1], 
                data_fim[0], data_fim[1],
                paths['prefeituras_file'],
                paths['assuntos_file']
            )
            
            if urls:
                run_extraction(urls, paths['db_file'], paths['error_log_file'])
                save_last_run(paths['last_run_file'], data_fim)
            
            log_execution_time(paths['execution_log_file'], start_time)

        elif choice == '4':
            print("\nSaindo...")
            break

        else:
            print("\n🔴 Opção inválida. Tente novamente.")

if __name__ == "__main__":
    main()