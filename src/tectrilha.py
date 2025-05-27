import requests
import pandas as pd
import sqlite3
import time
import json
from datetime import datetime
import sys
import os
from urllib.parse import urlparse
from requests.adapters import HTTPAdapter, Retry
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

class PrintManager:
    _lock = threading.Lock()
    
    @classmethod
    def print(cls, message, end="\n"):
        with cls._lock:
            print(message, end=end, flush=True)

# Configurações
REQUEST_TIMEOUT = 60
DELAY_BETWEEN_REQUESTS = 1
MAX_RETRIES = 5
RETRY_DELAY = 5
MAX_WORKERS = 5
DB_FILE = None  # Será definido no main()
ERROR_LOG_FILE = None  # Será definido no main()

# Armazenamento thread-local
thread_local = threading.local()

def get_db_connection():
    """Cria uma conexão SQLite por thread"""
    if not hasattr(thread_local, "conn"):
        thread_local.conn = sqlite3.connect(DB_FILE)
    return thread_local.conn

def close_db_connections():
    """Fecha todas as conexões SQLite"""
    if hasattr(thread_local, "conn"):
        thread_local.conn.close()
        del thread_local.conn

def main():
    global DB_FILE, ERROR_LOG_FILE
    
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    prefeituras_file = os.path.join(base_dir, "data", "prefeituras.csv")
    assuntos_file = os.path.join(base_dir, "data", "assuntos_tectrilha.csv")
    DB_FILE = os.path.join(base_dir, "bds", "tectrilha.db")
    ERROR_LOG_FILE = os.path.join(base_dir, "logs", "tectrilha_errors.log")

    if not all(os.path.exists(f) for f in [prefeituras_file, assuntos_file]):
        print("\n🔴 ERRO: Arquivos necessários não encontrados.")
        print(f"🔍 Procurando em: {prefeituras_file}")
        print(f"🔍 Procurando em: {assuntos_file}")
        return

    os.makedirs(os.path.dirname(DB_FILE), exist_ok=True)
    os.makedirs(os.path.dirname(ERROR_LOG_FILE), exist_ok=True)

    while True:
        print("\n" + "="*50)
        print("🚀 MENU PRINCIPAL - TECTRILHA DATA EXTRACTOR")
        print("="*50)
        print("1. Extrair dados por período")
        print("2. Reprocessar URLs com erro")
        print("3. Sair")
        
        opcao = input("\nEscolha uma opção (1-3): ")
        
        if opcao == "1":
            ano_inicio, ano_fim = get_periodo_usuario()
            run_extraction(ano_inicio, ano_fim, assuntos_file, prefeituras_file)
        elif opcao == "2":
            run_failed_urls(assuntos_file, prefeituras_file)
        elif opcao == "3":
            close_db_connections()
            print("\n👋 Encerrando o programa...")
            break
        else:
            print("🔴 Opção inválida. Tente novamente.")

def run_extraction(ano_inicio, ano_fim, assuntos_file, prefeituras_file):
    try:
        # Carrega os arquivos de configuração
        prefeituras = pd.read_csv(prefeituras_file)
        assuntos = pd.read_csv(assuntos_file)
        tectrilha_prefs = prefeituras[prefeituras['empresa'] == 'Tectrilha']

        if tectrilha_prefs.empty:
            PrintManager.print("\n🔴 Nenhuma prefeitura com empresa 'Tectrilha' encontrada.")
            return

        urls_to_process = []
        
        # Prepara todas as URLs para processamento
        for _, prefeitura in tectrilha_prefs.iterrows():
            municipio = prefeitura['municipio']
            prefeitura_nome = prefeitura['prefeitura']
            PrintManager.print(f"\n🏛️ Preparando URLs para: {prefeitura_nome} ({municipio})")

            for _, assunto in assuntos.iterrows():
                assunto_nome = assunto['assunto']
                years = [0] if assunto_nome == "bensImoveis" else range(ano_inicio, ano_fim + 1)

                for ano in years:
                    if ano == datetime.now().year:
                        continue

                    url = build_url(
                        prefeitura['url'],
                        assunto_nome,
                        assunto['parametros'],
                        prefeitura['unidadegestora'],
                        ano
                    )
                    
                    if is_valid_url(url):
                        urls_to_process.append((url, prefeitura_nome, municipio, ano, assunto_nome))
                    else:
                        PrintManager.print(f"   🟡 URL inválida ignorada: {url[:50]}...")

        PrintManager.print(f"\n🔍 Total de URLs válidas para processar: {len(urls_to_process)}")
        
        if not urls_to_process:
            PrintManager.print("\nℹ️ Nenhuma URL válida para processar.")
            return
            
        PrintManager.print(f"🚀 Iniciando processamento com {MAX_WORKERS} workers paralelos...\n")
        
        # Variáveis para controle de progresso
        processed_count = 0
        total_urls = len(urls_to_process)
        start_time = time.time()
        
        # Função de callback para atualizar o progresso
        def update_progress(future):
            nonlocal processed_count
            processed_count += 1
            elapsed = time.time() - start_time
            remaining = (elapsed / processed_count) * (total_urls - processed_count) if processed_count > 0 else 0
            
            PrintManager.print(
                f"\r   📊 Progresso: {processed_count}/{total_urls} ({processed_count/total_urls:.1%}) | "
                f"⏱️ {elapsed:.1f}s | "
                f"⏳ {remaining:.1f}s restante", 
                end=''
            )
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Dispara todas as tarefas de uma vez
            futures = [executor.submit(process_single_url_wrapper, url_data) for url_data in urls_to_process]
            
            # Configura callbacks para atualizar o progresso
            for future in futures:
                future.add_done_callback(update_progress)
            
            # Espera todas as tarefas completarem
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    pass  # Erros já tratados no wrapper

        # Estatísticas finais
        elapsed_total = time.time() - start_time
        rate = total_urls / elapsed_total if elapsed_total > 0 else 0
        
        PrintManager.print("\n\n✅ EXTRAÇÃO CONCLUÍDA!")
        PrintManager.print(f"   Total de URLs processadas: {total_urls}")
        PrintManager.print(f"   Tempo total: {elapsed_total:.2f} segundos")
        PrintManager.print(f"   Velocidade: {rate:.2f} URLs/segundo")
        PrintManager.print(f"   Consulte o log para detalhes: {ERROR_LOG_FILE}")
        
    except Exception as e:
        PrintManager.print(f"\n🔴 ERRO GLOBAL: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        close_db_connections()
        # Força a exibição do menu principal após conclusão
        return

def process_single_url_wrapper(url_data):
    """Wrapper para processar cada URL com tratamento de erros"""
    url, prefeitura_nome, municipio, ano, assunto_nome = url_data
    conn = get_db_connection()
    
    # Mostra informações resumidas
    PrintManager.print(f"\n🔍 [{threading.current_thread().name}] Processando: {prefeitura_nome[:15]}... | {assunto_nome[:10]}... | {ano}")
    PrintManager.print(f"   📌 URL: {url[:50]}...")
    
    try:
        process_single_url(url, prefeitura_nome, municipio, ano, assunto_nome, conn)
        PrintManager.print("   ✅ Concluído com sucesso")
    except Exception as e:
        error_msg = f"{type(e).__name__} - {str(e)}"
        log_error(ERROR_LOG_FILE, url, error_msg)
        PrintManager.print(f"   ❌ Erro: {error_msg[:50]}...")

def get_retry_session():
    """Cria uma sessão com retry para lidar com falhas temporárias"""
    session = requests.Session()
    retries = Retry(
        total=MAX_RETRIES,
        backoff_factor=2,
        status_forcelist=[500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def process_single_url(url, prefeitura_nome, municipio, ano, assunto_nome, conn):
    session = get_retry_session()
    
    try:
        success, data = make_request_with_retry(session, url)
        
        if not success:
            raise Exception(f"Falha ao processar URL após {MAX_RETRIES} tentativas")
        
        if data:
            df = transform_json_to_dataframe(data, assunto_nome)
            if df is not None and not df.empty:
                df['prefeitura'] = prefeitura_nome
                df['municipio'] = municipio
                df['ano'] = ano
                
                df = df.rename(columns={col: clean_column_name(col) for col in df.columns})
                store_dataframe(conn, df, assunto_nome, url)
        
        time.sleep(DELAY_BETWEEN_REQUESTS)
    
    except requests.exceptions.RequestException as e:
        raise Exception(f"Erro na requisição: {str(e)}")
    except Exception as e:
        raise

def is_valid_url(url):
    """Verifica se a URL parece ser válida"""
    parsed = urlparse(url)
    return all([parsed.scheme, parsed.netloc])

def run_failed_urls(assuntos_file, prefeituras_file):
    try:
        if not os.path.exists(ERROR_LOG_FILE):
            print("\n🔴 Nenhum arquivo de log de erros encontrado.")
            return

        failed_urls = []
        with open(ERROR_LOG_FILE, 'r') as f:
            for line in f:
                if '|' in line:
                    parts = line.strip().split('|')
                    if len(parts) >= 2:
                        url = parts[1].strip()
                        if url not in failed_urls and is_valid_url(url):
                            failed_urls.append(url)

        if not failed_urls:
            print("\n✅ Nenhuma URL com erro para reprocessar.")
            return

        print(f"\n🔧 Reprocessando {len(failed_urls)} URLs com erro")
        prefeituras = pd.read_csv(prefeituras_file)
        success_count = 0
        temp_error_file = ERROR_LOG_FILE + ".temp"

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = []
            for url in failed_urls:
                futures.append(executor.submit(reprocess_single_url, url, prefeituras))
                time.sleep(0.1)

            for future in as_completed(futures):
                try:
                    if future.result():
                        success_count += 1
                except Exception as e:
                    print(f"🔴 Erro ao reprocessar URL: {str(e)}")

        if os.path.exists(temp_error_file):
            os.replace(temp_error_file, ERROR_LOG_FILE)
        else:
            open(ERROR_LOG_FILE, 'w').close()
        
        print(f"\n✅ Concluído! {success_count}/{len(failed_urls)} URLs reprocessadas com sucesso.")
        return  # Retorna explicitamente para o menu principal
    except Exception as e:
        print(f"\n🔴 Erro durante o reprocessamento: {str(e)}")
    finally:
        # Remove o input() que estava mantendo o programa parado
        pass  # Não é mais necessário

def process_single_url_wrapper(url_data):
    """Wrapper para processar cada URL com tratamento de erros"""
    url, prefeitura_nome, municipio, ano, assunto_nome = url_data
    conn = get_db_connection()
    
    # Mostra informações resumidas para evitar poluição visual
    print(f"\n🔍 Processando: {prefeitura_nome} | {assunto_nome} | {ano}", end='', flush=True)
    
    try:
        process_single_url(url, prefeitura_nome, municipio, ano, assunto_nome, conn)
        print(" ✅")  # Mostra sucesso na mesma linha
    except Exception as e:
        error_msg = f"{type(e).__name__} - {str(e)}"
        log_error(ERROR_LOG_FILE, url, error_msg)
        print(" ❌")  # Mostra falha na mesma linha
        print(f"   Detalhes do erro: {error_msg}")

def log_error(error_log_file, url, error_msg):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_entry = f"{timestamp}|{url}|{error_msg}\n"
    
    with open(error_log_file, 'a') as f:
        f.write(log_entry)
    
    print(f"🔴 Erro registrado no log: {error_msg}")

def reprocess_single_url(url, prefeituras):
    """Reprocessa uma única URL que falhou anteriormente"""
    conn = get_db_connection()
    session = get_retry_session()
    temp_error_file = ERROR_LOG_FILE + ".temp"
    
    try:
        parsed = urlparse(url)
        path_parts = parsed.path.split('/')
        assunto_nome = path_parts[-2] if len(path_parts) >= 2 else 'unknown'
        
        # Encontra a prefeitura correspondente à URL
        prefeitura_match = prefeituras[
            prefeituras['url'].str.contains(parsed.netloc, case=False, na=False)
        ]
        
        if prefeitura_match.empty:
            print(f"🟡 Prefeitura não encontrada para URL: {url}")
            return False

        prefeitura = prefeitura_match.iloc[0]
        municipio = prefeitura['municipio']
        prefeitura_nome = prefeitura['prefeitura']

        print(f"\n🔁 Tentando novamente: {url}")
        response = session.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        
        if not response.content.strip():
            print(f"🟡 Resposta vazia para URL: {url}")
            return False

        data = response.json()
        df = transform_json_to_dataframe(data, assunto_nome)

        if df is not None and not df.empty:
            # Extrai o ano da URL se existir
            ano = 0
            if 'exercicio=' in parsed.query:
                try:
                    ano = int(parsed.query.split('exercicio=')[1].split('&')[0])
                except (IndexError, ValueError):
                    pass
            
            # Adiciona metadados
            df['prefeitura'] = prefeitura_nome
            df['municipio'] = municipio
            df['ano'] = ano
            
            # Limpa nomes de colunas
            df = df.rename(columns={col: clean_column_name(col) for col in df.columns})
            
            # Armazena os dados
            store_dataframe(conn, df, assunto_nome, url)
            return True
        
        return False

    except Exception as e:
        error_msg = f"{type(e).__name__} - {str(e)}"
        with open(temp_error_file, 'a') as f:
            f.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|{url}|{error_msg}\n")
        print(f"🔴 Falha ao reprocessar URL: {url} - {error_msg}")
        return False
    finally:
        time.sleep(DELAY_BETWEEN_REQUESTS)

def run_failed_urls(error_log_file, assuntos_file, prefeituras_file, db_file):
    if not os.path.exists(error_log_file):
        print("\n🔴 Nenhum arquivo de log de erros encontrado.")
        return

    # Carrega URLs com erro
    failed_urls = []
    with open(error_log_file, 'r') as f:
        for line in f:
            if '|' in line:
                parts = line.strip().split('|')
                if len(parts) >= 2:
                    url = parts[1].strip()
                    if url not in failed_urls:
                        failed_urls.append(url)

    if not failed_urls:
        print("\n✅ Nenhuma URL com erro para reprocessar.")
        return

    print(f"\n🔧 Reprocessando {len(failed_urls)} URLs com erro")
    session = get_retry_session()
    prefeituras = pd.read_csv(prefeituras_file)
    conn = sqlite3.connect(db_file)
    success_count = 0
    temp_error_file = error_log_file + ".temp"

    for url in failed_urls:
        try:
            print(f"\n🔁 Tentando novamente: {url}")
            
            parsed = urlparse(url)
            path_parts = parsed.path.split('/')
            assunto_nome = path_parts[-2] if len(path_parts) >= 2 else 'unknown'
            
            prefeitura_match = prefeituras[
                prefeituras['url'].str.contains(parsed.netloc, case=False, na=False)
            ]
            
            if prefeitura_match.empty:
                print("🟡 Prefeitura não encontrada. Ignorando.")
                continue

            prefeitura = prefeitura_match.iloc[0]
            municipio = prefeitura['municipio']
            prefeitura_nome = prefeitura['prefeitura']

            response = session.get(url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            
            if not response.content.strip():
                print("🟡 Resposta vazia. Ignorando.")
                continue

            data = response.json()
            df = pd.DataFrame(data)

            if not df.empty:
                ano = 0
                if 'exercicio=' in parsed.query:
                    try:
                        ano = int(parsed.query.split('exercicio=')[1].split('&')[0])
                    except (IndexError, ValueError):
                        pass
                
                df['prefeitura'] = prefeitura_nome
                df['municipio'] = municipio
                df['ano'] = ano
                
                # Limpeza de colunas com NaN no nome
                df = df.rename(columns={col: clean_column_name(col) for col in df.columns})
                
                store_dataframe(conn, df, assunto_nome, url)
                success_count += 1
                print("✅ Sucesso")

        except Exception as e:
            error_msg = f"{type(e).__name__} - {str(e)}"
            print(f"🔴 Falhou novamente: {error_msg}")
            with open(temp_error_file, 'a') as f:
                f.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|{url}|{error_msg}\n")
        
        time.sleep(DELAY_BETWEEN_REQUESTS)

    conn.close()
    
    if os.path.exists(temp_error_file):
        os.replace(temp_error_file, error_log_file)
    else:
        open(error_log_file, 'w').close()
    
    print(f"\n✅ Concluído! {success_count}/{len(failed_urls)} URLs reprocessadas com sucesso.")

def make_request_with_retry(session, url):
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    for attempt in range(MAX_RETRIES):
        try:
            response = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            
            # Verifica se o conteúdo é JSON válido
            try:
                data = response.json()
                return True, data
            except ValueError:
                if attempt == MAX_RETRIES - 1:
                    raise ValueError("Resposta não é um JSON válido")
                time.sleep(RETRY_DELAY * (attempt + 1))
                continue
                
        except requests.exceptions.RequestException as e:
            if attempt == MAX_RETRIES - 1:
                raise
            time.sleep(RETRY_DELAY * (attempt + 1))
            
        except Exception as e:
            if attempt == MAX_RETRIES - 1:
                raise
            time.sleep(RETRY_DELAY * (attempt + 1))

def get_periodo_usuario():
    print("\n" + "="*50)
    print("DEFINIR PERÍODO DE EXTRAÇÃO")
    print("="*50)

    while True:
        try:
            ano_inicio = int(input("Ano inicial (AAAA): "))
            ano_fim = int(input("Ano final (AAAA): "))

            if ano_inicio > ano_fim:
                print("🔴 Data inicial deve ser anterior à data final. Tente novamente.")
                continue

            return ano_inicio, ano_fim

        except ValueError:
            print("🔴 Formato inválido. Use AAAA (ex: 2024). Tente novamente.")

def build_url(base_url, assunto, parametros, unidadegestora, ano):
    if pd.isna(parametros) or not parametros.strip():
        return f"{base_url.rstrip('/')}/{assunto}"
    
    try:
        ug_id = int(float(unidadegestora)) if not pd.isna(unidadegestora) else 0
    except (ValueError, TypeError):
        ug_id = 0
    
    params = parametros
    params = params.replace("{unidadeGestoraId}", str(ug_id))
    params = params.replace("{exercicio}", str(ano))
    
    if "{periodo}" in params:
        params = params.replace("{periodo}", "1")
    
    params = params.replace("?&", "?")
    if params.endswith("?"):
        params = params[:-1]
    
    return f"{base_url.rstrip('/')}/{assunto}{params}"

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

def data_exists(conn, table_name, df):
    try:
        cursor = conn.cursor()
        
        # Verifica se a tabela existe
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
        if not cursor.fetchone():
            return False
            
        # Obtém as colunas da tabela
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = [column[1] for column in cursor.fetchall()]
        
        # Encontra colunas em comum que podem ser usadas para verificação
        common_columns = [col for col in df.columns if col in columns]
        if not common_columns:
            return False
            
        # Cria uma condição WHERE baseada nos valores únicos
        conditions = []
        for _, row in df.iterrows():
            condition_parts = []
            for col in common_columns:
                value = row[col]
                if pd.isna(value):
                    condition_parts.append(f"{col} IS NULL")
                else:
                    if isinstance(value, str):
                        value = value.replace("'", "''")
                    condition_parts.append(f"{col} = '{value}'")
            conditions.append(" AND ".join(condition_parts))
        
        # Verifica cada registro
        for condition in conditions:
            query = f"SELECT COUNT(*) FROM {table_name} WHERE {condition}"
            cursor.execute(query)
            if cursor.fetchone()[0] > 0:
                return True
                
        return False
        
    except Exception as e:
        print(f"🔴 Erro ao verificar dados existentes: {str(e)}")
        return False
    
def store_dataframe(conn, df, table_name, url):
    try:
        # [código existente...]
        
        if data_exists(conn, table_name, df):
            PrintManager.print(f"   🟡 Dados já existem na tabela '{table_name}'. Ignorando.")
            return
        
        # [restante do código existente...]
        
    except Exception as e:
        PrintManager.print(f"   🔴 Erro ao armazenar: {str(e)[:50]}...")
        raise

def clean_column_name(col):
    # Remove caracteres especiais, substitui espaços e trata NaN
    if pd.isna(col):
        return "unknown_column"
    return ''.join(c if c.isalnum() else '_' for c in str(col)).strip('_')

if __name__ == "__main__":
    main()