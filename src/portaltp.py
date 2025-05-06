import requests
import pandas as pd
import sqlite3
from time import sleep, time
import os
from datetime import datetime
import calendar

def main():
    # Configurações de diretórios
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(base_dir, 'data')
    bds_dir = os.path.join(base_dir, 'bds')
    logs_dir = os.path.join(base_dir, 'logs')
    
    endpoints_file = os.path.join(data_dir, 'endpoints_portaltp.txt')
    prefeituras_file = os.path.join(data_dir, 'prefeituras.csv')
    db_file = os.path.join(bds_dir, 'portaltp.db')
    error_log_file = os.path.join(logs_dir, 'portaltp_errors.log')
    execution_log_file = os.path.join(logs_dir, 'portaltp_execution.log')
    last_run_file = os.path.join(logs_dir, 'portaltp_last_run.txt')

    # Criar diretórios se não existirem
    os.makedirs(bds_dir, exist_ok=True)
    os.makedirs(logs_dir, exist_ok=True)

    # Menu principal
    while True:
        print("\n" + "="*50)
        print("MENU PRINCIPAL - PORTALTP DATA EXTRACTOR")
        print("="*50)
        print("1. Rodar código para um período específico")
        print("2. Rodar URLs que falharam (do arquivo de log)")
        print("3. Continuar extração desde a última data")
        print("4. Sair")
        
        choice = input("\nEscolha uma opção (1-4): ")
        
        if choice == '1':
            start_time = time()
            log_execution(execution_log_file, "Opção 1: Rodar código para período específico")
            data_inicio, data_fim = get_periodo_usuario()
            run_extraction(data_inicio, data_fim, endpoints_file, prefeituras_file, db_file, error_log_file)
            save_last_run(last_run_file, data_fim)
            log_execution_time(execution_log_file, start_time)
        
        elif choice == '2':
            start_time = time()
            log_execution(execution_log_file, "Opção 2: Rodar URLs que falharam")
            run_failed_urls(error_log_file, endpoints_file, prefeituras_file, db_file)
            log_execution_time(execution_log_file, start_time)
        
        elif choice == '3':
            start_time = time()
            log_execution(execution_log_file, "Opção 3: Continuar desde última data")
            data_inicio = get_last_run(last_run_file)
            if data_inicio is None:
                print("\n🔴 Nenhuma execução anterior encontrada. Use a opção 1 primeiro.")
                continue
            
            data_fim = (datetime.now().year, datetime.now().month)
            run_extraction(data_inicio, data_fim, endpoints_file, prefeituras_file, db_file, error_log_file)
            save_last_run(last_run_file, data_fim)
            log_execution_time(execution_log_file, start_time)
        
        elif choice == '4':
            print("\nSaindo...")
            break
        
        else:
            print("\n🔴 Opção inválida. Tente novamente.")

def get_periodo_usuario():
    """Obtém o período desejado do usuário"""
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
            
            # Validar datas
            if (ano_inicio > ano_fim) or (ano_inicio == ano_fim and mes_inicio > mes_fim):
                print("🔴 Data inicial deve ser anterior à data final. Tente novamente.")
                continue
                
            if mes_inicio < 1 or mes_inicio > 12 or mes_fim < 1 or mes_fim > 12:
                print("🔴 Mês inválido. Deve ser entre 1 e 12. Tente novamente.")
                continue
                
            return (ano_inicio, mes_inicio), (ano_fim, mes_fim)
            
        except (ValueError, IndexError):
            print("🔴 Formato inválido. Use MM/AAAA (ex: 01/2024). Tente novamente.")

def run_extraction(data_inicio, data_fim, endpoints_file, prefeituras_file, db_file, error_log_file):
    """
    Executa a extração de dados para um período específico,
    evitando duplicatas e garantindo commits no banco de dados.
    """
    # Carrega endpoints e prefeituras
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
        
        # Cria a tabela se não existir 
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {endpoint_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                municipio TEXT,
                prefeitura TEXT,
                ano INTEGER,
                mes INTEGER
            )
        ''')
        conn.commit()

        for _, prefeitura in prefeituras_portaltp.iterrows():
            municipio = prefeitura['municipio']
            prefeitura_nome = prefeitura['prefeitura']
            base_url = prefeitura['url']
            
            print(f"\n🏛️ Prefeitura: {prefeitura_nome} ({municipio})")
            
            meses_periodo = generate_months_range(data_inicio, data_fim)
            
            for ano, mes in meses_periodo:
                print(f"📅 {mes:02d}/{ano}", end=' ', flush=True)
                
                # Verifica se os dados já existem (usando ano/mes como filtro)
                cursor.execute(f'''
                    SELECT 1 FROM {endpoint_name} 
                    WHERE municipio = ? AND prefeitura = ? AND ano = ? AND mes = ?
                    LIMIT 1
                ''', (municipio, prefeitura_nome, ano, mes))
                
                if cursor.fetchone():
                    print("✅ Já existe no BD", end=' ')
                    continue
                
                # Se não existir, faz a requisição
                url = f"{base_url}/{endpoint}?ano={ano}&mes={mes:02d}"
                
                try:
                    response = requests.get(url, timeout=10)
                    response.raise_for_status()
                    dados = response.json()
                    
                    df = pd.DataFrame(dados)
                    
                    if not df.empty:
                        # Adiciona metadados
                        df['municipio'] = municipio
                        df['prefeitura'] = prefeitura_nome
                        df['ano'] = ano
                        df['mes'] = mes
                        
                        # Adiciona colunas dinâmicas 
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
                        
                        # Insere os dados (commit automático no to_sql)
                        df.to_sql(endpoint_name, conn, if_exists='append', index=False)
                        print("✅ Dados salvos", end=' ')
                        
                except Exception as e:
                    print(f"🔴 ERRO: {str(e)}", end=' ')
                    with open(error_log_file, 'a') as f:
                        f.write(f"{url}|{str(e)}\n")
                
                sleep(1)  # Delay entre requisições
    
    conn.close()
    print("\n\n✅ EXTRAÇÃO CONCLUÍDA!")

def run_failed_urls(error_log_file, endpoints_file, prefeituras_file, db_file):
    """Tenta executar novamente as URLs que falharam"""
    if not os.path.exists(error_log_file):
        print("\n🔴 Nenhum arquivo de log de erros encontrado.")
        return

    with open(error_log_file, 'r') as f:
        failed_urls = [line.strip().split('|') for line in f if line.strip()]

    if not failed_urls:
        print("\n✅ Nenhuma URL com erro para reprocessar.")
        return

    print(f"\n🔧 Reprocessando {len(failed_urls)} URLs com erro")
    
    # Criar um novo arquivo de log para os erros que persistirem
    temp_error_file = error_log_file + ".temp"
    success_count = 0

    conn = sqlite3.connect(db_file)
    
    for url, error in failed_urls:
        try:
            print(f"🔁 Tentando novamente: {url}", end=' ', flush=True)
            
            # Extrair informações da URL
            parts = url.split('/')
            base_url = '/'.join(parts[:-2])
            endpoint = parts[-2]
            params = parts[-1].split('?')[1]
            
            # Encontrar a prefeitura correspondente
            prefeituras = load_prefeituras(prefeituras_file)
            prefeitura = prefeituras[prefeituras['url'] == base_url].iloc[0]
            
            endpoint_name = endpoint.split('/')[-1].replace('Get', '').lower()
            
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            dados = response.json()
            
            df = pd.DataFrame(dados)
            
            if not df.empty:
                if 'municipio' not in df.columns:
                    df['municipio'] = prefeitura['municipio']
                
                if 'prefeitura' not in df.columns:
                    df['prefeitura'] = prefeitura['prefeitura']
                
                cursor = conn.cursor()
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
                success_count += 1
                print("✅ Sucesso")
            
        except Exception as e:
            print(f"🔴 Falhou novamente: {str(e)}")
            with open(temp_error_file, 'a') as f:
                f.write(f"{url}|{str(e)}\n")
        
        sleep(1)  # Delay entre requisições
    
    conn.close()
    
    # Substituir o arquivo de log original pelo temporário
    os.replace(temp_error_file, error_log_file)
    
    print(f"\n✅ Concluído! {success_count}/{len(failed_urls)} URLs reprocessadas com sucesso.")

def generate_months_range(data_inicio, data_fim):
    """Gera uma lista de tuplas (ano, mês) para o período especificado"""
    meses = []
    ano_inicio, mes_inicio = data_inicio
    ano_fim, mes_fim = data_fim
    
    ano_atual = ano_inicio
    mes_atual = mes_inicio
    
    while (ano_atual < ano_fim) or (ano_atual == ano_fim and mes_atual <= mes_fim):
        meses.append((ano_atual, mes_atual))
        
        mes_atual += 1
        if mes_atual > 12:
            mes_atual = 1
            ano_atual += 1
    
    return meses

def save_last_run(last_run_file, data_fim):
    """Salva a última data de execução bem-sucedida"""
    with open(last_run_file, 'w') as f:
        f.write(f"{data_fim[0]},{data_fim[1]}")

def get_last_run(last_run_file):
    """Obtém a última data de execução bem-sucedida"""
    if not os.path.exists(last_run_file):
        return None
    
    with open(last_run_file, 'r') as f:
        content = f.read().strip()
        ano, mes = map(int, content.split(','))
        return (ano, mes)

def log_execution(log_file, message):
    """Registra a execução no arquivo de log"""
    timestamp = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    with open(log_file, 'a') as f:
        f.write(f"[{timestamp}] {message}\n")

def log_execution_time(log_file, start_time):
    """Registra o tempo de execução no arquivo de log"""
    elapsed_time = time() - start_time
    minutes, seconds = divmod(elapsed_time, 60)
    timestamp = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    with open(log_file, 'a') as f:
        f.write(f"[{timestamp}] Tempo de execução: {int(minutes)} minutos e {int(seconds)} segundos\n\n")

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