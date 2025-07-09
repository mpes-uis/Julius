import sqlite3
import os
from sqlite3 import Error

def create_connection(db_path):
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        return conn
    except Error as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
    return conn

def check_columns_exist(cursor, table_name, columns_to_check):
    cursor.execute(f"PRAGMA table_info({table_name})")
    existing_columns = [column[1] for column in cursor.fetchall()]
    
    missing_columns = [col for col in columns_to_check if col not in existing_columns]
    if missing_columns:
        print(f"Erro: As seguintes colunas não existem na tabela '{table_name}': {', '.join(missing_columns)}")
        print(f"Colunas existentes: {', '.join(existing_columns)}")
        return False
    return True

def combine_processo_ano(conn, table_name, processo_col, ano_col, output_col='processo_formatado', output_table=None):
    try:
        cursor = conn.cursor()
        
        # Verifica se a tabela e colunas existem
        if not check_columns_exist(cursor, table_name, [processo_col, ano_col]):
            return
        
        # Se output_table não for fornecido, atualiza a tabela existente
        if output_table is None:
            # Adiciona a nova coluna se ela não existir
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = [column[1] for column in cursor.fetchall()]
            
            if output_col not in columns:
                cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {output_col} TEXT")
            
            # Atualiza os valores (removendo o .0 dos números)
            update_sql = f"""
            UPDATE {table_name}
            SET {output_col} = 
                CASE 
                    WHEN {processo_col} IS NOT NULL AND {ano_col} IS NOT NULL 
                    THEN CAST(REPLACE({processo_col}, '.0', '') AS TEXT) || '/' || CAST(REPLACE({ano_col}, '.0', '') AS TEXT)
                    ELSE NULL
                END
            """
            cursor.execute(update_sql)
            print(f"Coluna '{output_col}' atualizada na tabela '{table_name}'")
        
        # Se output_table for fornecido, cria uma nova tabela
        else:
            # Cria a nova tabela
            cursor.execute(f"CREATE TABLE {output_table} AS SELECT * FROM {table_name} WHERE 1=0")
            
            # Adiciona a nova coluna
            cursor.execute(f"ALTER TABLE {output_table} ADD COLUMN {output_col} TEXT")
            
            # Copia os dados e preenche a nova coluna
            insert_sql = f"""
            INSERT INTO {output_table}
            SELECT *, 
                CASE 
                    WHEN {processo_col} IS NOT NULL AND {ano_col} IS NOT NULL 
                    THEN CAST(REPLACE({processo_col}, '.0', '') AS TEXT) || '/' || CAST(REPLACE({ano_col}, '.0', '') AS TEXT)
                    ELSE NULL
                END AS {output_col}
            FROM {table_name}
            """
            cursor.execute(insert_sql)
            print(f"Nova tabela '{output_table}' criada com a coluna '{output_col}'")
        
        conn.commit()
        
    except Error as e:
        print(f"Erro ao processar a tabela: {e}")
        conn.rollback()

def main():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    db_folder = "bds"
    database = "tectrilha.db"
    db_path = os.path.join(base_dir, "..", db_folder, database)
    
    table_name = "contratos" 
    processo_col = "Processo" 
    ano_col = "AnoProcesso" 
    output_col = "processo_formatado"
    
    # Opção 1: Atualizar a tabela existente (deixe output_table=None)
    # Opção 2: Criar nova tabela (especifique o nome da nova tabela)
    output_table = None  # Ou "nova_tabela_processos"
    
    print(f"Tentando acessar o banco de dados em: {db_path}")
    
    # Conecta ao banco de dados
    conn = create_connection(db_path)
    
    if conn is not None:
        combine_processo_ano(conn, table_name, processo_col, ano_col, output_col, output_table)
        conn.close()
    else:
        print("Erro ao conectar ao banco de dados")

if __name__ == '__main__':
    main()