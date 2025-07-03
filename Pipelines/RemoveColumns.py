import sqlite3
import argparse
import os
from pathlib import Path
from typing import Dict, List, Tuple

def get_db_path(db_name: str) -> Path:
    """Retorna o caminho absoluto para o banco de dados, respeitando a estrutura de pastas"""
    project_root = Path(__file__).parent.parent  # Sobe dois níveis (pipelines → projeto)
    db_path = project_root / "bds" / db_name
    
    if not db_path.exists():
        raise FileNotFoundError(f"Banco de dados não encontrado em: {db_path}")
    
    if not db_path.is_file():
        raise ValueError(f"Caminho não é um arquivo: {db_path}")
    
    return db_path

def get_table_schema(conn: sqlite3.Connection, table: str) -> Dict:
    """Obtém o schema completo de uma tabela"""
    cursor = conn.cursor()
    
    # Obtém informações das colunas
    cursor.execute(f"PRAGMA table_info({table})")
    columns = cursor.fetchall()
    
    # Obtém a SQL original de criação da tabela
    cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,))
    create_sql = cursor.fetchone()[0]
    
    # Obtém índices
    cursor.execute(f"SELECT name, sql FROM sqlite_master WHERE type='index' AND tbl_name=?", (table,))
    indexes = cursor.fetchall()
    
    return {
        'columns': columns,
        'create_sql': create_sql,
        'indexes': indexes
    }

def drop_columns(db_path: Path, table: str, columns_to_drop: List[str]) -> bool:
    """
    Remove colunas de uma tabela SQLite de forma segura
    
    Args:
        db_path: Caminho para o arquivo SQLite
        table: Nome da tabela
        columns_to_drop: Lista de nomes de colunas para remover
    
    Returns:
        bool: True se a operação foi bem sucedida
    """
    conn = None
    try:
        conn = sqlite3.connect(str(db_path))
        conn.execute("PRAGMA foreign_keys=OFF")  # Desativa temporariamente chaves estrangeiras
        cursor = conn.cursor()
        
        # 1. Verifica se a tabela existe
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
        if not cursor.fetchone():
            raise ValueError(f"Tabela '{table}' não existe no banco de dados")
        
        # 2. Obtém o schema completo da tabela
        schema = get_table_schema(conn, table)
        existing_columns = [col[1] for col in schema['columns']]
        
        # 3. Validações
        if len(existing_columns) - len(columns_to_drop) < 1:
            raise ValueError("Não é possível remover todas as colunas. A tabela deve ter pelo menos uma coluna restante.")
            
        for col in columns_to_drop:
            if col not in existing_columns:
                raise ValueError(f"Coluna '{col}' não existe na tabela '{table}'")
        
        # 4. Prepara o novo schema sem as colunas a serem removidas
        # Processa a SQL de criação para remover as colunas
        lines = schema['create_sql'].split('\n')
        new_lines = []
        
        for line in lines:
            # Verifica se a linha contém uma definição de coluna a ser removida
            drop_line = False
            for col in columns_to_drop:
                if f'"{col}"' in line or f' {col} ' in line:
                    drop_line = True
                    break
            
            if not drop_line:
                new_lines.append(line)
        
        new_create_sql = '\n'.join(new_lines)
        
        # 5. Cria nova tabela temporária
        temp_table = f"{table}_temp_{os.getpid()}"  # Nome único usando PID
        cursor.execute(new_create_sql.replace(table, temp_table))
        
        # 6. Copia os dados (excluindo as colunas a serem removidas)
        columns_select = []
        for col in schema['columns']:
            col_name = col[1]
            if col_name not in columns_to_drop:
                columns_select.append(f'"{col_name}"')
        
        insert_sql = f"""
        INSERT INTO {temp_table}
        SELECT {', '.join(columns_select)}
        FROM {table}
        """
        cursor.execute(insert_sql)
        
        # 7. Remove a tabela original e renomeia a temporária
        cursor.execute(f"DROP TABLE {table}")
        cursor.execute(f"ALTER TABLE {temp_table} RENAME TO {table}")
        
        # 8. Recria os índices (apenas os que não referenciam colunas removidas)
        for index_name, index_sql in schema['indexes']:
            # Verifica se o índice referencia alguma coluna removida
            index_valid = True
            for col in columns_to_drop:
                if f'"{col}"' in index_sql or f' {col} ' in index_sql:
                    index_valid = False
                    break
            
            if index_valid:
                cursor.execute(index_sql)
        
        conn.commit()
        conn.close()
        
        print(f"Tabela '{table}' atualizada com sucesso!")
        print("Colunas removidas:")
        for col in columns_to_drop:
            print(f"  {col}")
        
        return True
        
    except Exception as e:
        print(f"Erro durante a remoção das colunas: {str(e)}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()

def main():
    # Configura o parser de argumentos
    parser = argparse.ArgumentParser(
        description='Remove colunas de uma tabela em bancos de dados SQLite',
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument('db_name', help='Nome do arquivo do banco de dados (deve estar na pasta bancos_de_dados)')
    parser.add_argument('table', help='Nome da tabela')
    parser.add_argument('columns', nargs='+', 
        help='Nomes das colunas a serem removidas\nExemplo: email telefone endereco')
    
    args = parser.parse_args()
    
    try:
        # Obtém o caminho absoluto do banco de dados
        db_path = get_db_path(args.db_name)
        
        print(f"Banco de dados: {db_path}")
        print(f"Tabela: {args.table}")
        print("Colunas a serem removidas:")
        for col in args.columns:
            print(f"  {col}")
        
        # Confirmação do usuário
        confirm = input("\nConfirmar a operação? (s/n): ").strip().lower()
        if confirm != 's':
            print("Operação cancelada pelo usuário")
            return
        
        # Executa a remoção das colunas
        success = drop_columns(db_path, args.table, args.columns)
        
        if not success:
            print("Falha ao remover as colunas.")
            return
            
    except Exception as e:
        print(f"Erro: {str(e)}")

if __name__ == "__main__":
    main()


# Modo de uso: python RemoveColumns.py xxxx.db tabela coluna1 coluna2 ...