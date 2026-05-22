import sqlite3
import argparse
import os
from pathlib import Path
from typing import Dict, List, Tuple

def get_db_path(db_name: str) -> Path:
    """Retorna o caminho absoluto para o banco de dados"""
    project_root = Path(__file__).parent.parent
    db_path = project_root / "bds" / db_name
    
    if not db_path.exists():
        raise FileNotFoundError(f"Banco de dados não encontrado em: {db_path}")
    
    if not db_path.is_file():
        raise ValueError(f"Caminho não é um arquivo: {db_path}")
    
    return db_path

def get_table_schema(conn: sqlite3.Connection, table: str) -> Dict:
    """Obtém o schema completo de uma tabela"""
    cursor = conn.cursor()
    
    cursor.execute(f"PRAGMA table_info({table})")
    columns = cursor.fetchall()
    
    cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,))
    create_sql = cursor.fetchone()[0]
    
    cursor.execute(f"SELECT name, sql FROM sqlite_master WHERE type='index' AND tbl_name=?", (table,))
    indexes = cursor.fetchall()
    
    return {
        'columns': columns,
        'create_sql': create_sql,
        'indexes': indexes
    }

def change_column_types(db_path: Path, table: str, type_changes: Dict[str, str]) -> bool:
    """
    Altera os tipos de colunas em uma tabela SQLite
    
    Args:
        db_path: Caminho para o arquivo SQLite
        table: Nome da tabela
        type_changes: Dicionário {nome_da_coluna: novo_tipo}
    
    Returns:
        bool: True se a operação foi bem sucedida
    """
    conn = None
    try:
        conn = sqlite3.connect(str(db_path))
        conn.execute("PRAGMA foreign_keys=OFF")
        cursor = conn.cursor()
        
        # 1. Verifica se a tabela existe
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
        if not cursor.fetchone():
            raise ValueError(f"Tabela '{table}' não existe no banco de dados")
        
        # 2. Obtém o schema completo da tabela
        schema = get_table_schema(conn, table)
        existing_columns = {col[1]: col[2] for col in schema['columns']}  # nome: tipo
        
        # 3. Validações
        for col_name in type_changes:
            if col_name not in existing_columns:
                raise ValueError(f"Coluna '{col_name}' não existe na tabela '{table}'")
        
        # 4. Prepara o novo schema com os tipos alterados
        new_create_sql = schema['create_sql']
        for col_name, new_type in type_changes.items():
            # Substitui a definição do tipo da coluna
            old_type = existing_columns[col_name]
            new_create_sql = new_create_sql.replace(
                f'"{col_name}" {old_type}',
                f'"{col_name}" {new_type}'
            )
            new_create_sql = new_create_sql.replace(
                f' {col_name} {old_type}',
                f' {col_name} {new_type}'
            )
        
        # 5. Cria nova tabela temporária
        temp_table = f"{table}_temp_{os.getpid()}"
        cursor.execute(new_create_sql.replace(table, temp_table))
        
        # 6. Copia os dados
        columns_select = [f'"{col[1]}"' for col in schema['columns']]
        insert_sql = f"""
        INSERT INTO {temp_table}
        SELECT {', '.join(columns_select)}
        FROM {table}
        """
        cursor.execute(insert_sql)
        
        # 7. Remove a tabela original e renomeia a temporária
        cursor.execute(f"DROP TABLE {table}")
        cursor.execute(f"ALTER TABLE {temp_table} RENAME TO {table}")
        
        # 8. Recria os índices
        for index_name, index_sql in schema['indexes']:
            cursor.execute(index_sql)
        
        conn.commit()
        
        print(f"Tabela '{table}' atualizada com sucesso!")
        print("Tipos alterados:")
        for col, new_type in type_changes.items():
            print(f"  {col}: {existing_columns[col]} → {new_type}")
        
        return True
        
    except Exception as e:
        print(f"Erro durante a alteração de tipos: {str(e)}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()

def main():
    parser = argparse.ArgumentParser(
        description='Altera tipos de colunas em tabelas SQLite',
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument('db_name', help='Nome do arquivo do banco de dados')
    parser.add_argument('table', help='Nome da tabela')
    parser.add_argument('changes', nargs='+', 
        help='Alterações no formato coluna:novo_tipo\nExemplo: idade:INTEGER preco:REAL')
    
    args = parser.parse_args()
    
    try:
        # Processa os mapeamentos de alteração
        type_changes = {}
        for change in args.changes:
            if ':' not in change:
                raise ValueError(f"Formato inválido: '{change}'. Use coluna:novo_tipo")
            col, new_type = change.split(':', 1)
            type_changes[col.strip()] = new_type.strip()
        
        db_path = get_db_path(args.db_name)
        
        print(f"Banco de dados: {db_path}")
        print(f"Tabela: {args.table}")
        print("Alterações a serem aplicadas:")
        for col, new_type in type_changes.items():
            print(f"  {col}: → {new_type}")
        
        confirm = input("\nConfirmar a operação? (s/n): ").strip().lower()
        if confirm != 's':
            print("Operação cancelada pelo usuário")
            return
        
        success = change_column_types(db_path, args.table, type_changes)
        
        if not success:
            print("Falha ao alterar os tipos das colunas.")
            
    except Exception as e:
        print(f"Erro: {str(e)}")

if __name__ == "__main__":
    main()