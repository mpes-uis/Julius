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

def rename_columns(db_path: Path, table: str, rename_map: Dict[str, str]) -> bool:
    """
    Renomeia múltiplas colunas em uma tabela SQLite de forma eficiente
    
    Args:
        db_path: Caminho para o arquivo SQLite
        table: Nome da tabela
        rename_map: Dicionário {nome_antigo: nome_novo}
    
    Returns:
        bool: True se a operação foi bem sucedida
    """
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
        for old_name in rename_map:
            if old_name not in existing_columns:
                raise ValueError(f"Coluna '{old_name}' não existe na tabela '{table}'")
            
        new_names = list(rename_map.values())
        for new_name in new_names:
            if new_name in existing_columns and new_name not in rename_map.keys():
                raise ValueError(f"Coluna '{new_name}' já existe na tabela '{table}'")
        
        # 4. Prepara o novo schema
        new_create_sql = schema['create_sql']
        for old_name, new_name in rename_map.items():
            new_create_sql = new_create_sql.replace(f'"{old_name}"', f'"{new_name}"')
            new_create_sql = new_create_sql.replace(f' {old_name} ', f' {new_name} ')
        
        # 5. Cria nova tabela temporária
        temp_table = f"{table}_temp_{os.getpid()}"  # Nome único usando PID
        cursor.execute(new_create_sql.replace(table, temp_table))
        
        # 6. Copia os dados
        columns_select = []
        for col in schema['columns']:
            col_name = col[1]
            if col_name in rename_map:
                columns_select.append(f'"{col_name}" AS "{rename_map[col_name]}"')
            else:
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
        
        # 8. Recria os índices
        for index_name, index_sql in schema['indexes']:
            new_index_sql = index_sql
            for old_name, new_name in rename_map.items():
                new_index_sql = new_index_sql.replace(f'"{old_name}"', f'"{new_name}"')
            cursor.execute(new_index_sql)
        
        conn.commit()
        conn.close()
        
        print(f"Tabela '{table}' atualizada com sucesso!")
        print("Colunas renomeadas:")
        for old, new in rename_map.items():
            print(f"  {old} → {new}")
        
        return True
        
    except Exception as e:
        print(f"Erro durante a renomeação: {str(e)}")
        conn.rollback()
        return False
    finally:
        if conn:
            conn.close()

def main():
    # Configura o parser de argumentos
    parser = argparse.ArgumentParser(
        description='Renomeia múltiplas colunas em bancos de dados SQLite',
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument('db_name', help='Nome do arquivo do banco de dados (deve estar na pasta bancos_de_dados)')
    parser.add_argument('table', help='Nome da tabela')
    parser.add_argument('renames', nargs='+', 
        help='Pares de nomes no formato antigo:novo\nExemplo: email:endereco_email nome:full_name')
    
    args = parser.parse_args()
    
    try:
        # Processa os mapeamentos de renomeação
        rename_map = {}
        for rename_pair in args.renames:
            if ':' not in rename_pair:
                raise ValueError(f"Formato inválido: '{rename_pair}'. Use antigo:novo")
            old, new = rename_pair.split(':', 1)
            rename_map[old.strip()] = new.strip()
        
        # Obtém o caminho absoluto do banco de dados
        db_path = get_db_path(args.db_name)
        
        print(f"Banco de dados: {db_path}")
        print(f"Tabela: {args.table}")
        print("Renomeações a serem aplicadas:")
        for old, new in rename_map.items():
            print(f"  {old} → {new}")
        
        # Confirmação do usuário
        confirm = input("\nConfirmar a operação? (s/n): ").strip().lower()
        if confirm != 's':
            print("Operação cancelada pelo usuário")
            return
        
        # Executa a renomeação
        success = rename_columns(db_path, args.table, rename_map)
        
        if not success:
            print("Falha ao renomear as colunas.")
            return
            
    except Exception as e:
        print(f"Erro: {str(e)}")

if __name__ == "__main__":
    main()