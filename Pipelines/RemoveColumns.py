import sqlite3
import argparse
import os
import re
from pathlib import Path
from typing import Dict, List, Tuple, Optional

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
    result = cursor.fetchone()
    create_sql = result[0] if result else None
    
    cursor.execute(f"SELECT name, sql FROM sqlite_master WHERE type='index' AND tbl_name=?", (table,))
    indexes = cursor.fetchall()
    
    return {
        'columns': columns,
        'create_sql': create_sql,
        'indexes': indexes
    }

def clean_create_sql(create_sql: str, columns_to_drop: List[str]) -> str:
    """Processa a SQL de criação para remover as colunas especificadas"""
    if not create_sql:
        raise ValueError("SQL de criação da tabela não disponível")

    # Padroniza a SQL para processamento
    create_sql = create_sql.replace('\n', ' ')
    create_sql = re.sub(r'\s+', ' ', create_sql).strip()

    # Encontra a parte entre parênteses
    start = create_sql.find('(')
    end = create_sql.rfind(')')
    if start == -1 or end == -1:
        raise ValueError("SQL de criação mal formada - parênteses não encontrados")

    preamble = create_sql[:start+1]
    postamble = create_sql[end:]
    columns_part = create_sql[start+1:end]

    # Divide as definições de colunas
    column_defs = [col.strip() for col in columns_part.split(',') if col.strip()]
    
    # Filtra colunas a serem removidas
    def should_keep(col_def: str) -> bool:
        col_name = re.split(r'\s', col_def)[0].strip('"\'[]`')
        return col_name not in columns_to_drop

    filtered_columns = [col for col in column_defs if should_keep(col)]

    # Reconstrói a SQL
    if not filtered_columns:
        raise ValueError("Não é possível remover todas as colunas")

    new_columns_part = ', '.join(filtered_columns)
    new_create_sql = preamble + new_columns_part + postamble

    return new_create_sql

def drop_columns(db_path: Path, table: str, columns_to_drop: List[str]) -> bool:
    """Remove colunas de uma tabela SQLite"""
    conn = None
    try:
        conn = sqlite3.connect(str(db_path))
        conn.execute("PRAGMA foreign_keys=OFF")
        cursor = conn.cursor()
        
        # Verifica se a tabela existe
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
        if not cursor.fetchone():
            raise ValueError(f"Tabela '{table}' não existe")

        # Obtém schema
        schema = get_table_schema(conn, table)
        existing_columns = [col[1] for col in schema['columns']]
        
        # Validações
        for col in columns_to_drop:
            if col not in existing_columns:
                raise ValueError(f"Coluna '{col}' não existe")

        if len(existing_columns) - len(columns_to_drop) < 1:
            raise ValueError("A tabela deve ter pelo menos uma coluna restante")

        # Processa SQL de criação
        new_create_sql = clean_create_sql(schema['create_sql'], columns_to_drop)
        
        # Cria tabela temporária
        temp_table = f"{table}_temp_{os.getpid()}"
        cursor.execute(new_create_sql.replace(table, temp_table))
        
        # Copia dados
        columns_select = [f'"{col}"' for col in existing_columns if col not in columns_to_drop]
        cursor.execute(f"INSERT INTO {temp_table} SELECT {', '.join(columns_select)} FROM {table}")
        
        # Remove original e renomeia temporária
        cursor.execute(f"DROP TABLE {table}")
        cursor.execute(f"ALTER TABLE {temp_table} RENAME TO {table}")
        
        # Recria índices válidos
        for index_name, index_sql in schema['indexes']:
            if index_sql and not any(col in index_sql for col in columns_to_drop):
                try:
                    cursor.execute(index_sql)
                except sqlite3.OperationalError:
                    print(f"Aviso: Índice {index_name} não pôde ser recriado")
        
        conn.commit()
        
        print(f"\n✅ Tabela '{table}' atualizada com sucesso!")
        print("\n🚫 Colunas removidas:")
        for col in columns_to_drop:
            print(f"  - {col}")
        
        remaining = [col for col in existing_columns if col not in columns_to_drop]
        print("\n✅ Colunas restantes:")
        for col in remaining:
            print(f"  - {col}")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Erro durante a remoção: {str(e)}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()

def main():
    parser = argparse.ArgumentParser(
        description='Remove colunas de tabelas SQLite',
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument('db_name', help='Nome do banco de dados')
    parser.add_argument('table', help='Nome da tabela')
    parser.add_argument('columns', nargs='+', help='Colunas a remover')
    
    args = parser.parse_args()
    
    try:
        db_path = get_db_path(args.db_name)
        
        print(f"\n📁 Banco de dados: {db_path}")
        print(f"📊 Tabela: {args.table}")
        print("🗑️ Colunas a remover:")
        for col in args.columns:
            print(f"  - {col}")
        
        confirm = input("\n⚠️ Confirmar? (s/n): ").strip().lower()
        if confirm != 's':
            print("\nOperação cancelada")
            return
        
        print("\n⚙️ Processando...")
        if drop_columns(db_path, args.table, args.columns):
            print("\n🎉 Operação concluída com sucesso!")
        else:
            print("\n❌ Falha na operação")
            
    except Exception as e:
        print(f"\n❌ Erro: {str(e)}")

if __name__ == "__main__":
    main()