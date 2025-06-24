import sqlite3
from pathlib import Path
import pandas as pd

def unificar_tabelas_sqlite(
        caminho_bd1: str,
        nome_tabela1: str,
        caminho_bd2: str,
        nome_tabela2: str,
        caminho_bd_saida: str,
        nome_tabela_saida: str
):
    # Verificação de caminhos
    caminho_bd1 = Path(caminho_bd1).absolute()
    caminho_bd2 = Path(caminho_bd2).absolute()
    print(f"\nVerificando bancos de dados:")
    print(f"- BD1: {caminho_bd1} {'EXISTE' if caminho_bd1.exists() else 'NÃO ENCONTRADO'}")
    print(f"- BD2: {caminho_bd2} {'EXISTE' if caminho_bd2.exists() else 'NÃO ENCONTRADO'}")

    if not caminho_bd1.exists() or not caminho_bd2.exists():
        raise FileNotFoundError("Um ou mais bancos de dados não foram encontrados")

    try:
        # Conexão em modo somente leitura
        conn1 = sqlite3.connect(f"file:{caminho_bd1}?mode=ro", uri=True)
        conn2 = sqlite3.connect(f"file:{caminho_bd2}?mode=ro", uri=True)

        # Função para carregar tabelas com tratamento de nomes
        def carregar_tabela_segura(conn, nome_tabela):
            tabelas_disponiveis = pd.read_sql(
                "SELECT name FROM sqlite_master WHERE type='table'", 
                conn
            )['name'].tolist()
            
            # Busca case-insensitive e remove duplicatas
            tabelas_validas = [t for t in tabelas_disponiveis 
                             if t.lower() == nome_tabela.lower()]
            
            if not tabelas_validas:
                raise ValueError(f"Tabela {nome_tabela} não encontrada. Tabelas disponíveis: {tabelas_disponiveis}")
            
            return tabelas_validas[0]

        print("\nCarregando tabelas...")
        nome_real_tabela1 = carregar_tabela_segura(conn1, nome_tabela1)
        nome_real_tabela2 = carregar_tabela_segura(conn2, nome_tabela2)
        print(f"- Tabela 1 carregada: {nome_real_tabela1}")
        print(f"- Tabela 2 carregada: {nome_real_tabela2}")

        # Carrega os dados
        df1 = pd.read_sql(f'SELECT * FROM "{nome_real_tabela1}"', conn1)
        df2 = pd.read_sql(f'SELECT * FROM "{nome_real_tabela2}"', conn2)

        # Normaliza nomes de colunas (remove duplicatas case-insensitive)
        def normalizar_colunas(df):
            df.columns = [col.lower() for col in df.columns]
            return df.loc[:, ~df.columns.duplicated()]

        df1 = normalizar_colunas(df1)
        df2 = normalizar_colunas(df2)

        print("\nResumo dos dados após normalização:")
        print(f"- Tabela 1: {len(df1)} registros, {len(df1.columns)} colunas")
        print(f"- Tabela 2: {len(df2)} registros, {len(df2.columns)} colunas")

        # Unificação
        df_unificado = pd.concat([df1, df2], ignore_index=True)
        
        # Remove possíveis duplicatas completas
        df_unificado = df_unificado.drop_duplicates()

        # Salva o resultado
        conn_saida = sqlite3.connect(caminho_bd_saida)
        df_unificado.to_sql(
            nome_tabela_saida, 
            conn_saida, 
            if_exists='replace', 
            index=False
        )
        
        print(f"\nUnificação concluída com sucesso!")
        print(f"Tabela criada em: {Path(caminho_bd_saida).absolute()}")
        print(f"Total de registros: {len(df_unificado)}")
        print(f"Colunas: {', '.join(df_unificado.columns)}")

    except Exception as e:
        print(f"\nERRO CRÍTICO: {str(e)}")
        raise
    finally:
        conn1.close()
        conn2.close()
        if 'conn_saida' in locals():
            conn_saida.close()

if __name__ == "__main__":
    unificar_tabelas_sqlite(
        caminho_bd1="bds/portaltp.db",
        nome_tabela1="contratosaditivos",
        caminho_bd2="bds/tectrilha.db",
        nome_tabela2="contratos",
        caminho_bd_saida="bds/contratos.db",
        nome_tabela_saida="contratos"
    )