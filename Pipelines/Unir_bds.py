import sqlite3
from pathlib import Path
import pandas as pd

def unificar_tabelas_sqlite(
        caminho_bd1: str,
        nome_tabela1: str,
        caminho_bd2: str,
        nome_tabela2: str,
        caminho_bd_saida: str,
        nome_tabela_saida: str,
        colunas_chave: list = None
):
    """
    Unifica tabelas de dois bancos de dados SQLite em uma nova tabela.
    
    Parâmetros:
        caminho_bd1: Caminho para o primeiro banco de dados
        nome_tabela1: Nome da tabela no primeiro banco de dados
        caminho_bd2: Caminho para o segundo banco de dados
        nome_tabela2: Nome da tabela no segundo banco de dados
        caminho_bd_saida: Caminho para o banco de dados de saída
        nome_tabela_saida: Nome da tabela de saída
        colunas_chave: Lista de colunas para verificação de duplicatas (opcional)
    """
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
            df.columns = [col.strip().lower() for col in df.columns]
            return df.loc[:, ~df.columns.duplicated()]

        df1 = normalizar_colunas(df1)
        df2 = normalizar_colunas(df2)

        # Função para converter a coluna valor para numérico
        def converter_coluna_valor(df):
            if 'valor' in df.columns:
                # Converte para numérico, forçando valores inválidos para NaN
                df['valor'] = pd.to_numeric(df['valor'], errors='coerce')
            return df

        # Converte a coluna 'valor' em ambos os DataFrames
        df1 = converter_coluna_valor(df1)
        df2 = converter_coluna_valor(df2)

        print("\nResumo dos dados originais:")
        print(f"- Tabela 1: {len(df1)} registros, {len(df1.columns)} colunas")
        print(f"- Tabela 2: {len(df2)} registros, {len(df2.columns)} colunas")

        # Verifica estrutura das tabelas
        print("\nEstrutura das tabelas:")
        print("Tabela 1 - Tipos de dados:\n", df1.dtypes)
        print("\nTabela 2 - Tipos de dados:\n", df2.dtypes)

        print("\nSobreposição de colunas:")
        colunas_comuns = set(df1.columns) & set(df2.columns)
        print("Colunas em comum:", colunas_comuns)
        print("Colunas exclusivas Tabela 1:", set(df1.columns) - set(df2.columns))
        print("Colunas exclusivas Tabela 2:", set(df2.columns) - set(df1.columns))

        # Unificação com preservação de dados
        print("\nProcessando unificação...")
        
        # Se houver colunas em comum, faz merge, senão concatena
        if colunas_comuns:
            # Usa outer join para preservar todos os registros
            df_unificado = pd.merge(
                df1, 
                df2, 
                how='outer', 
                on=list(colunas_comuns),
                suffixes=('_bd1', '_bd2')
            )
            print(f"Registros após merge (outer join): {len(df_unificado)}")
        else:
            # Concatena simples se não houver colunas em comum
            df_unificado = pd.concat([df1, df2], ignore_index=True)
            print(f"Registros após concatenação: {len(df_unificado)}")

        # Remove duplicatas baseadas em colunas chave (se especificado)
        if colunas_chave:
            # Verifica se as colunas chave existem
            colunas_faltantes = [col for col in colunas_chave if col not in df_unificado.columns]
            if colunas_faltantes:
                print(f"\nAVISO: Colunas chave não encontradas: {colunas_faltantes}")
                print("Removendo duplicatas completas (todas as colunas)")
                df_unificado = df_unificado.drop_duplicates()
            else:
                print(f"\nRemovendo duplicatas baseadas nas colunas: {colunas_chave}")
                df_unificado = df_unificado.drop_duplicates(subset=colunas_chave)
        else:
            print("\nRemovendo duplicatas completas (todas as colunas)")
            df_unificado = df_unificado.drop_duplicates()

        print(f"\nRegistros após remoção de duplicatas: {len(df_unificado)}")

        # Verifica valores NaN na coluna 'valor' se existir
        if 'valor' in df_unificado.columns:
            nan_count = df_unificado['valor'].isna().sum()
            if nan_count > 0:
                print(f"\nAVISO: {nan_count} valores não numéricos foram convertidos para NaN na coluna 'valor'")

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
        nome_tabela1="bensimoveis",
        caminho_bd2="bds/tectrilha.db",
        nome_tabela2="bensImoveis",
        caminho_bd_saida="bds/bensImoveis.db",
        nome_tabela_saida="bensImoveis",
        colunas_chave=[]
    )