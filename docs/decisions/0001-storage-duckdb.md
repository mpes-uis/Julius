# ADR 0001 — Storage: DuckDB

- **Status:** Aceito
- **Data:** 2026-05-22
- **Decisor:** @pedropberger

## Contexto

A v1 usa **SQLite** com um arquivo `.db` por provider (`portaltp.db`, `tectrilha.db`, `agape&alphatec.db`). Schemas crescem dinamicamente via `ALTER TABLE ADD COLUMN` conforme campos novos aparecem nas respostas. Pipelines de pós-processamento ([legacy/Pipelines/Unir_bds.py](../../legacy/Pipelines/Unir_bds.py)) precisam carregar tabelas inteiras em pandas para unir bancos.

A v2 mira:
- Análises (tipologias de improbidade) sobre milhões de linhas.
- Possibilidade de ler CSVs/parquets externos diretamente.
- Joins eficientes entre tabelas de providers diferentes.
- Continuar simples (sem servidor de banco).

## Opções consideradas

| Opção | Prós | Contras |
|---|---|---|
| **SQLite** (manter v1) | Familiar; arquivos `.db` existentes; menor mudança | Lento em scans analíticos; limitação de tipos; sem leitura direta de parquet/csv; joins pesados puxam tudo pra memória |
| **DuckDB** | Colunar, otimizado para análise; lê CSV/parquet/JSON nativo; SQL completo; single-file; embedded como SQLite | API um pouco diferente; ecosistema menor |
| **PostgreSQL** | Padrão de mercado; multiusuário; ferramental maduro | Requer servidor; overhead de operação; overkill para single-machine |

## Decisão

**DuckDB**, single-file em `data/julius.duckdb`.

## Consequências

- Migração necessária dos `.db` antigos via `julius migrate-sqlite` (Fase 5).
- Pipelines `unify` e `normalize` ganham com SQL nativo (sem pandas no caminho quente).
- DuckDB lê CSV (`data/prefeituras.csv`) e YAML/JSON direto via extensões — pode simplificar carregamento de configs no futuro.
- Trade-off: agentes que mexerem no código precisam aprender quirks do DuckDB (`?` vs `$1`, tipos próprios, etc.).
- Possibilidade futura de exportar tabelas para parquet para análise externa.
