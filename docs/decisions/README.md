# Architectural Decision Records (ADRs)

Decisões arquiteturais imutáveis. Cada arquivo registra **contexto, opções, decisão e consequências** de uma escolha técnica relevante.

## Como usar

- **Ler** antes de questionar uma decisão — talvez já tenha sido considerada.
- **Adicionar** um novo ADR quando uma decisão técnica relevante for tomada (escolha de lib, padrão arquitetural, mudança de paradigma).
- **Não editar** ADRs existentes após o merge. Para reverter uma decisão, criar um novo ADR com status "Substitui XXXX".
- **Numerar** sequencialmente: `NNNN-titulo-kebab.md` (ex: `0006-cache-redis.md`).

## Índice

- [0001 — Storage: DuckDB](0001-storage-duckdb.md)
- [0002 — HTTP: httpx async](0002-http-async-httpx.md)
- [0003 — CLI: Typer](0003-cli-typer.md)
- [0004 — Rollout faseado](0004-phased-rollout.md)
- [0005 — Idioma: código EN, CLI PT-BR](0005-idioma-codigo-cli.md)
