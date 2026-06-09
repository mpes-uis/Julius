# Julius — Backlog Fase 5 (Pipelines + Migração)

> Criado após conclusão das Fases 0-3. Fase 4 (CR2/PortalFacil) adiada — APIs não acessíveis sem engenharia reversa com browser.
>
> **Convenção:** tarefas são babysteps da Fase 5 do [plan.md](plan.md). Execute em ordem, valide com @pedropberger antes de iniciar Fase 6.

---

## Fase 5 — Pipelines de normalização + migração SQLite

### 5.1 — Comando `julius migrate-sqlite`

- [ ] Criar `julius/storage/migrate_sqlite.py`
  - Lê `.db` SQLite com `sqlite3`
  - Para cada tabela: lê em chunks, chama `DuckDBStore.write_records()`
  - Preserva metadados se existirem colunas `municipio`, `prefeitura`, `ano`, `mes`
  - Progresso via `structlog`
- [ ] Plugar no CLI: `julius migrate-sqlite <caminho.db> [--provider <nome>]`
- [ ] Testes: `tests/storage/test_migrate_sqlite.py`
- [ ] Smoke test: migrar um dos `.db` de `legacy/bds/` (se existir)

### 5.2 — Pipeline `normalize`

- [ ] Criar `julius/pipelines/normalize.py`
  - `normalize_column_names(table, conn)` — snake_case, sem acentos (reutilizar `_safe()` do store)
  - `cast_numeric_columns(table, conn)` — detecta colunas que são números e faz `CAST` em DuckDB
  - `deduplicate(table, conn, key_cols)` — remove duplicatas por colunas chave
  - `drop_columns(table, conn, cols)` — porta do `legacy/Pipelines/RemoveColumns.py`
  - `rename_columns(table, conn, rename_map)` — porta do `legacy/Pipelines/RenameColumns.py`
- [ ] Plugar no CLI: `julius pipeline normalize --table <nome> [--cast] [--dedup]`
- [ ] Testes unitários com DuckDB in-memory

### 5.3 — Pipeline `unify`

- [ ] Criar `julius/pipelines/unify.py`
  - `unify_tables(tables, output_table, conn)` — equivalente ao `legacy/Pipelines/Unir_bds.py`
  - Usa `INSERT INTO ... SELECT` nativo do DuckDB (sem pandas)
  - Normaliza colunas antes de unir (snake_case, lowercase)
  - Relatório: colunas em comum, exclusivas, total de registros
- [ ] Plugar no CLI: `julius pipeline unify --tables contratos,licitacoes --output unified_compras`
- [ ] Testes

### 5.4 — Pipeline `format-processo` (Tectrilha)

- [ ] Criar `julius/pipelines/format_processo.py`
  - Porta do `legacy/Pipelines/JuntarProcessoTectrilha.py`
  - `format_processo(table, conn, processo_col, ano_col, output_col)`
  - Cria coluna `processo_formatado` = `numero/ano`
- [ ] Plugar no CLI: `julius pipeline format-processo --table contratos`

### 5.5 — Documentação dos pipelines

- [ ] Atualizar `docs/architecture.md` seção 2.7 (Pipelines) com detalhes da implementação
- [ ] Atualizar `README.md` e `README-ptbr.md` com exemplos de uso dos comandos de pipeline

---

## Fase 4 adiada — CR2 e PortalFacil

> **Diagnóstico (2026-06-05):** APIs inacessíveis sem engenharia reversa com browser.
>
> - **CR2 (Conceição da Barra):** WordPress puro, sem REST API de transparência exposta.
>   Dados em tabelas customizadas, sem endpoint público documentado.
> - **PortalFacil (Dores do Rio Preto, Iconha, Itapemirim):** Angular SPA com IIS
>   que redireciona todas as rotas para `index.html`. URL da API backend embutida
>   no JS minificado sem padrão claro. Precisaria de DevTools/browser para capturar.
>
> **Municípios afetados:** 4 de 78 (5%). Julius cobre 74/78 (95%) com as Fases 0-3.
>
> **Como desbloquear:** abrir DevTools no browser, ir para Network tab, filtrar XHR/Fetch,
> navegar até licitações/contratos no portal da prefeitura e capturar o padrão de URL.
> Depois criar YAML em `data/endpoints/` e implementar provider em `julius/providers/`.

- [ ] **CR2:** investigar com DevTools → `data/endpoints/cr2.yaml` → `julius/providers/cr2.py`
- [ ] **PortalFacil:** investigar com DevTools → `data/endpoints/portalfacil.yaml` → `julius/providers/portalfacil.py`
