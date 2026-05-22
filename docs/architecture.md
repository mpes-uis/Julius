# Arquitetura — Julius 2.0

> Documento técnico vivo. Atualizar quando houver mudança estrutural relevante.

## 1. Visão de alto nível

```
┌─────────────────────────────────────────────────────────────┐
│  CLI (Typer) — entrypoint                                   │
│  julius extract | retry-failed | status | pipeline | ...    │
└─────────────────────────────────────────────────────────────┘
                            │
              ┌─────────────┴─────────────┐
              ▼                           ▼
      ┌──────────────┐            ┌──────────────┐
      │  Extractor   │            │   Pipeline   │
      │   Runner     │            │  (Normalize, │
      │ (async +    │             │   Unify)     │
      │  Semaphore)  │            └──────┬───────┘
      └──────┬───────┘                   │
             │                           │
             ▼                           ▼
      ┌──────────────┐            ┌──────────────┐
      │  Provider    │            │   Storage    │
      │ (PortalTP,   │───────────▶│   (DuckDB)   │
      │  Tectrilha,  │            │              │
      │  Agape, ...) │            └──────────────┘
      └──────┬───────┘                   ▲
             │                           │
             ▼                           │
      ┌──────────────┐                   │
      │  HTTP Client │                   │
      │  (httpx +    │                   │
      │   tenacity)  │                   │
      └──────────────┘                   │
                                         │
      ┌──────────────┐                   │
      │  Checkpoint  │───────────────────┘
      │ (idempotência)│
      └──────────────┘
```

## 2. Componentes

### 2.1 CLI (`julius/cli.py`)

Typer app. Comandos principais:

```
julius extract --provider <p> --from MM/AAAA --to MM/AAAA [--only <slug>]
julius extract --all --from MM/AAAA --to MM/AAAA
julius retry-failed --provider <p>
julius status
julius pipeline normalize --table <t>
julius pipeline unify --tables <t1,t2,...>
julius migrate-sqlite <path>
```

Mensagens em PT-BR. Sem prompts interativos (compatível com cron/Airflow/Docker).

### 2.2 Models (`julius/models/`)

Pydantic v2.

- **`Prefeitura`**: `id`, `nome`, `municipio_slug`, `url_base`, `provider` (enum), `unidade_gestora` (Optional[int]).
- **`Provider`** (enum): `portaltp`, `tectrilha`, `agape`, `alphatec`, `cr2`, `portalfacil`.
- **`EndpointSpec`**: `name`, `path_template`, `params` (list de `ParamSpec`), `freq` (`yearly` ou `monthly`).
- **`ExtractionJob`**: `prefeitura`, `endpoint`, `year`, `month` (Optional).
- **`ExtractionResult`**: `job`, `status` (`success`, `empty`, `failed`, `skipped`), `records_count`, `error` (Optional), `duration_ms`.

### 2.3 Providers (`julius/providers/`)

ABC `Provider` define a interface comum. Cada implementação encapsula as quirks de um portal:

```python
class Provider(ABC):
    name: ClassVar[str]

    @abstractmethod
    def list_endpoints(self) -> list[EndpointSpec]: ...

    @abstractmethod
    def build_url(self, prefeitura: Prefeitura, endpoint: EndpointSpec, *, year: int, month: int | None) -> str: ...

    async def fetch(self, client: httpx.AsyncClient, url: str) -> bytes:
        # default impl: GET, raise_for_status
        ...

    @abstractmethod
    def parse(self, payload: bytes) -> list[dict]: ...
```

Registry (`registry.py`) faz `get_provider(name) -> Provider`.

### 2.4 HTTP (`julius/http/`)

- `client.py`: factory de `httpx.AsyncClient` com timeout, headers (User-Agent custom), limits.
- `retry.py`: política `tenacity` — 3 tentativas, backoff exponencial (1s, 2s, 4s), retry só em 5xx, timeout, `httpx.ConnectError`.

### 2.5 Extractor (`julius/extractor/`)

- `scheduler.py`: gera lista de `ExtractionJob` a partir de (provider, prefeituras, endpoints, date_range).
- `checkpoint.py`: tabela `_checkpoints` no DuckDB com schema `(provider, prefeitura, endpoint, year, month, status, last_attempt, error)`. Funções `already_extracted(job) -> bool`, `mark_done`, `mark_failed`.
- `runner.py`: `async run(jobs, *, concurrency)`. Usa `asyncio.Semaphore(concurrency)` global + `Semaphore(1)` por host para rate limit. Para cada job: skip se checkpoint, fetch (com retry), parse, write, mark_done. Erros não derrubam o run — só são logados e marcados.

### 2.6 Storage (`julius/storage/`)

DuckDB único arquivo (`data/julius.duckdb`).

- `duckdb_store.py`:
  - `connect() -> duckdb.Connection`.
  - `write_records(table: str, records: list[dict], meta: dict)` — cria tabela se não existe (todas as colunas TEXT), insere com colunas fixas (`municipio`, `prefeitura`, `provider`, `ano`, `mes`, `extracted_at`).
  - Sem `ALTER TABLE` dinâmico no caminho quente: se aparecer coluna nova, ela é serializada em uma coluna `_extras` (JSON) e tratada pelo pipeline de normalização.
- `schema.py`: criação das tabelas fixas (`_checkpoints`, `_errors`).
- `migrate_sqlite.py`: lê `.db` antigo (sqlite3) e copia tabelas para DuckDB. Usado uma vez na transição.

### 2.7 Pipelines (`julius/pipelines/`)

Transformações pós-ingestão. Idempotentes. Lêem da DuckDB e escrevem na DuckDB.

- `normalize.py`: padronização de nomes de colunas, tipos, encoding, dedup.
- `unify.py`: consolida mesma entidade entre providers (ex: `contratos` do PortalTP + `contratos` da Tectrilha → `unified.contratos`).
- `format_processo.py`: caso específico de Tectrilha (formatar `processo/ano`).

### 2.8 Analysis (`julius/analysis/`)

Placeholder para tipologias. Fora do escopo das Fases 0-6.

## 3. Fluxo de extração (Fase 2+)

1. CLI parse args → cria lista de jobs via `scheduler.iter_jobs(...)`.
2. Para cada job: `checkpoint.already_extracted(job)` → se sim, skip.
3. `provider.build_url(...)` → URL.
4. Com semaphore: `await provider.fetch(client, url)` (retry via tenacity).
5. `provider.parse(payload)` → list[dict].
6. `storage.write_records(table=endpoint.name, records, meta={...})`.
7. `checkpoint.mark_done(job)`.
8. Em qualquer falha: `checkpoint.mark_failed(job, error)`, log estruturado, segue para o próximo.

## 4. Configuração

`.env` carregado por `pydantic-settings`. Chaves:

```
JULIUS_DATA_DIR=./data
JULIUS_DB_PATH=./data/julius.duckdb
JULIUS_LOG_DIR=./logs
JULIUS_LOG_LEVEL=INFO
JULIUS_HTTP_TIMEOUT=30
JULIUS_HTTP_CONCURRENCY=20
JULIUS_HTTP_PER_HOST_DELAY=1.0
JULIUS_RETRY_MAX_ATTEMPTS=3
```

## 5. Logs

`structlog` com dois sinks:

- Console: formatação humana, nível INFO+.
- Arquivo `logs/julius.jsonl`: JSON estruturado, todos os níveis. Rotação diária.

Eventos canônicos: `extraction.started`, `extraction.job.skipped`, `extraction.job.success`, `extraction.job.failed`, `extraction.finished`, `http.request`, `storage.write`, `checkpoint.mark_done`.

## 6. Testes

Estratégia em camadas:

- **Unit:** `build_url`, `parse`, validators de model, lógica de scheduler.
- **Integration (mock HTTP):** `respx` mocka respostas; testa runner end-to-end contra DuckDB em memória.
- **Smoke (rede real, opt-in):** marker `@pytest.mark.smoke`; pega 1 prefeitura por provider, 1 mês, valida que extrai algo. Roda manualmente.

## 7. Decisões registradas (ADRs)

- [0001 — Storage: DuckDB](decisions/0001-storage-duckdb.md)
- [0002 — HTTP: httpx async](decisions/0002-http-async-httpx.md)
- [0003 — CLI: Typer](decisions/0003-cli-typer.md)
- [0004 — Rollout: faseado](decisions/0004-phased-rollout.md)
- [0005 — Idioma: código EN, CLI PT-BR](decisions/0005-idioma-codigo-cli.md)
