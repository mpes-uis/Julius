# Plano de Reestruturação — Julius 2.0

> **Status:** rascunho aprovado em 2026-05-22 por @pedropberger.
> Refatoração completa, reescrita do zero. Código legado preservado em `legacy/`.
> Mensagens da CLI em **PT-BR**. Código/docstrings/comentários em **inglês**.

Este documento é a fonte de verdade do plano. Agentes futuros devem ler antes de mexer no código e atualizar à medida que cada fase é concluída.

---

## 1. Contexto

Julius captura dados dos Portais da Transparência das 78 prefeituras do Estado do Espírito Santo. Cada portal usa um de 6 provedores de API distintos:

| Provedor | Status legado | Status v2 |
|---|---|---|
| `portaltp` | implementado | **fase 2** |
| `tectrilha` | implementado | fase 3 |
| `agape` | implementado (junto com alphatec) | fase 3 |
| `alphatec` | implementado (junto com agape) | fase 3 |
| `cr2` | **não implementado** | fase 4 |
| `portalfacil` | **não implementado** | fase 4 |

### Documentação descoberta

- **PortalTP:** endpoint de descoberta no padrão `https://{municipio}-es.portaltp.com.br/api/dadosabertos.aspx` (ex: [Santa Maria de Jetibá](https://santamariadejetiba-es.portaltp.com.br/api/dadosabertos.aspx)).
  - **Atenção:** a doc oficial mostra padrão **`/api/{grupo}/api-{recurso}.aspx`** (ex: `/api/compras/api-licitacoes.aspx`).
  - O código legado usa padrão **`{Grupo}/Get{Recurso}`** (ex: `Compras/GetLicitacoes`). Aparentemente são duas gerações de API coexistindo no PortalTP — **validar na fase 2** qual usar (provavelmente a nova).
  - Parâmetros comuns: `Ano` (All/2017-2026), `Mês` (All/Janeiro-Dezembro), `Formato` (JSON/TXT/CSV/XML).
  - Sem auth, sem paginação documentada, licença CC0.

- **Demais provedores:** documentação a coletar quando chegar a vez. Pedro deve fornecer ou os agentes devem buscar.

### Problemas críticos do código legado

1. [main.py:30-33](main.py#L30-L33) está quebrado — importa `scr.` (typo) e o módulo `agape&alphatec.py` não é importável em Python.
2. ~80% de duplicação entre [src/portaltp.py](src/portaltp.py), [src/tectrilha.py](src/tectrilha.py) e [src/agape&alphatec.py](src/agape&alphatec.py).
3. CR2 e PortalFacil sem implementação.
4. [data/prefeituras.csv](data/prefeituras.csv): capitalização inconsistente em `empresa`, URLs sem normalizar.
5. Schema dinâmico via `ALTER TABLE ADD COLUMN` conforme campos aparecem — frágil.
6. SQL com f-string interpolando nomes de tabela.
7. Sem testes, sem typing, sem config externa, sem logging estruturado.
8. Pipelines em [Pipelines/](Pipelines/) são patches manuais para problemas que não deveriam existir se a captura normalizasse na entrada.
9. Loop síncrono com `sleep(1)` — extração completa leva horas.
10. CSVs no root do projeto ([contratos_aditivos_2024.csv](contratos_aditivos_2024.csv), [licitacoes_consolidadas_2024.csv](licitacoes_consolidadas_2024.csv)) — **descartar, não é dado relevante** (confirmado por @pedropberger).

---

## 2. Stack escolhida

| Camada | Escolha |
|---|---|
| Empacotamento | `uv` + `pyproject.toml` |
| Validação | `pydantic` v2 |
| HTTP | `httpx` async + `tenacity` |
| Storage | **DuckDB** |
| CLI | **Typer** (mensagens em PT-BR) |
| Config | `pydantic-settings` (`.env`) |
| Logging | `structlog` (JSON em arquivo, formatado no console) |
| Testes | `pytest` + `respx` |
| Lint/format | `ruff` + `mypy --strict` |
| CI | GitHub Actions |
| Container | `python:3.12-slim` |

---

## 3. Arquitetura

```
julius/
├── pyproject.toml
├── .env.example
├── plan.md                     # este documento
├── README.md
├── Dockerfile
├── docker-compose.yml
│
├── julius/
│   ├── __init__.py
│   ├── cli.py                  # Typer entrypoint
│   ├── config.py               # Settings
│   ├── logging.py              # structlog setup
│   │
│   ├── models/
│   │   ├── prefeitura.py
│   │   ├── endpoint.py
│   │   └── job.py
│   │
│   ├── providers/
│   │   ├── base.py             # Provider ABC
│   │   ├── portaltp.py
│   │   ├── tectrilha.py
│   │   ├── agape.py
│   │   ├── alphatec.py
│   │   ├── cr2.py
│   │   ├── portalfacil.py
│   │   └── registry.py
│   │
│   ├── http/
│   │   ├── client.py           # httpx.AsyncClient
│   │   └── retry.py
│   │
│   ├── extractor/
│   │   ├── runner.py           # asyncio + Semaphore
│   │   ├── scheduler.py
│   │   └── checkpoint.py       # idempotência
│   │
│   ├── storage/
│   │   ├── duckdb_store.py
│   │   ├── schema.py
│   │   └── migrate_sqlite.py
│   │
│   ├── pipelines/
│   │   ├── normalize.py
│   │   ├── unify.py
│   │   └── format_processo.py
│   │
│   └── analysis/               # placeholder (fase 7)
│
├── data/
│   ├── prefeituras.csv
│   └── endpoints/
│       ├── portaltp.yaml
│       ├── tectrilha.yaml
│       ├── agape.yaml
│       ├── alphatec.yaml
│       ├── cr2.yaml
│       └── portalfacil.yaml
│
├── tests/
│   ├── fixtures/
│   ├── providers/
│   ├── extractor/
│   └── storage/
│
├── legacy/                     # código atual preservado
└── .github/workflows/ci.yml
```

### Interface do Provider (ABC)

```python
class Provider(ABC):
    name: ClassVar[str]

    @abstractmethod
    def list_endpoints(self) -> list[EndpointSpec]: ...

    @abstractmethod
    def build_url(self, base: str, endpoint: EndpointSpec, *, year: int, month: int | None, **kw) -> str: ...

    @abstractmethod
    async def fetch(self, client: httpx.AsyncClient, url: str) -> bytes: ...

    @abstractmethod
    def parse(self, payload: bytes) -> list[dict]: ...
```

### Comandos da CLI

```bash
julius extract --provider portaltp --from 01/2024 --to 12/2024
julius extract --all --from 01/2024 --to 12/2024
julius retry-failed --provider portaltp
julius status
julius pipeline normalize --table contratos
julius pipeline unify --tables contratos
julius migrate-sqlite legacy/bds/portaltp.db
```

---

## 4. Roadmap — Babysteps por Fase

> **Regra:** parar ao fim de cada fase, validar com @pedropberger antes de seguir.

### Fase 0 — Limpeza prévia

**Objetivo:** preservar legado e arrumar dados de entrada antes de qualquer código novo.

Babysteps:
1. Criar branch `refactor/v2`.
2. Criar pasta `legacy/`.
3. `git mv main.py legacy/main.py`.
4. `git mv src legacy/src`.
5. `git mv Pipelines legacy/Pipelines`.
6. `git mv dockerfile legacy/dockerfile`.
7. `git mv requirements.txt legacy/requirements.txt`.
8. Apagar `contratos_aditivos_2024.csv` e `licitacoes_consolidadas_2024.csv` do root (lixo confirmado).
9. Manter `data/` no lugar (vai evoluir, não mover).
10. Normalizar [data/prefeituras.csv](data/prefeituras.csv):
    - `empresa` em lowercase: `portaltp`, `tectrilha`, `agape`, `alphatec`, `cr2`, `portalfacil`.
    - URLs com `https://` explícito, sem trailing slash.
    - Coluna `unidadegestora` como inteiro nullable (não float).
    - Trim em `municipio`/`prefeitura`.
11. Commit: `chore(refactor): preserve legacy code under legacy/`.
12. Parar e validar.

**Critério de aceitação:** repo tem `legacy/` com tudo antigo, `data/prefeituras.csv` normalizado, root limpo, branch criada.

---

### Fase 1 — Fundação

**Objetivo:** esqueleto do projeto novo funcionando, `julius --help` responde, CI verde, sem providers ainda.

Babysteps:
1. Criar `pyproject.toml` com `uv`, definindo Python 3.12+, deps: `typer`, `httpx`, `pydantic`, `pydantic-settings`, `duckdb`, `structlog`, `tenacity`, `pyyaml`.
2. Dev deps: `pytest`, `pytest-asyncio`, `respx`, `ruff`, `mypy`.
3. Criar `julius/__init__.py` (`__version__ = "2.0.0-dev"`).
4. Criar `julius/config.py` com `Settings` (paths, paralelismo, timeouts, rate limit).
5. Criar `julius/logging.py` (structlog: JSON em `logs/`, console formatado).
6. Criar `julius/models/prefeitura.py` (pydantic model, validators de URL, enum de Provider).
7. Criar `julius/models/endpoint.py` (`EndpointSpec`: nome, path, params template, schema esperado).
8. Criar `julius/models/job.py` (`ExtractionJob`, `ExtractionResult`).
9. Criar `julius/providers/base.py` (ABC).
10. Criar `julius/providers/registry.py` (`get_provider(name) -> Provider`).
11. Criar `julius/cli.py` mínimo: `app = typer.Typer()`, comando `version`, comando `extract` stub que só imprime args.
12. Criar `.env.example` documentando variáveis.
13. Criar `.github/workflows/ci.yml`: ruff + mypy + pytest.
14. Criar `tests/conftest.py` vazio com fixtures básicas.
15. Configurar `ruff.toml` e `mypy.ini` (strict).
16. Rodar `uv run julius --help` e confirmar.
17. Rodar `uv run pytest` (sem testes ainda — verde por default).
18. Rodar `uv run ruff check . && uv run mypy julius`.
19. Commit: `feat(v2): foundation — pyproject, models, CLI skeleton, CI`.
20. Parar e validar.

**Critério de aceitação:** `julius --help` mostra comandos, CI passa, sem código de provider ainda.

---

### Fase 2 — Provider PortalTP end-to-end

**Objetivo:** extrair dados de **uma** prefeitura PortalTP via `julius extract --provider portaltp --from 01/2024 --to 01/2024 --only afonso-claudio` e gravar em DuckDB. Validar a arquitetura inteira aqui.

Babysteps:
1. Buscar a doc da API PortalTP para uma prefeitura: `GET https://{slug}-es.portaltp.com.br/api/dadosabertos.aspx`. Decidir entre padrão antigo (`Compras/GetLicitacoes`) e padrão novo (`/api/compras/api-licitacoes.aspx`) — testar ambos contra uma prefeitura real, optar pelo que retorna dados consistentes.
2. Criar `data/endpoints/portaltp.yaml` com a lista de endpoints (formato `EndpointSpec`).
3. Criar `julius/providers/portaltp.py`:
   - `class PortalTP(Provider)`.
   - `list_endpoints()` lê do YAML.
   - `build_url()` monta URL com `ano`/`mes`.
   - `fetch()` faz GET com httpx.
   - `parse()` decodifica utf-8-sig se necessário, json.loads, normaliza para `list[dict]`.
4. Criar `julius/http/client.py`: factory de `httpx.AsyncClient` com timeout, headers padrão, limits.
5. Criar `julius/http/retry.py`: política `tenacity` (3 tries, backoff exp, retry só em 5xx/timeout).
6. Criar `julius/storage/duckdb_store.py`:
   - `DuckDBStore` com `connect()`, `write_records(table, records, meta)`.
   - Cria tabela com colunas inferidas + colunas fixas (`municipio`, `prefeitura`, `ano`, `mes`, `extracted_at`).
   - Tipos: tudo TEXT inicialmente; pipeline de normalização cuida do resto.
7. Criar `julius/extractor/scheduler.py`:
   - `iter_jobs(provider, prefeituras, endpoints, date_range) -> Iterator[ExtractionJob]`.
8. Criar `julius/extractor/checkpoint.py`:
   - Tabela `_checkpoints` no DuckDB.
   - `already_extracted(job) -> bool`, `mark_done(job)`, `mark_failed(job, error)`.
9. Criar `julius/extractor/runner.py`:
   - `async def run(jobs, *, concurrency: int)` com `asyncio.Semaphore`.
   - Por host: rate limit (1 req/s por host, configurável).
   - Para cada job: skip se checkpoint, fetch, parse, write, mark_done.
10. Plugar comando `julius extract` em [julius/cli.py](julius/cli.py).
11. Testes:
    - `tests/providers/test_portaltp.py`: fixtures de resposta real, testa `build_url`, `parse`.
    - `tests/extractor/test_runner.py`: mock com `respx`, testa fluxo end-to-end.
    - `tests/storage/test_duckdb_store.py`: tabela criada, dados gravados, idempotência.
12. Rodar contra **uma** prefeitura real (Afonso Cláudio) para 1 mês, validar dados no DuckDB.
13. Rodar contra **5** prefeituras PortalTP para 1 mês, validar paralelismo.
14. Commit: `feat(v2): portaltp provider + extractor + duckdb storage`.
15. Parar e validar.

**Critério de aceitação:**
- Comando funciona para PortalTP.
- Reexecução não duplica (checkpoint).
- DuckDB tem tabelas por endpoint com metadata (`municipio`, `prefeitura`, `ano`, `mes`, `extracted_at`).
- Cobertura de testes >70% nos módulos novos.

---

### Fase 3 — Tectrilha, Agape, Alphatec

**Objetivo:** todos os providers do legado migrados.

Babysteps (por provider, repetir):
1. Coletar doc/exemplos do provider (Pedro fornece ou inspecionar URLs do legado).
2. Criar `data/endpoints/{provider}.yaml`.
3. Criar `julius/providers/{provider}.py` herdando de `Provider`.
4. Particularidades:
   - **Tectrilha:** parâmetro `unidadeGestoraId` (vem do CSV de prefeituras), `exercicio` em vez de `ano`, sem `mes`.
   - **Agape:** padrão similar ao PortalTP, mas paths em snake_case (`/api/agape/orcamento_receita`).
   - **Alphatec:** mesmo padrão de Agape. Herda da classe Agape se possível.
5. Testes por provider com fixtures.
6. Validar com 2-3 prefeituras reais por provider.
7. Commit por provider.
8. Parar e validar ao final dos 3.

**Critério de aceitação:** 4 providers do legado funcionam via `julius extract --provider X` ou `julius extract --all`.

---

### Fase 4 — CR2 e PortalFacil (novos)

**Objetivo:** cobrir os 7-8 municípios que estavam descobertos mas sem implementação.

Babysteps:
1. **CR2:** investigar [https://conceicaodabarra.cr2.site/](https://conceicaodabarra.cr2.site/) — buscar página de dados abertos / API.
2. Documentar endpoints encontrados em `data/endpoints/cr2.yaml`.
3. Implementar `julius/providers/cr2.py`.
4. Testes + validação contra Conceição da Barra.
5. **PortalFacil:** investigar [https://dadosabertos-portalfacil.azurewebsites.net/](https://dadosabertos-portalfacil.azurewebsites.net/) — os IDs no CSV (391, 209, 438) indicam que a URL é parametrizada por ID de município.
6. Documentar em `data/endpoints/portalfacil.yaml`.
7. Implementar `julius/providers/portalfacil.py`.
8. Testes + validação contra Dores do Rio Preto / Iconha / Itapemirim.
9. Commit por provider.
10. Parar e validar.

**Critério de aceitação:** todos os 78 municípios extraíveis. `julius extract --all` cobre 100%.

---

### Fase 5 — Pipelines + migração SQLite

**Objetivo:** trazer transformações de [legacy/Pipelines/](legacy/Pipelines/) para a v2 e importar dados antigos.

Babysteps:
1. Portar [legacy/Pipelines/RenameColumns.py](legacy/Pipelines/RenameColumns.py) → `julius/pipelines/normalize.py` (renomear via dict declarativo).
2. Portar [legacy/Pipelines/RemoveColumns.py](legacy/Pipelines/RemoveColumns.py) → idem.
3. Portar [legacy/Pipelines/Type.py](legacy/Pipelines/Type.py) → coerção de tipos via pydantic ou DuckDB CAST.
4. Portar [legacy/Pipelines/JuntarProcessoTectrilha.py](legacy/Pipelines/JuntarProcessoTectrilha.py) → `julius/pipelines/format_processo.py`.
5. Portar [legacy/Pipelines/Unir_bds.py](legacy/Pipelines/Unir_bds.py) → `julius/pipelines/unify.py` (DuckDB nativo: `INSERT INTO ... SELECT ...`).
6. Plugar em `julius pipeline` (subcomando Typer).
7. Criar `julius/storage/migrate_sqlite.py`: lê `.db` antigo e copia tabelas para DuckDB.
8. Comando `julius migrate-sqlite legacy/bds/{file}.db`.
9. Testes.
10. Commit.
11. Parar e validar.

**Critério de aceitação:** dados antigos migráveis, pipelines de normalização rodam via CLI.

---

### Fase 6 — Docker + docs

**Objetivo:** entregar imagem e documentação.

Babysteps:
1. Novo `Dockerfile` com `python:3.12-slim`, multi-stage, `uv` para deps.
2. `docker-compose.yml` com volume persistente para DuckDB e logs.
3. Atualizar `README.md`: instalação, comandos, exemplos, arquitetura, contribuição.
4. Atualizar `README-ptbr.md` espelhando.
5. Adicionar diagramas (arquitetura, fluxo de extração) — usar mermaid no markdown.
6. Apagar `legacy/` se @pedropberger confirmar.
7. Commit.
8. Tag `v2.0.0`.

**Critério de aceitação:** `docker compose up` extrai dados; README cobre instalação e uso.

---

### Fase 7 — Análise (futuro, fora do escopo desta refatoração)

Esqueleto em `julius/analysis/` para tipologias:
- Acumulação indevida de cargos públicos (Julius original).
- Preços anômalos em contratos/licitações.
- Servidores com remuneração fora de faixa.
- Concentração de fornecedores.

Não implementar agora. A arquitetura (DuckDB + dados normalizados) já suporta — adicionar quando houver demanda.

---

## 5. Convenções

- **Idioma do código:** inglês (nomes, comentários, docstrings).
- **Idioma da CLI / logs do usuário:** PT-BR.
- **Commits:** Conventional Commits (`feat:`, `fix:`, `chore:`, `refactor:`, `test:`, `docs:`).
- **Branch:** `refactor/v2` durante a refatoração. Merge final para `main` com squash.
- **Testes:** todo provider novo tem fixtures de resposta real + testes de `build_url` e `parse`.
- **Sem `print()`** no código de produção — só `structlog`.
- **Sem f-string em SQL** — usar parâmetros ou `duckdb.sql()` com bindings.
- **Sem `ALTER TABLE ADD COLUMN` dinâmico** — schema é declarado no YAML do endpoint, ou tudo TEXT na ingestão e tipagem fica para o pipeline `normalize`.

## 6. Riscos conhecidos

- **APIs instáveis:** algumas prefeituras (especialmente as menores) têm portais offline com frequência. Plano: retry + log + skip, nunca falhar a execução inteira por uma URL.
- **Mudança de padrão PortalTP:** doc mostra padrão novo (`api-licitacoes.aspx`) mas código antigo usa antigo (`GetLicitacoes`). Resolver na fase 2 antes de espalhar.
- **Rate limit:** portais não documentam — usar 1 req/s por host como default conservador, configurável.
- **Encoding:** algumas APIs retornam `utf-8-sig` (BOM). Sempre tratar.
- **Schemas inconsistentes entre municípios do mesmo provider:** mesmo dentro do PortalTP, campos podem variar. Pipeline `normalize` cuida disso pós-ingestão.

## 7. Como agentes futuros devem usar este documento

1. Ler este `plan.md` inteiro antes de qualquer mudança.
2. Identificar qual fase está em andamento (verificar branch, commits, código).
3. Seguir os babysteps da fase atual em ordem.
4. Atualizar a seção "Status" da fase ao concluir (marcar com ✅).
5. **Parar e pedir validação ao @pedropberger ao fim de cada fase** — não emendar fases sem confirmação.
6. Se descobrir algo novo (ex: API muda, novo provider aparece), adicionar ao documento e ao registro de "Documentação descoberta" / "Riscos".

## 8. Status atual

- [x] Fase 0 — Limpeza prévia ✅ (commit 9c7698a)
- [x] Fase 1 — Fundação ✅
- [x] Fase 2 — Provider PortalTP end-to-end ✅
- [x] Fase 3 — Tectrilha, Agape, Alphatec ✅
- [ ] Fase 4 — CR2 e PortalFacil
- [ ] Fase 5 — Pipelines + migração SQLite
- [ ] Fase 6 — Docker + docs
- [ ] Fase 7 — Análise (futuro)

> Atualizar marcação acima ao final de cada fase.
