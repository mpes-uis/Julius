# Convenções de Código e Processo

## Idioma

| Onde | Idioma |
|---|---|
| Nomes de variáveis, classes, funções, módulos | Inglês |
| Comentários no código | Inglês |
| Docstrings | Inglês |
| Mensagens da CLI (stdout/stderr para o usuário) | PT-BR |
| Logs estruturados (`event=...`) | Inglês (chaves) |
| Logs de mensagem livre (`message=...`) | EN |
| `README.md` | Inglês |
| `README-ptbr.md` | PT-BR |
| `CLAUDE.md`, `plan.md`, `docs/` | PT-BR |
| Commits | Inglês (Conventional Commits) |
| Issue/PR descriptions | Indiferente, preferência PT-BR |

> **Razão:** README do projeto declara explicitamente que código e comentários ficam em inglês para servir como ferramenta de aprendizado. CLI em PT-BR porque o usuário final é brasileiro (servidores públicos, ONGs).

## Estilo de código

- **Formatter / linter:** `ruff` (substitui black, isort, flake8). Config em `pyproject.toml`.
- **Type checker:** `mypy --strict`. Sem `# type: ignore` sem comentário explicando.
- **Python:** 3.12+. Pode usar `match`, `Self`, PEP 695 generics, etc.
- **Imports:** ordenados pelo ruff. Imports absolutos sempre (`from julius.providers.base import Provider`, não `from .base`).
- **Linhas:** 100 chars.
- **Docstrings:** Google style. Obrigatórias em classes públicas e funções públicas.

## Type hints

- Tudo tipado. Sem `Any` salvo em fronteiras (parsing JSON cru). Comentar o porquê.
- Modelos de dado: pydantic v2 BaseModel.
- IO: `pathlib.Path`, nunca `str`.
- Datetimes: `datetime` com `tzinfo` (UTC ou America/Sao_Paulo conforme o caso). Sem naive.

## Async

- Tudo que toca rede ou processo de extração é `async`.
- IO de disco síncrono está OK (DuckDB é síncrono mesmo).
- Não misturar `requests` e `httpx`: apenas `httpx`.

## SQL

- **Sem f-string em SQL.** Sempre bindings:
  ```python
  conn.execute("INSERT INTO foo (a, b) VALUES (?, ?)", [a, b])
  ```
- Para nomes de tabela dinâmicos (necessário em alguns lugares): validar contra allowlist de regex (`^[a-z_][a-z0-9_]*$`) antes de interpolar.
- **Sem `ALTER TABLE ADD COLUMN` em produção.** Schema decidido na ingestão (tudo TEXT + `_extras` JSON) ou no pipeline `normalize`.

## Logging

- `structlog` em todo o código de produção. Zero `print()`.
- Eventos canônicos com nome em snake_case e dot-separated:
  - `extraction.started`
  - `extraction.job.success`
  - `extraction.job.failed`
  - `http.request`
  - etc.
- Sempre incluir contexto estruturado: `provider=`, `prefeitura=`, `endpoint=`, `year=`, `mes=`.

## Tratamento de erros

- Erros esperados (timeout, 404, JSON inválido, prefeitura offline) → logar e seguir. Marcar checkpoint como falhou para retry posterior.
- Erros inesperados → propagar, deixar o runner capturar e logar com traceback completo.
- Nunca `except Exception: pass`.
- Nunca derrubar a extração inteira por uma URL.

## Testes

- Framework: `pytest`. Async via `pytest-asyncio`.
- HTTP: `respx` para mock.
- Fixtures de respostas reais em `tests/fixtures/<provider>/<endpoint>.json`. Gerar capturando 1x da API real, depois nunca mais bater rede.
- Marker `smoke`: testes que tocam a rede de verdade. Não rodam em CI por padrão.
- Cobertura: alvo ≥ 80% nos módulos `providers/`, `extractor/`, `storage/`.

## Commits

Conventional Commits:

```
feat: add CR2 provider
fix(portaltp): handle utf-8-sig BOM in response
chore(deps): bump httpx to 0.27
refactor(extractor): extract checkpoint logic to separate module
test(tectrilha): add fixtures for contratos endpoint
docs(architecture): clarify checkpoint lifecycle
```

Escopo opcional mas recomendado. Mensagem em inglês.

## Branches

- `main` — estável. Só recebe merges de PRs.
- `refactor/v2` — branch principal da refatoração.
- `feat/...`, `fix/...` — branches de feature, partem de `refactor/v2` durante a refatoração.

## PRs

- Título no padrão Conventional Commits.
- Descrição obrigatória: o que muda, por quê, como testar.
- Atualizar `plan.md` se concluir uma fase.
- Atualizar `docs/` se mudar arquitetura, convenções ou catálogo de providers.
- Tests passando, lint limpo, mypy limpo.

## Arquivos no root

Apenas: `README.md`, `README-ptbr.md`, `CLAUDE.md`, `plan.md`, `pyproject.toml`, `.env.example`, `Dockerfile`, `docker-compose.yml`, `.gitignore`, `.github/`, `LICENSE`. Nada mais.

Documentação técnica → `docs/`. Dados → `data/`. Código → `julius/`. Testes → `tests/`. Código antigo → `legacy/`.

## Dependências novas

Antes de adicionar uma dependência:

1. Existe na stdlib? Se sim, usar stdlib.
2. Existe uma dep já presente que cobre? Se sim, usar.
3. Se realmente nova: criar ADR em `docs/decisions/` justificando.
