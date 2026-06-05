# CLAUDE.md — Guia para Agentes no Projeto Julius

> **Este arquivo é o ponto de entrada para qualquer agente (Claude Code, Cursor, Copilot, etc.) trabalhando neste repositório.**
> Leia este documento ANTES de qualquer ação. Atualize-o quando descobrir algo que outros agentes devem saber.

---

## 1. Em uma frase

Julius é um robô em Python que captura dados dos Portais da Transparência das 78 prefeituras do Estado do Espírito Santo (Brasil) via APIs públicas, normaliza e armazena para posterior análise de indicadores de improbidade administrativa.

## 2. Estado do projeto em uma frase

**Refatoração v2.0 — Fases 0–3 concluídas.** 4 providers implementados (PortalTP, Tectrilha, Agape, Alphatec), cobrindo 74/78 municípios (95%). Fase 4 (CR2/PortalFacil) adiada; Fase 5 (pipelines) é o próximo passo.

## 3. Ordem de leitura para um agente novo

1. **Este arquivo** (`CLAUDE.md`) — contexto, convenções, processo.
2. **[plan.md](plan.md)** — roadmap por fases com babysteps. Sempre identificar a fase atual antes de agir.
3. **[docs/architecture.md](docs/architecture.md)** — arquitetura técnica detalhada.
4. **[docs/conventions.md](docs/conventions.md)** — código, commits, idioma.
5. **[docs/providers.md](docs/providers.md)** — catálogo das APIs dos provedores.
6. **[docs/decisions/](docs/decisions/)** — ADRs (decisões arquiteturais com contexto e trade-offs).
7. **[README.md](README.md)** / **[README-ptbr.md](README-ptbr.md)** — visão pública do projeto.

## 4. Princípios agênticos deste projeto

### 4.1 Preservação de memória

Tudo que for **decisão**, **plano** ou **diretriz** vira documento `.md`. Conversa não é memória — arquivo é.

- **Decisões arquiteturais** (escolha de lib, padrão, trade-off) → criar/atualizar ADR em `docs/decisions/NNNN-titulo-kebab.md`.
- **Conhecimento de domínio** (como uma API se comporta, qual encoding usar, qual quirk de qual prefeitura) → registrar em `docs/providers.md` ou no YAML do endpoint.
- **Convenções de código** novas → `docs/conventions.md`.
- **Roadmap** → `plan.md`. Marcar checkboxes ao concluir fases.
- **Riscos descobertos** → seção "Riscos conhecidos" do `plan.md` ou ADR específica.

### 4.2 Babysteps explícitos

Cada fase em `plan.md` tem babysteps numerados. Execute-os em ordem. Não pule. Se um passo virar dois ou três, atualize o documento.

### 4.3 Parar ao fim de cada fase

**Nenhuma fase começa sem validação humana da anterior.** Ao concluir uma fase:

1. Atualizar a checkbox em `plan.md`.
2. Resumir o que foi feito (1-3 linhas) e quaisquer desvios do plano.
3. Pedir validação ao usuário antes de seguir.

### 4.4 Idempotência e reentrância

Outro agente pode pegar este projeto amanhã. O estado deve ser inferível dos arquivos, não da conversa. Em particular:

- Branch atual indica fase em andamento.
- Checkboxes em `plan.md` indicam o que está feito.
- Commits seguem Conventional Commits para serem auto-explicativos.
- ADRs ficam imutáveis após mergeadas (corrigir via nova ADR que substitui).

### 4.5 Documentação é parte da entrega

Um PR sem doc atualizada é PR incompleto. Toda mudança que afeta arquitetura, convenções ou uso da CLI exige atualização do `.md` correspondente no mesmo PR.

---

## 5. Convenções rápidas

(Detalhes em [docs/conventions.md](docs/conventions.md))

- **Código, comentários, docstrings:** inglês.
- **CLI, mensagens ao usuário, README-ptbr.md:** português brasileiro.
- **Commits:** Conventional Commits.
- **Branch principal da refatoração:** `refactor/v2`.
- **Testes:** `pytest`; todo provider novo tem fixtures + testes de `build_url` e `parse`.
- **Lint:** `ruff` + `mypy --strict`.
- **Sem `print()`** no código de produção — apenas `structlog`.
- **Sem f-string em SQL** — bindings ou DuckDB params.

## 6. Stack

| Camada | Tech |
|---|---|
| Empacotamento | `uv` + `pyproject.toml` |
| Linguagem | Python 3.12+ |
| Validação | `pydantic` v2 |
| HTTP | `httpx` async + `tenacity` |
| Storage | DuckDB |
| CLI | Typer |
| Config | `pydantic-settings` |
| Logging | `structlog` |
| Testes | `pytest`, `pytest-asyncio`, `respx` |

## 7. Layout esperado (após Fase 1)

```
julius/                 # pacote
├── cli.py
├── config.py
├── logging.py
├── models/
├── providers/
├── http/
├── extractor/
├── storage/
├── pipelines/
└── analysis/

data/                   # dados de configuração (CSV de prefeituras, YAML de endpoints)
tests/
docs/                   # documentação técnica e ADRs
legacy/                 # código v1 preservado
```

## 8. O que NÃO fazer

- **Não** criar arquivos `.md` no root para documentação técnica — usar `docs/`. O root só tem `README*.md`, `CLAUDE.md` e `plan.md`.
- **Não** começar uma fase nova sem confirmação do usuário.
- **Não** apagar `legacy/` até o usuário autorizar explicitamente (Fase 6).
- **Não** introduzir uma dependência nova sem registrar em ADR.
- **Não** mudar convenção de código sem atualizar `docs/conventions.md`.
- **Não** reescrever este `CLAUDE.md` por estilo — só por conteúdo. É lido por dezenas de agentes.

## 9. Quando em dúvida

- **Sobre arquitetura:** ler [docs/architecture.md](docs/architecture.md) e ADRs relevantes.
- **Sobre uma API específica:** ler [docs/providers.md](docs/providers.md).
- **Sobre se a tarefa pertence à fase atual:** ler [plan.md](plan.md). Se não pertence, parar e perguntar.
- **Sobre estilo de código:** ler [docs/conventions.md](docs/conventions.md).
- **Se nada esclarecer:** perguntar ao usuário (@pedropberger) antes de inventar.

---

**Última atualização:** 2026-06-05 (Fases 0–3 concluídas; Fase 4 adiada; Fase 5 backlog em `todo.md`).
