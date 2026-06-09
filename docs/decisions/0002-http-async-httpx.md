# ADR 0002 — HTTP: httpx async

- **Status:** Aceito
- **Data:** 2026-05-22
- **Decisor:** @pedropberger

## Contexto

A v1 usa `requests` síncrono com `sleep(1)` entre requests. Extração completa (78 prefeituras × ~40 endpoints × 12 meses) leva várias horas. Sem paralelismo real, sem rate limit por host.

## Opções consideradas

| Opção | Prós | Contras |
|---|---|---|
| `requests` + `ThreadPoolExecutor` | Não reescreve fluxo; simples | GIL atrapalha; menos eficiente que async em IO; harder per-host rate limit |
| `httpx` **async** | Mesma API de `requests`; suporta async nativo; HTTP/2; client com pool; integração com `respx` para testes | Exige `async/await` em todo extractor |
| `aiohttp` | Async maduro | API menos amigável que httpx; menos integração com pydantic |

## Decisão

**`httpx` async**, com:

- `AsyncClient` único por execução (pool de conexões).
- `asyncio.Semaphore(N)` global (default `JULIUS_HTTP_CONCURRENCY=20`).
- Rate limit por host (semaphore de tamanho 1 por host, com delay configurável `JULIUS_HTTP_PER_HOST_DELAY=1.0s`).
- Retry via `tenacity`: 3 tentativas, backoff exponencial 1s/2s/4s, retry em 5xx e timeouts.

## Consequências

- Extractor inteiro vira async.
- Testes usam `respx` (mock HTTP nativo para httpx).
- Ganho esperado: 5-10× mais rápido em extração full vs. v1.
- Trade-off: agentes precisam entender `asyncio`. Mitigado pela documentação no `docs/architecture.md`.
- Rate limit conservador por padrão para não derrubar portais frágeis (alguns municípios pequenos têm infra precária).
