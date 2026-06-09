# ADR 0005 — Idioma: código em inglês, CLI em PT-BR

- **Status:** Aceito
- **Data:** 2026-05-22
- **Decisor:** @pedropberger

## Contexto

A v1 mistura inglês e português em variáveis, comentários, mensagens e nomes de função (`run_extraction`, `processar_prefeitura`, `data_inicio`, "Rodar código para um período específico"). O README declara explicitamente:

> "o Readme e toda documentação (até os comentários dos códigos) estarem em inglês como lingua padrão, aprender inglês sempre é bom."

Mas o usuário final do CLI é brasileiro (servidores públicos, ONGs, jornalistas) e a UX em inglês cria barreira.

## Decisão

**Código (identificadores, comentários, docstrings) em inglês. Mensagens da CLI ao usuário em PT-BR.**

Detalhes em [docs/conventions.md](../conventions.md#idioma).

## Consequências

- Código é didático (aprendizado de inglês, contribuição internacional).
- UX da CLI é acessível ao público-alvo.
- Logs estruturados têm chaves em EN (`event=extraction.started`) mas mensagens livres podem ser PT-BR.
- Documentação dupla: `README.md` em EN (público técnico internacional), `README-ptbr.md` em PT-BR (público brasileiro). `CLAUDE.md`, `plan.md` e `docs/` em PT-BR (audiência é agentes + Pedro).
- Trade-off: agentes precisam aplicar a regra consistentemente. PRs que misturem idiomas no lugar errado devem ser corrigidos.
