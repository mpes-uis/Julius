# ADR 0004 — Rollout faseado da refatoração

- **Status:** Aceito
- **Data:** 2026-05-22
- **Decisor:** @pedropberger

## Contexto

A refatoração é grande (reescrita completa). Risco real de:
- Investir tempo numa arquitetura que se mostra inadequada só ao espalhar nos 6 providers.
- Perder cobertura de dados durante a transição.
- Bloquear o uso do sistema por semanas.

## Opções consideradas

| Opção | Prós | Contras |
|---|---|---|
| Big bang (tudo de uma vez) | Resultado final coeso | Risco alto; sem validação intermediária; longa janela sem feedback |
| **Faseado por provider** | Valida arquitetura cedo (Fase 2); legado coexiste; cada fase entrega valor | Mais commits, mais pontos de parada |
| Só providers, depois resto | Cobre prefeituras rápido | Pula pipelines/análise; pode acumular dívida |

## Decisão

**Rollout faseado em 7 fases** detalhadas em [plan.md](../../plan.md):

0. Limpeza prévia
1. Fundação
2. PortalTP end-to-end (provider de referência)
3. Tectrilha, Agape, Alphatec
4. CR2, PortalFacil (novos)
5. Pipelines + migração SQLite
6. Docker + docs
7. Análise (futuro)

**Regra explícita:** parar e validar com o usuário ao fim de cada fase. Não emendar.

## Consequências

- Cada fase tem critério de aceitação documentado.
- Legado (`legacy/`) coexiste até a Fase 6.
- Agentes futuros podem retomar exatamente de onde parou — basta ler `plan.md` e checar branches/commits.
- Trade-off: mais cerimônia (commits, validações). Ganho: confiança e reversibilidade.
