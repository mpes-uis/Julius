# ADR 0003 — CLI: Typer

- **Status:** Aceito
- **Data:** 2026-05-22
- **Decisor:** @pedropberger

## Contexto

A v1 usa menus interativos (`input()` em loop) em cada um dos 3 scripts. Inutilizável em automação (cron, Airflow, Docker), inconsistente entre scripts, sem help.

## Opções consideradas

| Opção | Prós | Contras |
|---|---|---|
| Menu interativo (manter) | Amigável para usuário final manual | Inviável para automação; sem help; sem composição |
| `argparse` (stdlib) | Sem dep nova; padrão Python | Verboso; sem geração automática de help rico |
| `click` | Maduro; usado em larga escala | API menos type-friendly; mais boilerplate |
| **`Typer`** | Type hints viram CLI; rich help; testável; baseado em click | Dep nova; opiniões fortes |

## Decisão

**`Typer`**. Comandos da CLI são funções tipadas, parâmetros viram opções automaticamente.

Mensagens em PT-BR (ver [ADR 0005](0005-idioma-codigo-cli.md)).

Comandos planejados em [docs/architecture.md](../architecture.md#2.1).

## Consequências

- `julius --help` funciona; subcomandos auto-documentados.
- Compatível com Airflow / cron / Docker / GitHub Actions.
- Testes de CLI via `typer.testing.CliRunner`.
- Sem prompts interativos no fluxo principal — qualquer dado de entrada é flag ou env var.
- Trade-off: usuários acostumados ao menu antigo precisam aprender flags. Mitigação: README com exemplos completos.
