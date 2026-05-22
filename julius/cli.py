"""Julius CLI — ponto de entrada principal."""

from datetime import date
from typing import Annotated

import typer

from julius import __version__

app = typer.Typer(
    name="julius",
    help="Julius — captura e análise de dados de Portais da Transparência municipais.",
    add_completion=False,
    no_args_is_help=True,
)


def _parse_date(value: str) -> date:
    """Parse MM/AAAA into a date (day=1)."""
    try:
        parts = value.strip().split("/")
        if len(parts) != 2:
            raise ValueError
        month, year = int(parts[0]), int(parts[1])
        return date(year, month, 1)
    except (ValueError, IndexError) as exc:
        raise typer.BadParameter(
            f"Formato inválido: '{value}'. Use MM/AAAA (ex: 01/2024)."
        ) from exc


@app.command()
def version() -> None:
    """Exibe a versão do Julius."""
    typer.echo(f"julius {__version__}")


@app.command()
def extract(
    from_date: Annotated[
        str,
        typer.Option("--from", help="Data inicial no formato MM/AAAA (ex: 01/2024)."),
    ],
    to_date: Annotated[
        str,
        typer.Option("--to", help="Data final no formato MM/AAAA (ex: 12/2024)."),
    ],
    provider: Annotated[
        str | None,
        typer.Option("--provider", "-p", help="Nome do provedor (ex: portaltp, tectrilha)."),
    ] = None,
    all_providers: Annotated[
        bool,
        typer.Option("--all", help="Extrair de todos os provedores."),
    ] = False,
    only: Annotated[
        str | None,
        typer.Option(help="Filtrar por slug de município (ex: afonso-claudio)."),
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", help="Mostra os jobs sem executar a extração."),
    ] = False,
) -> None:
    """Extrai dados dos Portais da Transparência para o período informado."""
    if not provider and not all_providers:
        typer.echo("Erro: informe --provider <nome> ou --all.", err=True)
        raise typer.Exit(1)

    start = _parse_date(from_date)
    end = _parse_date(to_date)

    if start > end:
        typer.echo("Erro: data inicial deve ser anterior à data final.", err=True)
        raise typer.Exit(1)

    providers = [provider] if provider else ["all"]
    typer.echo(
        f"[julius] extração: provider={providers}, período={from_date}→{to_date}"
        + (f", município={only}" if only else "")
        + (" [dry-run]" if dry_run else "")
    )
    typer.echo("Implementação completa disponível na Fase 2.")


@app.command(name="retry-failed")
def retry_failed(
    provider: Annotated[
        str | None,
        typer.Option("--provider", "-p", help="Filtrar por provedor."),
    ] = None,
) -> None:
    """Retenta extrações que falharam anteriormente."""
    typer.echo(f"[julius] retry-failed: provider={provider or 'todos'}")
    typer.echo("Implementação completa disponível na Fase 2.")


@app.command()
def status() -> None:
    """Exibe resumo da última extração (sucesso, falhas, cobertura)."""
    typer.echo("[julius] status: Implementação completa disponível na Fase 2.")


if __name__ == "__main__":
    app()
