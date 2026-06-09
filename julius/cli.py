"""Julius CLI — ponto de entrada principal."""

import asyncio
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

    from julius.config import settings
    from julius.extractor.checkpoint import Checkpoint
    from julius.extractor.runner import Runner
    from julius.extractor.scheduler import iter_jobs
    from julius.logging import setup_logging
    from julius.providers.registry import get_provider, list_providers
    from julius.storage.duckdb_store import DuckDBStore
    from julius.storage.prefeituras import load_prefeituras

    setup_logging(settings.log_dir, settings.log_level)

    provider_names = list_providers() if all_providers else [provider]

    for prov_name in provider_names:
        assert prov_name is not None
        try:
            prov = get_provider(prov_name)
        except KeyError as exc:
            typer.echo(f"Erro: {exc}", err=True)
            raise typer.Exit(1) from exc

        prefeituras = load_prefeituras(provider=prov_name, slug=only)
        if not prefeituras:
            typer.echo(
                f"Nenhuma prefeitura encontrada para provider='{prov_name}'"
                + (f", município='{only}'" if only else "")
                + "."
            )
            continue

        jobs = list(iter_jobs(prov, prefeituras, start=start, end=end))

        if dry_run:
            typer.echo(f"\n[{prov_name}] {len(jobs)} jobs (dry-run — sem extração):")
            for job in jobs[:20]:
                typer.echo(f"  {job.key}")
            if len(jobs) > 20:
                typer.echo(f"  ... e mais {len(jobs) - 20}")
            continue

        typer.echo(
            f"\n[{prov_name}] Iniciando extração: {len(prefeituras)} prefeituras, "
            f"{len(prov.list_endpoints())} endpoints, período {from_date}→{to_date}"
        )

        store = DuckDBStore(settings.db_path)
        checkpoint = Checkpoint(store.conn)
        runner = Runner(store, checkpoint)

        asyncio.run(runner.run(prov, iter(jobs)))

        summary = checkpoint.summary()
        typer.echo(f"\n[{prov_name}] Concluído: {summary}")
        store.close()


@app.command(name="retry-failed")
def retry_failed(
    provider: Annotated[
        str | None,
        typer.Option("--provider", "-p", help="Filtrar por provedor."),
    ] = None,
) -> None:
    """Retenta extrações que falharam anteriormente."""
    from julius.config import settings
    from julius.extractor.checkpoint import Checkpoint
    from julius.logging import setup_logging
    from julius.storage.duckdb_store import DuckDBStore

    setup_logging(settings.log_dir, settings.log_level)
    store = DuckDBStore(settings.db_path)
    checkpoint = Checkpoint(store.conn)
    failed = checkpoint.failed_jobs(provider=provider)

    if not failed:
        typer.echo("Nenhuma extração com falha encontrada.")
        store.close()
        return

    typer.echo(f"{len(failed)} extrações com falha para provider='{provider or 'todos'}'.")
    typer.echo("Re-execução de falhas será implementada na próxima fase.")
    store.close()


@app.command()
def status() -> None:
    """Exibe resumo da última extração (sucesso, falhas, cobertura)."""
    from julius.config import settings
    from julius.extractor.checkpoint import Checkpoint
    from julius.storage.duckdb_store import DuckDBStore

    if not settings.db_path.exists():
        typer.echo("Nenhuma extração realizada ainda. Execute 'julius extract' primeiro.")
        return

    store = DuckDBStore(settings.db_path)
    checkpoint = Checkpoint(store.conn)
    summary = checkpoint.summary()
    store.close()

    if not summary:
        typer.echo("Nenhum registro de extração encontrado.")
        return

    typer.echo("\nResumo da extração:")
    for status_name, count in sorted(summary.items()):
        typer.echo(f"  {status_name:12s}: {count:>6,}")


if __name__ == "__main__":
    app()
