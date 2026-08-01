"""
Etapa 1: origem (TLC) -> landing zone.

O arquivo é copiado bit a bit, sem parse, cast ou filtro. Isso permite
reprocessar tudo sem depender da origem estar no ar, e torna auditável contra o
original qualquer decisão de limpeza feita adiante.

Uso:
    python -m src.ingestion.landing                    # todos os períodos
    python -m src.ingestion.landing --periods 2023-01 2023-02
    python -m src.ingestion.landing --force            # ignora idempotência
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import time
from datetime import datetime, timezone

import requests

from src import config

logger = logging.getLogger(__name__)

CHUNK_SIZE = 1024 * 1024  # 1 MiB
MAX_RETRIES = 4
BACKOFF_BASE_SECONDS = 2
REQUEST_TIMEOUT = (10, 120)  # (connect, read)


def _remote_size(url: str) -> int | None:
    """Tamanho anunciado pela origem, via HEAD. None se a origem não informar."""
    try:
        response = requests.head(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        response.raise_for_status()
        size = response.headers.get("Content-Length")
        return int(size) if size else None
    except requests.RequestException as exc:
        logger.warning("HEAD falhou para %s (%s); seguindo sem checagem", url, exc)
        return None


def _local_size(path: str) -> int | None:
    try:
        return os.path.getsize(path)
    except OSError:
        return None


def _sha256(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(CHUNK_SIZE), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _download(url: str, destination: str) -> None:
    """Download em streaming, com retry e backoff exponencial.

    Escreve em arquivo temporário e só renomeia no fim: se a conexão cair no
    meio, a landing não fica com um parquet truncado que passaria pela checagem
    de idempotência na execução seguinte.
    """
    temporary = f"{destination}.part"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with requests.get(url, stream=True, timeout=REQUEST_TIMEOUT) as response:
                response.raise_for_status()
                with open(temporary, "wb") as handle:
                    for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                        if chunk:
                            handle.write(chunk)
            os.replace(temporary, destination)
            return
        except (requests.RequestException, OSError) as exc:
            if os.path.exists(temporary):
                os.remove(temporary)
            if attempt == MAX_RETRIES:
                raise
            wait = BACKOFF_BASE_SECONDS ** attempt
            logger.warning(
                "Tentativa %d/%d falhou para %s (%s). Nova tentativa em %ds",
                attempt, MAX_RETRIES, url, exc, wait,
            )
            time.sleep(wait)


def _append_manifest(record: dict) -> None:
    """Acrescenta um registro ao manifest.

    Reescreve o arquivo inteiro em vez de abrir em modo append: Volumes do
    Unity Catalog não suportam escrita incremental de forma confiável.
    """
    path = config.manifest_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)

    existing = ""
    try:
        with open(path, "r", encoding="utf-8") as handle:
            existing = handle.read()
    except FileNotFoundError:
        pass

    with open(path, "w", encoding="utf-8") as handle:
        handle.write(existing)
        handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def ingest_period(period: str, force: bool = False) -> dict:
    """Garante o parquet de `period` na landing. Retorna o registro de manifest."""
    url = config.source_url(period)
    destination = config.landing_file(period)
    os.makedirs(config.landing_dir(period), exist_ok=True)

    remote_size = _remote_size(url)
    local_size = _local_size(destination)

    # Divergência de tamanho indica republicação do mês pela TLC ou download
    # incompleto; nos dois casos baixar de novo é o correto. Se o HEAD falhou e
    # não há tamanho de referência, confia-se no arquivo local (`--force`
    # existe para o caso duvidoso).
    if not force and local_size is not None:
        if remote_size is None or local_size == remote_size:
            logger.info("[%s] já presente e íntegro (%d bytes); pulando", period, local_size)
            return {
                "period": period,
                "status": "skipped",
                "bytes": local_size,
                "source_url": url,
                "landing_path": destination,
                "ingested_at": datetime.now(timezone.utc).isoformat(),
            }
        logger.info(
            "[%s] tamanho divergente (local=%s, origem=%s); re-baixando",
            period, local_size, remote_size,
        )

    logger.info("[%s] baixando %s", period, url)
    started = time.monotonic()
    _download(url, destination)
    elapsed = time.monotonic() - started

    size = _local_size(destination)
    checksum = _sha256(destination)
    logger.info("[%s] ok: %d bytes em %.1fs (sha256=%s...)", period, size, elapsed, checksum[:12])

    record = {
        "period": period,
        "status": "downloaded",
        "bytes": size,
        "sha256": checksum,
        "source_url": url,
        "landing_path": destination,
        "duration_seconds": round(elapsed, 2),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }
    _append_manifest(record)
    return record


def ingest(periods: tuple[str, ...] = config.PERIODS, force: bool = False) -> list[dict]:
    """Ingere todos os períodos.

    Falha em um período não aborta os demais. Como a ingestão é idempotente,
    reexecutar resolve apenas o que faltou.
    """
    results: list[dict] = []
    for period in periods:
        try:
            results.append(ingest_period(period, force=force))
        except Exception as exc:  # noqa: BLE001 - resiliência por período
            logger.error("[%s] falhou: %s", period, exc)
            results.append({"period": period, "status": "failed", "error": str(exc)})
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description="Ingestão TLC -> landing zone")
    parser.add_argument("--periods", nargs="+", default=list(config.PERIODS),
                        help="Períodos no formato YYYY-MM")
    parser.add_argument("--force", action="store_true",
                        help="Re-baixa mesmo que o arquivo já exista")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s | %(levelname)-7s | %(message)s",
    )

    logger.info("Landing zone: %s", config.landing_root())
    results = ingest(tuple(args.periods), force=args.force)

    failed = [r for r in results if r["status"] == "failed"]
    total_bytes = sum(r.get("bytes") or 0 for r in results)
    logger.info(
        "Resumo: %d baixado(s), %d pulado(s), %d falha(s) | %.1f MiB na landing",
        sum(1 for r in results if r["status"] == "downloaded"),
        sum(1 for r in results if r["status"] == "skipped"),
        len(failed),
        total_bytes / 1024 / 1024,
    )
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
