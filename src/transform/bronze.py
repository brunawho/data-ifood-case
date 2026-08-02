"""
Etapa 2: landing -> bronze.

Preserva a granularidade e os nomes da origem, sem descartar registro algum, e
acrescenta colunas de auditoria. Os tipos são canonizados para que os cinco
meses formem uma tabela única.

Os parquets mensais da TLC não têm schema estável (a mesma coluna aparece como
int32 num mês e int64 noutro). Por isso a leitura é feita um arquivo por vez,
com CAST posterior: assim a conversão ocorre no plano do Spark e não no leitor
de Parquet, evitando SchemaColumnConvertNotSupportedException.

Uso:
    python -m src.transform.bronze
    python -m src.transform.bronze --periods 2023-01
"""

from __future__ import annotations

import argparse
import logging

from pyspark.sql import DataFrame, SparkSession, functions as F

from src import config
from src.utils.spark import ensure_namespaces, get_spark

logger = logging.getLogger(__name__)

# Tipo canônico de cada coluna da origem. A ordem define a ordem na tabela.
# Colunas ausentes num mês específico entram como NULL, sem quebrar a execução.
CANONICAL_TYPES: dict[str, str] = {
    "VendorID": "int",
    # NTZ preservado: a TLC registra hora local de NY, sem offset. Converter
    # para TIMESTAMP faria o Spark interpretar o horário de parede no fuso da
    # sessão, e a pergunta 2 do case depende justamente da hora do dia.
    "tpep_pickup_datetime": "timestamp_ntz",
    "tpep_dropoff_datetime": "timestamp_ntz",
    # Nullable na origem; o tratamento do nulo é decisão da silver.
    "passenger_count": "int",
    "trip_distance": "double",
    "RatecodeID": "int",
    "store_and_fwd_flag": "string",
    "PULocationID": "int",
    "DOLocationID": "int",
    "payment_type": "int",
    "fare_amount": "double",
    "extra": "double",
    "mta_tax": "double",
    "tip_amount": "double",
    "tolls_amount": "double",
    "improvement_surcharge": "double",
    "total_amount": "double",
    "congestion_surcharge": "double",
    "airport_fee": "double",
}

PARTITION_COLUMN = "_ref_period"


def _resolve(available: list[str], wanted: str) -> str | None:
    """Casa nome de coluna ignorando caixa.

    A TLC já trocou a caixa entre versões (`airport_fee` virou `Airport_fee` em
    2024), então a resolução caixa-insensitiva evita um mapa por competência.
    """
    lookup = {name.lower(): name for name in available}
    return lookup.get(wanted.lower())


def check_required_columns(available: list[str], period: str) -> None:
    """Falha se faltar alguma coluna exigida pelo case na camada de consumo.

    Preencher uma dessas cinco com NULL produziria uma camada de consumo
    silenciosamente inutilizável. Colunas opcionais podem faltar: viram NULL
    com aviso.

    Raises:
        ValueError: se qualquer coluna de `config.REQUIRED_COLUMNS` estiver
            ausente, considerando o casamento caixa-insensitivo.
    """
    ausentes = sorted(
        coluna for coluna in config.REQUIRED_COLUMNS if _resolve(available, coluna) is None
    )
    if ausentes:
        raise ValueError(
            f"[{period}] colunas obrigatórias ausentes na origem: {ausentes}. "
            f"Verifique se o schema da TLC mudou."
        )


def read_landing(spark: SparkSession, period: str) -> DataFrame:
    """Lê o parquet original de um período e devolve com tipos canônicos."""
    path = config.landing_file(period)
    raw = spark.read.parquet(path)
    available = raw.columns

    projection = []
    missing = []
    for column, target_type in CANONICAL_TYPES.items():
        source = _resolve(available, column)
        if source is None:
            missing.append(column)
            projection.append(F.lit(None).cast(target_type).alias(column))
        else:
            projection.append(F.col(f"`{source}`").cast(target_type).alias(column))

    check_required_columns(available, period)

    if missing:
        logger.warning("[%s] colunas opcionais ausentes, preenchidas com NULL: %s",
                       period, ", ".join(missing))

    unexpected = set(available) - {
        _resolve(available, c) for c in CANONICAL_TYPES
    } - {None}
    if unexpected:
        # Coluna nova na origem não derruba o pipeline, mas fica registrada.
        logger.warning("[%s] colunas novas na origem, ignoradas: %s",
                       period, ", ".join(sorted(unexpected)))

    year, month = period.split("-")
    # Projeção única: `_metadata` é coluna oculta da relação de arquivo. O
    # analisador do Spark consegue resolvê-la através de um Project (regra
    # AddMetadataColumns), mas selecioná-la junto das demais dispensa essa
    # dependência e deixa a intenção explícita.
    # input_file_name() não é usada por não ser suportada em serverless/Photon.
    return raw.select(
        *projection,
        F.col("_metadata.file_path").alias("_source_file"),
        F.current_timestamp().alias("_ingested_at"),
        F.lit(period).alias(PARTITION_COLUMN),
        F.lit(int(year)).alias("_ref_year"),
        F.lit(int(month)).alias("_ref_month"),
    )


def write_bronze(df: DataFrame, period: str, spark: SparkSession) -> int:
    """Escreve um período na bronze de forma idempotente.

    `replaceWhere` substitui apenas a partição do período, o que permite
    reprocessar um mês isolado sem recarregar os cinco.
    """
    table = config.TABLE_BRONZE
    writer = df.write.format("delta")

    if spark.catalog.tableExists(table):
        writer = writer.mode("overwrite").option(
            "replaceWhere", f"{PARTITION_COLUMN} = '{period}'"
        )
    else:
        writer = writer.mode("overwrite").partitionBy(PARTITION_COLUMN)

    writer.saveAsTable(table)
    count = df.count()
    logger.info("[%s] bronze gravada: %s registros", period, f"{count:,}".replace(",", "."))
    return count


def build_bronze(
    spark: SparkSession, periods: tuple[str, ...] = config.PERIODS
) -> dict[str, int]:
    ensure_namespaces(spark)
    counts: dict[str, int] = {}
    for period in periods:
        df = read_landing(spark, period)
        counts[period] = write_bronze(df, period, spark)

    spark.sql(f"""
        COMMENT ON TABLE {config.TABLE_BRONZE} IS
        'Bronze - corridas de yellow taxi da NYC TLC, schema da origem com tipos
         canonizados e colunas de auditoria. Nenhum registro filtrado.'
    """)
    return counts


def main() -> int:
    parser = argparse.ArgumentParser(description="Landing -> Bronze")
    parser.add_argument("--periods", nargs="+", default=list(config.PERIODS))
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s | %(levelname)-7s | %(message)s",
    )

    spark = get_spark("ifood-case-bronze")
    counts = build_bronze(spark, tuple(args.periods))
    total = sum(counts.values())
    logger.info("Total na bronze: %s registros em %d período(s)",
                f"{total:,}".replace(",", "."), len(counts))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
