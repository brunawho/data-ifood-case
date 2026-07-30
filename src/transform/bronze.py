"""
Etapa 2 do pipeline: landing -> bronze.

Contrato desta camada:
  * mesma granularidade da origem (1 linha = 1 corrida), nenhum registro
    descartado - inclusive os inválidos, que só serão tratados na silver;
  * nomes de coluna preservados como na origem;
  * tipos canonizados, para que os 5 meses formem uma tabela única;
  * colunas de auditoria (`_source_file`, `_ingested_at`, `_ref_period`).

Por que canonizar tipo aqui? Os parquets mensais da TLC não têm schema
estável: a mesma coluna aparece como int32 num mês e int64 noutro, e valores
monetários oscilam entre decimal e double. Se lêssemos os 5 arquivos de uma vez
o Spark falharia na união, e `mergeSchema` resolveria de forma imprevisível.

Estratégia adotada: ler **um arquivo por vez** (schema inferido do footer
daquele arquivo, sempre consistente consigo mesmo) e então fazer CAST explícito
para o tipo canônico. O cast acontece no plano do Spark, não no leitor de
Parquet, o que evita SchemaColumnConvertNotSupportedException.

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
    "tpep_pickup_datetime": "timestamp",
    "tpep_dropoff_datetime": "timestamp",
    # Nullable na origem (aparece como double justamente por isso). Mantemos
    # int nullable: o tratamento de nulo é decisão da silver, não da bronze.
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

    A TLC já trocou a caixa de colunas entre versões (`airport_fee` virou
    `Airport_fee` em arquivos de 2024). Resolver por caixa-insensitiva torna a
    ingestão resistente a isso sem precisar de mapa por competência.
    """
    lookup = {name.lower(): name for name in available}
    return lookup.get(wanted.lower())


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

    if missing:
        logger.warning("[%s] colunas ausentes na origem, preenchidas com NULL: %s",
                       period, ", ".join(missing))

    unexpected = set(available) - {
        _resolve(available, c) for c in CANONICAL_TYPES
    } - {None}
    if unexpected:
        # Não falha: a origem ganhar coluna nova não deve derrubar o pipeline.
        # Mas registra, porque é sinal de que o schema mudou e vale revisar.
        logger.warning("[%s] colunas novas na origem, ignoradas: %s",
                       period, ", ".join(sorted(unexpected)))

    year, month = period.split("-")
    return raw.select(*projection).select(
        "*",
        # `_metadata.file_path` em vez de input_file_name(): esta última não é
        # suportada em compute serverless / Photon.
        F.col("_metadata.file_path").alias("_source_file"),
        F.current_timestamp().alias("_ingested_at"),
        F.lit(period).alias(PARTITION_COLUMN),
        F.lit(int(year)).alias("_ref_year"),
        F.lit(int(month)).alias("_ref_month"),
    )


def write_bronze(df: DataFrame, period: str, spark: SparkSession) -> int:
    """
    Escreve um período na bronze de forma idempotente.

    `replaceWhere` substitui apenas a partição do período, deixando os outros
    meses intactos. É isso que permite reprocessar um mês isolado sem recarregar
    os cinco - e é a razão de a tabela ser particionada por `_ref_period`.
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
    logger.info("[%s] bronze gravada: %d registros", period, f"{count:,}".replace(",", "."))
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
