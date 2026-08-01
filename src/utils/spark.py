"""
Obtenção da SparkSession.

No Databricks a sessão já existe e é gerenciada pelo runtime; localmente
precisa ser construída com as extensões do Delta. Os módulos de transformação
apenas chamam `get_spark()` e não sabem onde estão rodando.
"""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession

from src import config

logger = logging.getLogger(__name__)


def get_spark(app_name: str = "ifood-case") -> SparkSession:
    """Devolve a SparkSession ativa (Databricks) ou cria uma local com Delta."""
    if config.IS_DATABRICKS:
        spark = SparkSession.getActiveSession()
        if spark is None:  # fallback para databricks-connect
            spark = SparkSession.builder.getOrCreate()
        return spark

    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", f"{config.LOCAL_ROOT}/warehouse")
        # A base tem datas anteriores a 1900; sem isto o Spark 3.x lança exceção
        # ao lê-las (rebase de calendário juliano/gregoriano no Parquet).
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
    )
    # Atenção: esta função configura `spark.jars.packages` com as coordenadas
    # Maven do Delta compatíveis com a versão instalada via pip. Os JARs são
    # baixados do Maven Central na primeira execução, então a inicialização
    # exige rede. Atrás de proxy, é preciso pré-popular o cache Ivy ou apontar
    # `spark.jars` para os artefatos locais.
    return configure_spark_with_delta_pip(builder).getOrCreate()


def ensure_namespaces(spark: SparkSession) -> None:
    """Cria catálogo/schemas/volume se não existirem (idempotente)."""
    if config.IS_DATABRICKS:
        spark.sql(f"USE CATALOG {config.CATALOG}")
        for schema in (
            config.SCHEMA_RAW,
            config.SCHEMA_BRONZE,
            config.SCHEMA_SILVER,
            config.SCHEMA_GOLD,
        ):
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.CATALOG}.{schema}")
        spark.sql(
            f"CREATE VOLUME IF NOT EXISTS "
            f"{config.CATALOG}.{config.SCHEMA_RAW}.{config.VOLUME_LANDING}"
        )
    else:
        for schema in (config.SCHEMA_BRONZE, config.SCHEMA_SILVER, config.SCHEMA_GOLD):
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema}")
    logger.info("Namespaces garantidos no catálogo %s", config.CATALOG)
