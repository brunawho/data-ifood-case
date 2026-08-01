"""
Etapa 4: bronze -> silver, a camada de consumo.

Descarta apenas o comprovadamente inválido - corrida fora do escopo temporal e
corrida com duração não-positiva, 6.284 registros ou 0,039% da base. O que é
suspeito mas não inválido é preservado e sinalizado em colunas `flag_*`, e o
descartado vai para uma tabela de quarentena com o motivo.

Sinalizar em vez de filtrar porque a silver precisa servir perguntas ainda não
formuladas: remover os estornos aqui decidiria, por todos os futuros
consumidores, que ninguém vai querer analisá-los. Decisões de métrica pertencem
à gold. Os volumes de cada anomalia estão em `docs/achados-eda.md`.

Uso:
    python -m src.transform.silver
"""

from __future__ import annotations

import argparse
import logging

from pyspark.sql import Column, DataFrame, SparkSession, functions as F

from src import config
from src.utils.spark import ensure_namespaces, get_spark

logger = logging.getLogger(__name__)

PARTITION_COLUMN = "pickup_year_month"

# Fornecedores documentados no dicionário de dados da TLC para 2023.
KNOWN_VENDORS = (1, 2)

# 4 passageiros em sedan, 5 em minivan autorizada, mais criança de colo. Acima
# de 6 não corresponde a nenhuma configuração legal em NY.
MAX_PLAUSIBLE_PASSENGERS = 6

# Implausível (taxímetro esquecido ligado), mas não impossível: sinaliza.
EXTREME_DURATION_SECONDS = 24 * 60 * 60


def _duration_seconds() -> Column:
    return F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp(
        "tpep_pickup_datetime"
    )


def _scope_start() -> str:
    return min(config.PERIODS)


def _scope_end() -> str:
    return max(config.PERIODS)


def classify(df: DataFrame) -> DataFrame:
    """Adiciona colunas derivadas, flags de qualidade e o motivo de descarte.

    Apenas anota, sem filtrar. A separação acontece depois, para que o mesmo
    cálculo alimente silver e quarentena sem risco de divergirem.
    """
    competencia = F.date_format("tpep_pickup_datetime", "yyyy-MM")
    duracao = _duration_seconds()

    fora_do_escopo = ~competencia.between(_scope_start(), _scope_end())
    duracao_invalida = duracao <= 0

    return (
        df.withColumn("trip_duration_seconds", duracao)
        .withColumn(PARTITION_COLUMN, competencia)
        .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
        .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
        .withColumn("pickup_day_of_week", F.dayofweek("tpep_pickup_datetime"))
        # Sinalizações: suspeito, porém preservado.
        .withColumn("flag_valor_nao_positivo", F.col("total_amount") <= 0)
        .withColumn(
            "flag_duracao_extrema", F.col("trip_duration_seconds") > EXTREME_DURATION_SECONDS
        )
        .withColumn("flag_passageiros_ausente", F.col("passenger_count").isNull())
        .withColumn("flag_passageiros_zero", F.col("passenger_count") == 0)
        .withColumn(
            "flag_passageiros_implausivel",
            F.col("passenger_count") > MAX_PLAUSIBLE_PASSENGERS,
        )
        .withColumn(
            "flag_fornecedor_desconhecido", ~F.col("VendorID").isin(*KNOWN_VENDORS)
        )
        # Motivo do descarte; NULL significa registro válido.
        .withColumn(
            "motivo_descarte",
            F.when(fora_do_escopo, F.lit("fora_do_escopo_temporal"))
            .when(duracao_invalida, F.lit("duracao_nao_positiva")),
        )
    )


def build_silver(spark: SparkSession) -> dict[str, int]:
    """Reconstrói silver e quarentena a partir da bronze completa.

    A reconstrução total é intencional. A silver é particionada pela data real
    da corrida e a bronze pelo arquivo de origem, e a EDA mostrou que essas
    chaves não se correspondem: uma partição da bronze alimenta várias da
    silver. Recarregar um mês isolado com garantia de completude exigiria
    varrer toda a bronze de qualquer forma, então o "incremental" seria uma
    reconstrução total com passos extras e risco de inconsistência.

    Em volume maior a alternativa seria MERGE por chave de negócio, que esta
    base não possui.
    """
    ensure_namespaces(spark)

    # Sem .cache(): serverless não suporta persistência de DataFrame. As
    # contagens saem das tabelas gravadas, onde COUNT(*) é resolvido pelas
    # estatísticas do log de transação.
    classificado = classify(spark.table(config.TABLE_BRONZE))

    validos = classificado.filter(F.col("motivo_descarte").isNull()).drop(
        "motivo_descarte"
    )
    descartados = classificado.filter(F.col("motivo_descarte").isNotNull())

    (
        validos.withColumn("_processed_at", F.current_timestamp())
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy(PARTITION_COLUMN)
        .saveAsTable(config.TABLE_SILVER)
    )

    (
        descartados.withColumn("_processed_at", F.current_timestamp())
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(config.TABLE_SILVER_QUARANTINE)
    )

    total_validos = spark.table(config.TABLE_SILVER).count()
    total_descartados = spark.table(config.TABLE_SILVER_QUARANTINE).count()

    spark.sql(f"""
        COMMENT ON TABLE {config.TABLE_SILVER} IS
        'Silver - camada de consumo. Corridas de yellow taxi jan-mai/2023, uma
         linha por corrida. Descarta apenas registros comprovadamente invalidos;
         anomalias preservadas e sinalizadas em colunas flag_*. Ver
         docs/achados-eda.md.'
    """)
    spark.sql(f"""
        COMMENT ON TABLE {config.TABLE_SILVER_QUARANTINE} IS
        'Quarentena - registros descartados na silver, com o motivo em
         motivo_descarte. Existe para que nenhum dado seja perdido silenciosamente.'
    """)

    logger.info(
        "Silver: %s validos | quarentena: %s descartados",
        f"{total_validos:,}".replace(",", "."),
        f"{total_descartados:,}".replace(",", "."),
    )
    return {"validos": total_validos, "descartados": total_descartados}


def main() -> int:
    parser = argparse.ArgumentParser(description="Bronze -> Silver")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s | %(levelname)-7s | %(message)s",
    )

    spark = get_spark("ifood-case-silver")
    build_silver(spark)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
