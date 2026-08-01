"""
Etapa 5 do pipeline: silver -> gold.

A gold materializa as respostas às duas perguntas do case. É aqui - e não na
silver - que ficam as decisões de métrica: excluir estornos de uma média é uma
escolha analítica, não uma correção de qualidade. A silver preserva o dado; a
gold interpreta.

Todas as agregações são construídas sobre `pickup_year_month`, derivado da data
real da corrida, nunca do arquivo de origem. A análise exploratória mostrou que
cada competência recebe corridas de dois ou três arquivos distintos.

Uso:
    python -m src.transform.gold
"""

from __future__ import annotations

import argparse
import logging

from pyspark.sql import DataFrame, SparkSession, functions as F

from src import config
from src.utils.spark import ensure_namespaces, get_spark

logger = logging.getLogger(__name__)

TABLE_MONTHLY = config.table(config.SCHEMA_GOLD, "yellow_trips_monthly")
TABLE_HOURLY = config.table(config.SCHEMA_GOLD, "yellow_trips_hourly_passengers")


def monthly_revenue(spark: SparkSession) -> DataFrame:
    """Pergunta 1 — agregação mensal de `total_amount`.

    O enunciado ("média de valor total recebido em um mês considerando todos os
    yellow táxis da frota") admite duas leituras, com respostas separadas por
    seis ordens de grandeza:

      (a) ticket médio por corrida, agrupado por mês;
      (b) faturamento total da frota em cada mês.

    A tabela entrega as duas, e cada uma em duas versões: incluindo e excluindo
    os lançamentos não-positivos (estornos e ajustes contábeis). Escolher
    silenciosamente esconderia uma decisão que altera o resultado.
    """
    silver = spark.table(config.TABLE_SILVER)
    positivo = ~F.col("flag_valor_nao_positivo")

    return (
        silver.groupBy("pickup_year_month")
        .agg(
            F.count("*").alias("corridas"),
            F.sum(F.when(positivo, 1).otherwise(0)).alias("corridas_faturadas"),
            # --- Leitura (a): ticket médio por corrida ---------------------- #
            F.round(F.avg("total_amount"), 2).alias("ticket_medio_bruto"),
            F.round(F.avg(F.when(positivo, F.col("total_amount"))), 2).alias(
                "ticket_medio_sem_estornos"
            ),
            # --- Leitura (b): faturamento da frota no mês ------------------- #
            F.round(F.sum("total_amount"), 2).alias("faturamento_bruto"),
            F.round(F.sum(F.when(positivo, F.col("total_amount"))), 2).alias(
                "faturamento_sem_estornos"
            ),
            # --- Contexto -------------------------------------------------- #
            F.round(F.percentile_approx("total_amount", 0.5), 2).alias(
                "mediana_total_amount"
            ),
            F.sum(F.when(F.col("flag_valor_nao_positivo"), 1).otherwise(0)).alias(
                "estornos"
            ),
        )
        .orderBy("pickup_year_month")
    )


def hourly_passengers(spark: SparkSession, period: str = "2023-05") -> DataFrame:
    """Pergunta 2 — média de `passenger_count` por hora do dia.

    `AVG` ignora nulos nativamente, e esse é o comportamento correto: nulo
    significa que o taxímetro não registrou o passageiro, não que a corrida
    teve zero passageiros. Tratar como zero introduziria viés para baixo.

    As colunas alternativas tornam a decisão auditável em vez de implícita.
    """
    silver = spark.table(config.TABLE_SILVER)

    return (
        silver.filter(F.col("pickup_year_month") == period)
        .groupBy("pickup_hour")
        .agg(
            F.count("*").alias("corridas"),
            # Resposta adotada: AVG ignorando nulos.
            F.round(F.avg("passenger_count"), 4).alias("media_passageiros"),
            # Alternativas, para tornar a decisão explícita.
            F.round(F.avg(F.coalesce("passenger_count", F.lit(0))), 4).alias(
                "media_nulo_como_zero"
            ),
            F.round(
                F.avg(F.when(F.col("passenger_count") > 0, F.col("passenger_count"))), 4
            ).alias("media_apenas_positivos"),
            F.sum(F.when(F.col("flag_passageiros_ausente"), 1).otherwise(0)).alias(
                "sem_registro"
            ),
            F.sum("passenger_count").alias("total_passageiros"),
        )
        .orderBy("pickup_hour")
    )


def build_gold(spark: SparkSession) -> dict[str, int]:
    """Materializa as duas tabelas da gold."""
    ensure_namespaces(spark)

    monthly = monthly_revenue(spark)
    monthly.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(TABLE_MONTHLY)

    hourly = hourly_passengers(spark)
    hourly.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(TABLE_HOURLY)

    spark.sql(f"""
        COMMENT ON TABLE {TABLE_MONTHLY} IS
        'Gold - pergunta 1 do case. Agregacao mensal de total_amount nas duas
         leituras possiveis do enunciado (ticket medio e faturamento), com e sem
         lancamentos nao-positivos.'
    """)
    spark.sql(f"""
        COMMENT ON TABLE {TABLE_HOURLY} IS
        'Gold - pergunta 2 do case. Media de passageiros por hora do dia em
         maio/2023, com as alternativas de tratamento de nulo explicitadas.'
    """)

    counts = {
        "monthly": spark.table(TABLE_MONTHLY).count(),
        "hourly": spark.table(TABLE_HOURLY).count(),
    }
    logger.info("Gold: %s linhas mensais, %s linhas horarias",
                counts["monthly"], counts["hourly"])
    return counts


def main() -> int:
    parser = argparse.ArgumentParser(description="Silver -> Gold")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s | %(levelname)-7s | %(message)s",
    )

    spark = get_spark("ifood-case-gold")
    build_gold(spark)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
