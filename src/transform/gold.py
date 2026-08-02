"""
Etapa 5: silver -> gold.

Materializa as respostas às duas perguntas do case. As decisões de métrica
ficam aqui, e não na silver: excluir registros com `total_amount <= 0` de uma
média é escolha analítica, não correção de qualidade.

As agregações usam `pickup_year_month`, derivado da data real da corrida. Cada
competência recebe corridas de dois ou três arquivos distintos, então agrupar
pelo arquivo de origem daria resposta errada.

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
    """Pergunta 1 - agregação mensal de `total_amount`.

    O enunciado admite duas leituras separadas por seis ordens de grandeza:
    (a) ticket médio por corrida e (b) faturamento total da frota no mês. A
    tabela entrega as duas, cada uma com e sem os lançamentos não-positivos.

    A resposta oficial é a que inclui todos os registros; a versão filtrada é
    análise de sensibilidade, e depende da hipótese de que os valores
    não-positivos sejam reversões, que os dados não comprovam.
    """
    silver = spark.table(config.TABLE_SILVER)
    positivo = ~F.col("flag_valor_nao_positivo")

    return (
        silver.groupBy("pickup_year_month")
        .agg(
            F.count("*").alias("corridas"),
            F.sum(F.when(positivo, 1).otherwise(0)).alias("corridas_faturadas"),
            # Leitura (a): ticket médio por corrida.
            F.avg("total_amount").alias("ticket_medio_bruto"),
            F.avg(F.when(positivo, F.col("total_amount"))).alias(
                "ticket_medio_sem_nao_positivos"
            ),
            # Leitura (b): faturamento da frota no mês.
            F.sum("total_amount").alias("faturamento_bruto"),
            F.sum(F.when(positivo, F.col("total_amount"))).alias(
                "faturamento_sem_nao_positivos"
            ),
            F.percentile_approx("total_amount", 0.5).alias("mediana_total_amount"),
            F.sum(F.when(F.col("flag_valor_nao_positivo"), 1).otherwise(0)).alias(
                "lancamentos_nao_positivos"
            ),
        )
        .orderBy("pickup_year_month")
    )


def hourly_passengers(spark: SparkSession, period: str = "2023-05") -> DataFrame:
    """Pergunta 2 - média de `passenger_count` por hora do dia.

    `AVG` ignora nulos nativamente, e é o comportamento correto: nulo significa
    que o taxímetro não registrou, não que a corrida teve zero passageiros. As
    colunas alternativas tornam a decisão auditável.
    """
    silver = spark.table(config.TABLE_SILVER)

    return (
        silver.filter(F.col("pickup_year_month") == period)
        .groupBy("pickup_hour")
        .agg(
            F.count("*").alias("corridas"),
            F.avg("passenger_count").alias("media_passageiros"),
            F.avg(F.coalesce("passenger_count", F.lit(0))).alias("media_nulo_como_zero"),
            F.avg(F.when(F.col("passenger_count") > 0, F.col("passenger_count"))).alias(
                "media_apenas_positivos"
            ),
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
