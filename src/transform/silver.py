"""
Etapa 4 do pipeline: bronze -> silver.

Contrato desta camada:
  * granularidade preservada (1 linha = 1 corrida);
  * descarta **apenas o que é comprovadamente inválido** - corrida fora do
    escopo temporal e corrida com duração não-positiva (6.285 registros,
    0,039% da base, conforme `docs/achados-eda.md`);
  * o que é suspeito mas não comprovadamente inválido é preservado e
    **sinalizado** em colunas `flag_*`, deixando a decisão ao consumidor;
  * o descartado vai para uma tabela de quarentena, com o motivo - dado
    descartado silenciosamente é dado perdido;
  * colunas derivadas prontas para consumo (data, competência, hora, duração).

Por que sinalizar em vez de filtrar? A silver é camada de consumo genérica, que
precisa servir perguntas ainda não formuladas. Remover os 141.407 estornos aqui
significaria decidir, em nome de todos os futuros consumidores da tabela, que
ninguém jamais vai querer analisar estorno. Decisões de métrica pertencem à
gold.

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

# Capacidade máxima plausível: 4 passageiros em sedan, 5 em minivan autorizada,
# mais criança de colo. Acima de 6 não corresponde a nenhuma configuração legal.
MAX_PLAUSIBLE_PASSENGERS = 6

# Corrida acima de 24h é implausível (taxímetro esquecido ligado), mas não
# impossível - por isso é sinalizada, não descartada.
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

    Nada é filtrado aqui: a função apenas anota. A separação entre válido e
    inválido acontece depois, o que permite que o mesmo cálculo alimente tanto
    a silver quanto a quarentena, sem risco de divergirem.
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
        # --- Sinalizações: suspeito, porém preservado -------------------- #
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
        # --- Motivo de descarte: NULL significa registro válido ----------- #
        .withColumn(
            "motivo_descarte",
            F.when(fora_do_escopo, F.lit("fora_do_escopo_temporal"))
            .when(duracao_invalida, F.lit("duracao_nao_positiva")),
        )
    )


def build_silver(spark: SparkSession) -> dict[str, int]:
    """Reconstrói silver e quarentena a partir da bronze completa.

    **Por que reconstrução total e não carga incremental por partição?**

    A silver é particionada por `pickup_year_month` (data real da corrida),
    enquanto a bronze é particionada por `_ref_period` (arquivo de origem). A
    EDA mostrou que essas chaves não têm correspondência: o arquivo de maio
    contém corrida de setembro, e o de janeiro contém corrida de fevereiro.

    Uma partição da bronze alimenta várias partições da silver, e uma partição
    da silver pode receber dados de qualquer arquivo. Não há como recarregar um
    mês da silver com garantia de completude sem varrer toda a bronze - o que
    tornaria o "incremental" apenas uma reconstrução total com passos extras e
    risco de inconsistência.

    Com 16 milhões de registros a reconstrução leva segundos, então a escolha
    simples também é a correta. Em volume maior, a solução seria um MERGE por
    chave de negócio - que esta base não possui, por não ter chave primária.
    """
    ensure_namespaces(spark)

    # Sem .cache(): compute serverless não suporta persistência de DataFrame.
    # O plano é reavaliado nas duas escritas, o que é aceitável - e as contagens
    # abaixo saem das tabelas Delta já gravadas, onde COUNT(*) é resolvido pelas
    # estatísticas do log de transação, sem varrer os dados.
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
