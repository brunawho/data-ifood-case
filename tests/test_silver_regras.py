"""
Testes das regras de qualidade da camada silver.

Estas são as regras que definem o que entra e o que sai da camada de consumo.
Exigem uma SparkSession local (sem Delta), então são mais lentos: a fixture é
de escopo de módulo para criar a sessão uma vez só.

Cada teste corresponde a uma decisão documentada em `docs/achados-eda.md`. Se
alguém alterar uma regra sem alterar a documentação, o teste falha.
"""

from __future__ import annotations

import datetime as dt

import pytest

pyspark = pytest.importorskip("pyspark", reason="PySpark não instalado")

from pyspark.sql import SparkSession, functions as F  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
)

SCHEMA = StructType(
    [
        StructField("VendorID", IntegerType()),
        StructField("tpep_pickup_datetime", TimestampNTZType()),
        StructField("tpep_dropoff_datetime", TimestampNTZType()),
        StructField("passenger_count", IntegerType()),
        StructField("total_amount", DoubleType()),
        StructField("_ref_period", StringType()),
    ]
)


@pytest.fixture(scope="module")
def spark():
    """Reaproveita a sessão ativa, ou cria uma local.

    No Databricks já existe uma sessão gerenciada pelo runtime, e o serverless
    não permite definir `master`. Localmente, cria-se uma sessão mínima, que
    exige JDK 8, 11 ou 17 instalado.
    """
    existente = SparkSession.getActiveSession()
    if existente is not None:
        yield existente
        return

    sessao = (
        SparkSession.builder.master("local[1]")
        .appName("testes-silver")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield sessao
    sessao.stop()


def corrida(
    pickup="2023-03-15 14:30:00",
    duracao_min=20,
    passageiros=1,
    valor=25.0,
    vendor=1,
    ref="2023-03",
):
    """Constrói uma corrida válida, sobrescrevendo só o que o teste precisa."""
    inicio = dt.datetime.fromisoformat(pickup)
    return (
        vendor,
        inicio,
        inicio + dt.timedelta(minutes=duracao_min),
        passageiros,
        valor,
        ref,
    )


def classificar(spark, linhas):
    from src.transform.silver import classify

    return classify(spark.createDataFrame(linhas, SCHEMA))


def motivos(df):
    return [r["motivo_descarte"] for r in df.select("motivo_descarte").collect()]


# --------------------------------------------------------------------------
# O que é descartado
# --------------------------------------------------------------------------
def test_corrida_dentro_do_escopo_e_mantida(spark):
    df = classificar(spark, [corrida()])
    assert motivos(df) == [None]


@pytest.mark.parametrize(
    "pickup",
    ["2008-12-31 23:01:42", "2001-01-01 00:06:49", "2023-09-05 18:20:48"],
)
def test_corrida_fora_do_escopo_e_descartada(spark, pickup):
    """Sujeira ocorre nas duas direções: 2001 até setembro/2023."""
    df = classificar(spark, [corrida(pickup=pickup)])
    assert motivos(df) == ["fora_do_escopo_temporal"]


@pytest.mark.parametrize("duracao", [-30, -1, 0])
def test_duracao_nao_positiva_e_descartada(spark, duracao):
    """Corrida com dropoff anterior ou igual ao pickup não é corrida."""
    df = classificar(spark, [corrida(duracao_min=duracao)])
    assert motivos(df) == ["duracao_nao_positiva"]


def test_registro_com_dois_problemas_recebe_apenas_o_primeiro_motivo(spark):
    """Os motivos são mutuamente exclusivos e avaliados em ordem.

    É por isso que a quarentena registrou 6.284 descartes e não 6.285: um
    registro está fora do escopo E tem duração não-positiva, e conta uma vez só.
    Somar as categorias da EDA superestima em 1.
    """
    df = classificar(spark, [corrida(pickup="2008-12-31 23:00:00", duracao_min=-5)])
    assert motivos(df) == ["fora_do_escopo_temporal"]


def test_corrida_na_virada_do_mes_e_preservada(spark):
    """Artefato de fronteira não é corrupção.

    Uma corrida iniciada em 1º de fevereiro transmitida com o lote de janeiro
    pertence a fevereiro, e deve ser mantida na competência correta.
    """
    df = classificar(spark, [corrida(pickup="2023-02-01 00:56:53", ref="2023-01")])
    linha = df.select("motivo_descarte", "pickup_year_month").collect()[0]
    assert linha["motivo_descarte"] is None
    assert linha["pickup_year_month"] == "2023-02"


# --------------------------------------------------------------------------
# O que é preservado com sinalização
# --------------------------------------------------------------------------
@pytest.mark.parametrize("valor", [-982.95, -10.0, 0.0])
def test_valor_nao_positivo_e_sinalizado_mas_nao_descartado(spark, valor):
    """Estornos são registros contábeis legítimos.

    Descartá-los na silver decidiria, por todos os consumidores da tabela, que
    ninguém vai querer analisá-los. A exclusão pertence à métrica, na gold.
    """
    df = classificar(spark, [corrida(valor=valor)])
    linha = df.select("motivo_descarte", "flag_valor_nao_positivo").collect()[0]
    assert linha["motivo_descarte"] is None, "não deve ser descartado"
    assert linha["flag_valor_nao_positivo"] is True


def test_duracao_extrema_e_sinalizada_mas_nao_descartada(spark):
    """167 horas é implausível, mas não impossível (taxímetro esquecido)."""
    df = classificar(spark, [corrida(duracao_min=48 * 60)])
    linha = df.select("motivo_descarte", "flag_duracao_extrema").collect()[0]
    assert linha["motivo_descarte"] is None
    assert linha["flag_duracao_extrema"] is True


def test_passageiro_ausente_e_sinalizado_e_nao_vira_zero(spark):
    """Nulo é ausência de registro, não corrida sem passageiro."""
    df = classificar(spark, [corrida(passageiros=None)])
    linha = df.select(
        "motivo_descarte", "flag_passageiros_ausente", "passenger_count"
    ).collect()[0]
    assert linha["motivo_descarte"] is None
    assert linha["flag_passageiros_ausente"] is True
    assert linha["passenger_count"] is None, "nulo não pode virar zero"


def test_passageiro_zero_e_preservado(spark):
    """Sem como distinguir erro de digitação de corrida cancelada.

    A amostra mostra corridas com zero passageiros, valor alto e duração normal:
    aconteceram, o motorista apenas não registrou.
    """
    df = classificar(spark, [corrida(passageiros=0, valor=51.65)])
    linha = df.select("motivo_descarte", "flag_passageiros_zero").collect()[0]
    assert linha["motivo_descarte"] is None
    assert linha["flag_passageiros_zero"] is True


@pytest.mark.parametrize("n, implausivel", [(4, False), (6, False), (7, True), (9, True)])
def test_limite_de_passageiros_segue_a_capacidade_legal(spark, n, implausivel):
    """4 em sedan, 5 em minivan, mais criança de colo. Acima de 6 não existe."""
    df = classificar(spark, [corrida(passageiros=n)])
    assert df.collect()[0]["flag_passageiros_implausivel"] is implausivel


@pytest.mark.parametrize("vendor, desconhecido", [(1, False), (2, False), (6, True)])
def test_fornecedor_fora_do_dicionario_e_sinalizado(spark, vendor, desconhecido):
    """A TLC documenta apenas 1 e 2 para 2023."""
    df = classificar(spark, [corrida(vendor=vendor)])
    assert df.collect()[0]["flag_fornecedor_desconhecido"] is desconhecido


# --------------------------------------------------------------------------
# Colunas derivadas
# --------------------------------------------------------------------------
def test_competencia_vem_da_corrida_e_nao_do_arquivo(spark):
    """A regra central da modelagem.

    Cada arquivo mensal contém corridas de outros meses. Derivar a competência
    do arquivo de origem produziria resposta errada nas duas perguntas do case.
    """
    df = classificar(spark, [corrida(pickup="2023-04-10 08:00:00", ref="2023-03")])
    assert df.collect()[0]["pickup_year_month"] == "2023-04"


def test_hora_do_pickup_e_extraida_para_a_pergunta_2(spark):
    df = classificar(spark, [corrida(pickup="2023-05-20 02:15:00")])
    assert df.collect()[0]["pickup_hour"] == 2


def test_duracao_e_calculada_em_segundos(spark):
    df = classificar(spark, [corrida(duracao_min=20)])
    assert df.collect()[0]["trip_duration_seconds"] == 1200


# --------------------------------------------------------------------------
# Conservação
# --------------------------------------------------------------------------
def test_nenhum_registro_se_perde_na_classificacao(spark):
    """Válidos + descartados = entrada. A mesma invariante do notebook 04."""
    linhas = [
        corrida(),
        corrida(pickup="2008-01-01 00:00:00"),
        corrida(duracao_min=0),
        corrida(valor=-50.0),
        corrida(passageiros=None),
    ]
    df = classificar(spark, linhas)
    validos = df.filter(F.col("motivo_descarte").isNull()).count()
    descartados = df.filter(F.col("motivo_descarte").isNotNull()).count()
    assert validos + descartados == len(linhas)
    assert descartados == 2


# --------------------------------------------------------------------------
# Contrato de schema da bronze
# --------------------------------------------------------------------------
def test_coluna_obrigatoria_ausente_interrompe_a_ingestao(spark, tmp_path):
    """As cinco colunas exigidas pelo case não podem virar NULL em silêncio.

    Preenchê-las com NULL produziria uma camada de consumo inutilizável sem
    nenhum sinal de erro. Colunas opcionais continuam sendo aceitas ausentes.
    """
    from src import config
    from src.transform.bronze import read_landing

    caminho = str(tmp_path / "sem_total_amount.parquet")
    (
        spark.createDataFrame(
            [(1, dt.datetime(2023, 3, 1, 10, 0), dt.datetime(2023, 3, 1, 10, 20), 1)],
            "VendorID int, tpep_pickup_datetime timestamp_ntz, "
            "tpep_dropoff_datetime timestamp_ntz, passenger_count int",
        )
        .write.mode("overwrite")
        .parquet(caminho)
    )

    original = config.landing_file
    config.landing_file = lambda period: caminho
    try:
        with pytest.raises(ValueError, match="total_amount"):
            read_landing(spark, "2023-03")
    finally:
        config.landing_file = original
