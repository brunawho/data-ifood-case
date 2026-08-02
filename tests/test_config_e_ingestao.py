"""
Testes de configuração, resolução de caminhos e idempotência da ingestão.

Não criam `SparkSession`, mas alguns importam `src.transform.bronze`, que
importa PySpark no topo do módulo. Portanto **exigem que o pacote pyspark esteja
instalado**, ainda que não precisem de JVM nem de cluster. Rodam em segundos.
"""

from __future__ import annotations

import importlib
import os

import pytest


@pytest.fixture
def config_local(monkeypatch, tmp_path):
    """Recarrega `src.config` em modo local, com raiz temporária.

    O módulo resolve caminhos no momento do import, então trocar variável de
    ambiente depois não tem efeito: é preciso reimportar.
    """
    monkeypatch.setenv("IFOOD_ENV", "local")
    monkeypatch.setenv("IFOOD_LOCAL_ROOT", str(tmp_path))
    monkeypatch.delenv("IFOOD_CATALOG", raising=False)
    from src import config

    return importlib.reload(config)


@pytest.fixture
def config_databricks(monkeypatch):
    monkeypatch.setenv("IFOOD_ENV", "databricks")
    monkeypatch.setenv("IFOOD_CATALOG", "workspace")
    from src import config

    return importlib.reload(config)


# --------------------------------------------------------------------------
# Escopo
# --------------------------------------------------------------------------
def test_periodos_cobrem_o_escopo_do_case(config_local):
    """O case pede janeiro a maio de 2023. Cinco competências, sem buracos."""
    assert config_local.PERIODS == (
        "2023-01",
        "2023-02",
        "2023-03",
        "2023-04",
        "2023-05",
    )


def test_colunas_exigidas_pelo_case_estao_declaradas(config_local):
    """As cinco colunas que o enunciado exige na camada de consumo."""
    assert set(config_local.REQUIRED_COLUMNS) == {
        "VendorID",
        "passenger_count",
        "total_amount",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
    }


# --------------------------------------------------------------------------
# Resolução de caminhos
# --------------------------------------------------------------------------
def test_landing_usa_volume_do_unity_catalog_no_databricks(config_databricks):
    assert config_databricks.landing_root() == "/Volumes/workspace/raw/landing"


def test_landing_usa_filesystem_local_fora_do_databricks(config_local, tmp_path):
    assert config_local.landing_root() == str(tmp_path / "landing")


def test_particao_da_landing_nomeia_referencia_do_arquivo(config_databricks):
    """A partição é `ref_year`/`ref_month`, não `year`/`month`.

    A distinção é deliberada: o arquivo de janeiro contém corridas de outros
    meses, então nomear a partição como data da corrida induziria ao erro de
    agrupar por ela. Se alguém renomear, este teste falha.
    """
    segmentos = config_databricks.landing_dir("2023-01").split("/")
    assert "ref_year=2023" in segmentos
    assert "ref_month=01" in segmentos
    # Comparação por segmento, e não por substring: "ref_year=2023" contém
    # "year=2023", então `in caminho` passaria mesmo com o nome errado.
    assert "year=2023" not in segmentos
    assert "month=01" not in segmentos


def test_url_de_origem_aponta_para_o_cdn_da_tlc(config_databricks):
    url = config_databricks.source_url("2023-05")
    assert url.endswith("/yellow_tripdata_2023-05.parquet")
    assert url.startswith("https://")


def test_tabelas_usam_namespace_de_tres_niveis(config_databricks):
    assert config_databricks.TABLE_BRONZE == "workspace.bronze.yellow_tripdata"
    assert config_databricks.TABLE_SILVER == "workspace.silver.fact_yellow_trips"


def test_catalogo_pode_ser_sobrescrito_por_ambiente(monkeypatch):
    """Trocar o catálogo deve reapontar todas as tabelas de uma vez."""
    monkeypatch.setenv("IFOOD_ENV", "databricks")
    monkeypatch.setenv("IFOOD_CATALOG", "outro_catalogo")
    from src import config

    recarregado = importlib.reload(config)
    assert recarregado.TABLE_BRONZE.startswith("outro_catalogo.")
    assert recarregado.landing_root().startswith("/Volumes/outro_catalogo/")


# --------------------------------------------------------------------------
# Casamento de nomes de coluna
# --------------------------------------------------------------------------
@pytest.mark.parametrize(
    "disponiveis, procurada, esperado",
    [
        (["airport_fee"], "airport_fee", "airport_fee"),
        # A TLC trocou a caixa desta coluna em 2024. Sem resolução
        # caixa-insensitiva, a ingestão de arquivos novos perderia o dado.
        (["Airport_fee"], "airport_fee", "Airport_fee"),
        (["VendorID"], "vendorid", "VendorID"),
        (["total_amount"], "trip_distance", None),
        ([], "VendorID", None),
    ],
)
def test_resolucao_de_coluna_ignora_caixa(disponiveis, procurada, esperado):
    from src.transform.bronze import _resolve

    assert _resolve(disponiveis, procurada) == esperado


def test_tipos_canonicos_cobrem_as_colunas_exigidas(config_local):
    from src.transform.bronze import CANONICAL_TYPES

    for coluna in config_local.REQUIRED_COLUMNS:
        assert coluna in CANONICAL_TYPES


def test_horarios_permanecem_sem_fuso():
    """`TIMESTAMP_NTZ` preservado.

    Converter para `timestamp` faria o Spark interpretar o horário de parede no
    fuso da sessão. Como a pergunta 2 do case é sobre hora do dia, a resposta
    passaria a depender de configuração de sessão.
    """
    from src.transform.bronze import CANONICAL_TYPES

    assert CANONICAL_TYPES["tpep_pickup_datetime"] == "timestamp_ntz"
    assert CANONICAL_TYPES["tpep_dropoff_datetime"] == "timestamp_ntz"


# --------------------------------------------------------------------------
# Idempotência da ingestão
# --------------------------------------------------------------------------
def test_arquivo_integro_nao_e_baixado_de_novo(config_local, monkeypatch, tmp_path):
    from src.ingestion import landing

    importlib.reload(landing)
    destino = config_local.landing_file("2023-01")
    os.makedirs(os.path.dirname(destino), exist_ok=True)
    with open(destino, "wb") as f:
        f.write(b"x" * 1024)

    monkeypatch.setattr(landing, "_remote_size", lambda url: 1024)
    monkeypatch.setattr(
        landing, "_download", lambda *a: pytest.fail("não deveria baixar")
    )
    monkeypatch.setattr(landing, "_append_manifest", lambda r: None)

    assert landing.ingest_period("2023-01")["status"] == "skipped"


def test_arquivo_truncado_e_baixado_de_novo(config_local, monkeypatch, tmp_path):
    """Tamanho divergente indica download incompleto ou republicação."""
    from src.ingestion import landing

    importlib.reload(landing)
    destino = config_local.landing_file("2023-01")
    os.makedirs(os.path.dirname(destino), exist_ok=True)
    with open(destino, "wb") as f:
        f.write(b"x" * 500)  # menor que o anunciado pela origem

    baixou = []
    monkeypatch.setattr(landing, "_remote_size", lambda url: 1024)
    monkeypatch.setattr(
        landing, "_download", lambda url, dest: (baixou.append(dest), 1024)[1]
    )
    monkeypatch.setattr(landing, "_sha256", lambda p: "abc123")
    monkeypatch.setattr(landing, "_append_manifest", lambda r: None)

    assert landing.ingest_period("2023-01")["status"] == "downloaded"
    assert baixou, "arquivo truncado deveria ter sido rebaixado"


def test_falha_em_um_periodo_nao_aborta_os_demais(config_local, monkeypatch):
    """Perder o quinto arquivo não pode invalidar os quatro anteriores."""
    from src.ingestion import landing

    importlib.reload(landing)

    def falha_em_marco(period, force=False):
        if period == "2023-03":
            raise RuntimeError("timeout simulado")
        return {"period": period, "status": "downloaded"}

    monkeypatch.setattr(landing, "ingest_period", falha_em_marco)
    resultados = landing.ingest(("2023-01", "2023-02", "2023-03", "2023-04"))

    status = {r["period"]: r["status"] for r in resultados}
    assert status["2023-03"] == "failed"
    assert status["2023-04"] == "downloaded"
