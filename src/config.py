"""
Configuração central: caminhos, nomes de objeto e ambiente.

Existe para que o mesmo código rode no Databricks e localmente sem `if`
espalhado pelo pipeline. Tudo pode ser sobrescrito por variável de ambiente.
"""

from __future__ import annotations

import os
from pathlib import Path

# CDN oficial da TLC, linkado na página do órgão.
TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
DATASET = "yellow_tripdata"

# Janela pedida no case: janeiro a maio de 2023.
PERIODS: tuple[str, ...] = ("2023-01", "2023-02", "2023-03", "2023-04", "2023-05")

# Colunas exigidas pelo case na camada de consumo.
REQUIRED_COLUMNS: tuple[str, ...] = (
    "VendorID",
    "passenger_count",
    "total_amount",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
)


# A detecção automática é conveniência; IFOOD_ENV tem precedência porque em
# compute serverless DATABRICKS_RUNTIME_VERSION não é garantida, e uma detecção
# errada aqui falha em silêncio.
_AUTO_ENV = "databricks" if "DATABRICKS_RUNTIME_VERSION" in os.environ else "local"
ENV = os.getenv("IFOOD_ENV", _AUTO_ENV)
IS_DATABRICKS = ENV == "databricks"

# O Free Edition não permite criar catálogo novo (falta storage credential).
CATALOG = os.getenv("IFOOD_CATALOG", "workspace" if IS_DATABRICKS else "spark_catalog")

SCHEMA_RAW = os.getenv("IFOOD_SCHEMA_RAW", "raw")
SCHEMA_BRONZE = os.getenv("IFOOD_SCHEMA_BRONZE", "bronze")
SCHEMA_SILVER = os.getenv("IFOOD_SCHEMA_SILVER", "silver")
SCHEMA_GOLD = os.getenv("IFOOD_SCHEMA_GOLD", "gold")

VOLUME_LANDING = os.getenv("IFOOD_VOLUME_LANDING", "landing")

LOCAL_ROOT = Path(os.getenv("IFOOD_LOCAL_ROOT", "./data")).resolve()


def landing_root() -> str:
    """Raiz da landing zone: Volume do UC no Databricks, pasta local fora dele."""
    if IS_DATABRICKS:
        return f"/Volumes/{CATALOG}/{SCHEMA_RAW}/{VOLUME_LANDING}"
    return str(LOCAL_ROOT / "landing")


def landing_dir(period: str) -> str:
    """Diretório de um período dentro da landing.

    As partições são `ref_year`/`ref_month` - referência do arquivo, não data da
    corrida. Os dois não coincidem: cada arquivo mensal da TLC contém registros
    com pickup fora do próprio mês.
    """
    year, month = period.split("-")
    return f"{landing_root()}/{DATASET}/ref_year={year}/ref_month={month}"


def landing_file(period: str) -> str:
    return f"{landing_dir(period)}/{DATASET}_{period}.parquet"


def source_url(period: str) -> str:
    return f"{TLC_BASE_URL}/{DATASET}_{period}.parquet"


def manifest_path() -> str:
    """Manifest de ingestão: linhagem e detecção de republicação na origem."""
    return f"{landing_root()}/_manifest/ingestion_log.jsonl"


def table(schema: str, name: str) -> str:
    """Nome qualificado no three-level namespace do Unity Catalog."""
    return f"{CATALOG}.{schema}.{name}"


TABLE_BRONZE = table(SCHEMA_BRONZE, "yellow_tripdata")
TABLE_SILVER = table(SCHEMA_SILVER, "fact_yellow_trips")
TABLE_SILVER_QUARANTINE = table(SCHEMA_SILVER, "fact_yellow_trips_quarantine")
