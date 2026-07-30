# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Ingestão: origem TLC → Landing → Bronze
# MAGIC
# MAGIC Este notebook é apenas o **orquestrador**. Toda a lógica vive em `src/`,
# MAGIC versionada e testável. Notebook que concentra regra de negócio não é
# MAGIC revisável em pull request nem coberto por teste — por isso aqui só há
# MAGIC chamadas e validações.
# MAGIC
# MAGIC | Camada | Objeto | Conteúdo |
# MAGIC |---|---|---|
# MAGIC | Landing | `/Volumes/workspace/raw/landing` | parquets originais, imutáveis |
# MAGIC | Bronze | `workspace.bronze.yellow_tripdata` | Delta, schema da origem + auditoria |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração
# MAGIC
# MAGIC As variáveis de ambiente precisam ser definidas **antes** de importar
# MAGIC `src.config`, porque ele resolve caminhos e nomes no momento do import.

# COMMAND ----------

import os
import sys

os.environ["IFOOD_ENV"] = "databricks"
os.environ["IFOOD_CATALOG"] = "workspace"

# Em Git folder a raiz do repo já entra no sys.path, mas ser explícito evita
# ImportError silencioso se o notebook for movido de pasta.
REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import logging

logging.basicConfig(level=logging.INFO, format="%(levelname)-7s | %(message)s", force=True)

from src import config

print(f"Ambiente : {config.ENV}")
print(f"Catálogo : {config.CATALOG}")
print(f"Landing  : {config.landing_root()}")
print(f"Bronze   : {config.TABLE_BRONZE}")
print(f"Períodos : {', '.join(config.PERIODS)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Etapa 1 — Landing
# MAGIC
# MAGIC Download dos 5 parquets originais para o Volume do Unity Catalog. É
# MAGIC idempotente: reexecutar não baixa de novo o que já está íntegro.
# MAGIC
# MAGIC São ~2 GiB no total, então a primeira execução leva alguns minutos.

# COMMAND ----------

from src.ingestion.landing import ingest

results = ingest()
display(results)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conferência da landing
# MAGIC O manifest é a linhagem da ingestão: origem, destino, tamanho, checksum
# MAGIC e horário de cada arquivo que entrou.

# COMMAND ----------

import json

with open(config.manifest_path(), encoding="utf-8") as handle:
    manifest = [json.loads(line) for line in handle if line.strip()]

display(spark.createDataFrame(manifest))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Etapa 2 — Bronze
# MAGIC
# MAGIC Leitura arquivo a arquivo, CAST para tipos canônicos, colunas de
# MAGIC auditoria e escrita Delta particionada por `_ref_period`.

# COMMAND ----------

from src.transform.bronze import build_bronze

counts = build_bronze(spark)
for period, count in counts.items():
    print(f"{period}: {count:>12,} registros".replace(",", "."))
print(f"{'TOTAL':7} {sum(counts.values()):>12,} registros".replace(",", "."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validação
# MAGIC
# MAGIC Três checagens que valem mais que um `SELECT *`:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1. Volume por competência. Se algum mês vier muito fora da faixa de
# MAGIC --    ~2,9 a 3,5 milhões, é sinal de arquivo truncado na landing.
# MAGIC SELECT _ref_period,
# MAGIC        COUNT(*)                              AS registros,
# MAGIC        COUNT(DISTINCT _source_file)          AS arquivos_origem,
# MAGIC        MIN(_ingested_at)                     AS ingerido_em
# MAGIC FROM workspace.bronze.yellow_tripdata
# MAGIC GROUP BY _ref_period
# MAGIC ORDER BY _ref_period

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 2. A evidência do problema que motivou nomear a partição como
# MAGIC --    "referência do arquivo" e não "data da corrida": cada arquivo
# MAGIC --    mensal contém corridas de fora do próprio mês.
# MAGIC SELECT _ref_period,
# MAGIC        MIN(tpep_pickup_datetime) AS pickup_minimo,
# MAGIC        MAX(tpep_pickup_datetime) AS pickup_maximo,
# MAGIC        SUM(CASE WHEN date_format(tpep_pickup_datetime, 'yyyy-MM') <> _ref_period
# MAGIC                 THEN 1 ELSE 0 END) AS corridas_fora_do_mes
# MAGIC FROM workspace.bronze.yellow_tripdata
# MAGIC GROUP BY _ref_period
# MAGIC ORDER BY _ref_period

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 3. Idempotência: nenhum registro deve ter dois horários de ingestão
# MAGIC --    distintos para a mesma partição, senão o replaceWhere duplicou.
# MAGIC SELECT _ref_period, COUNT(DISTINCT _ingested_at) AS lotes
# MAGIC FROM workspace.bronze.yellow_tripdata
# MAGIC GROUP BY _ref_period
# MAGIC HAVING COUNT(DISTINCT _ingested_at) > 1

# COMMAND ----------

# MAGIC %md
# MAGIC A bronze está pronta. O próximo notebook faz a **análise exploratória**,
# MAGIC que é o que define as regras de limpeza da silver — as regras saem do
# MAGIC diagnóstico dos dados, não de suposição.
