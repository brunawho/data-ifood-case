# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 02 — Bronze: landing → Delta
# MAGIC
# MAGIC **Etapa 2 de 5.** Tabula os parquets da landing em uma tabela Delta única,
# MAGIC governada pelo Unity Catalog e consultável por SQL.
# MAGIC
# MAGIC Preserva a granularidade e os nomes da origem, sem descartar registro
# MAGIC algum, e acrescenta as colunas de auditoria `_source_file`,
# MAGIC `_ingested_at` e `_ref_period`. Os tipos são canonizados para que os
# MAGIC cinco meses formem uma tabela única.
# MAGIC
# MAGIC Filtrar aqui seria erro de arquitetura: a bronze é a cópia fiel e
# MAGIC consultável da origem. A limpeza é responsabilidade da silver, e as regras
# MAGIC dela saem da EDA (notebook 03).
# MAGIC
# MAGIC **Pré-requisito:** `01_landing` executado.

# COMMAND ----------

# MAGIC %run ./_setup

# COMMAND ----------

mostrar_configuracao()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carga
# MAGIC
# MAGIC Mantenha a mesma lista de períodos usada no `01_landing` — a bronze lê da
# MAGIC landing, então só processa o que já foi baixado.
# MAGIC
# MAGIC A escrita usa `replaceWhere` por partição: reprocessar março não toca nos
# MAGIC outros quatro meses, e reexecutar não duplica dado.

# COMMAND ----------

from src.transform.bronze import build_bronze

PERIODOS = config.PERIODS

contagens = build_bronze(spark, PERIODOS)

for periodo, total in contagens.items():
    print(f"{periodo}: {total:>12,} registros".replace(",", "."))
print(f"{'TOTAL':<7} {sum(contagens.values()):>12,} registros".replace(",", "."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validação
# MAGIC
# MAGIC Três checagens que respondem perguntas específicas. `SELECT *` mostraria
# MAGIC apenas que "tem dado lá", o que não interessa.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. O volume está correto?
# MAGIC
# MAGIC Cada mês de *yellow taxi* em 2023 tem entre ~2,9 e ~3,5 milhões de
# MAGIC corridas. Valor muito fora dessa faixa indica arquivo truncado na landing.
# MAGIC `arquivos_origem = 1` por competência confirma que não houve mistura.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT _ref_period,
# MAGIC        COUNT(*)                     AS registros,
# MAGIC        COUNT(DISTINCT _source_file) AS arquivos_origem,
# MAGIC        MIN(_ingested_at)            AS ingerido_em
# MAGIC FROM workspace.bronze.yellow_tripdata
# MAGIC GROUP BY _ref_period
# MAGIC ORDER BY _ref_period

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. O dado está sujo de que forma?
# MAGIC
# MAGIC **Esta é a query mais importante do notebook.** Ela produz a evidência do
# MAGIC problema que motivou nomear a partição como `_ref_period` ("referência do
# MAGIC arquivo") e não como data da corrida: cada arquivo mensal da TLC contém
# MAGIC corridas com *pickup* fora do próprio mês, incluindo datas absurdas de
# MAGIC anos anteriores.
# MAGIC
# MAGIC O número que sair daqui é o que vai justificar a regra de filtro da silver.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT _ref_period,
# MAGIC        MIN(tpep_pickup_datetime) AS pickup_minimo,
# MAGIC        MAX(tpep_pickup_datetime) AS pickup_maximo,
# MAGIC        SUM(CASE WHEN date_format(tpep_pickup_datetime, 'yyyy-MM') <> _ref_period
# MAGIC                 THEN 1 ELSE 0 END) AS corridas_fora_do_mes,
# MAGIC        ROUND(100.0 * SUM(CASE WHEN date_format(tpep_pickup_datetime, 'yyyy-MM') <> _ref_period
# MAGIC                 THEN 1 ELSE 0 END) / COUNT(*), 4) AS pct_fora_do_mes
# MAGIC FROM workspace.bronze.yellow_tripdata
# MAGIC GROUP BY _ref_period
# MAGIC ORDER BY _ref_period

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. O pipeline é realmente idempotente?
# MAGIC
# MAGIC Se o `replaceWhere` estivesse acrescentando em vez de substituir, a mesma
# MAGIC partição teria registros com horários de ingestão distintos.
# MAGIC
# MAGIC **Resultado esperado: zero linhas.** Rode a célula de carga duas vezes e
# MAGIC confira que continua vazia.
# MAGIC
# MAGIC A verificação cobre o cenário de reexecução da mesma carga. Não detecta
# MAGIC duplicatas presentes dentro de um único arquivo de origem, nem uma
# MAGIC republicação da TLC com o mesmo tamanho de bytes.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT _ref_period, COUNT(DISTINCT _ingested_at) AS lotes_distintos
# MAGIC FROM workspace.bronze.yellow_tripdata
# MAGIC GROUP BY _ref_period
# MAGIC HAVING COUNT(DISTINCT _ingested_at) > 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### Histórico Delta
# MAGIC
# MAGIC O *transaction log* registra cada operação de escrita. É a prova de
# MAGIC linhagem no nível da tabela, complementando o manifest da landing.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY workspace.bronze.yellow_tripdata

# COMMAND ----------

# MAGIC %md
# MAGIC Bronze pronta e consultável por SQL. Próximo: **`03_eda`**, a análise
# MAGIC exploratória que define as regras de limpeza da silver.