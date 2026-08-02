# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Landing: origem TLC → Volume
# MAGIC
# MAGIC **Etapa 1 de 5.** Baixa os parquets originais de *yellow taxi* da NYC TLC
# MAGIC (jan–mai/2023) para um Volume do Unity Catalog.
# MAGIC
# MAGIC O arquivo é copiado byte a byte, sem parse, cast ou filtro. Isso permite
# MAGIC reprocessar tudo sem depender da origem estar no ar, e torna auditável
# MAGIC contra o original qualquer decisão de limpeza feita adiante.
# MAGIC
# MAGIC Feito em Python puro: Spark é engine de processamento distribuído, não
# MAGIC cliente HTTP. O download aconteceria no driver de qualquer forma, e usá-lo
# MAGIC custaria o controle sobre retry, checksum e escrita atômica.
# MAGIC
# MAGIC Idempotente: reexecutar não baixa de novo o que já está íntegro.

# COMMAND ----------

# MAGIC %run ./_setup

# COMMAND ----------

mostrar_configuracao()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preparação do catálogo
# MAGIC
# MAGIC Cria os schemas e o Volume do Unity Catalog, se ainda não existirem. É
# MAGIC idempotente (`CREATE ... IF NOT EXISTS`), então rodar em um workspace já
# MAGIC preparado não tem efeito.
# MAGIC
# MAGIC Necessário aqui porque a landing grava direto no Volume: sem ele, o
# MAGIC download falha no primeiro comando.

# COMMAND ----------

from src.utils.spark import ensure_namespaces

ensure_namespaces(spark)

display(spark.sql(f"SHOW SCHEMAS IN {config.CATALOG}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download
# MAGIC
# MAGIC Cerca de 45 MiB por mês, ~230 MiB no total.
# MAGIC
# MAGIC Para processar apenas parte do período, passe uma tupla explícita:
# MAGIC `ingest(("2023-01",))`.

# COMMAND ----------

from src.ingestion.landing import ingest

PERIODOS = config.PERIODS

resultados = ingest(PERIODOS)
display(resultados)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validação da landing

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Arquivos no Volume
# MAGIC Confere o que efetivamente está em disco, independente do que o log diz.

# COMMAND ----------

import os

arquivos = [
    {
        "arquivo": nome,
        "caminho": os.path.join(raiz, nome),
        "tamanho_mib": round(os.path.getsize(os.path.join(raiz, nome)) / 1024 / 1024, 1),
    }
    for raiz, _, nomes in os.walk(f"{config.landing_root()}/{config.DATASET}")
    for nome in nomes
]

display(spark.createDataFrame(arquivos) if arquivos else print("Nenhum arquivo na landing"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Manifest de ingestão
# MAGIC
# MAGIC Linhagem: origem, destino, tamanho, checksum e horário de cada arquivo.
# MAGIC Se um número da análise for questionado, é aqui que se rastreia qual
# MAGIC versão exata do dado foi usada.

# COMMAND ----------

import json

with open(config.manifest_path(), encoding="utf-8") as handle:
    manifest = [json.loads(linha) for linha in handle if linha.strip()]

display(spark.createDataFrame(manifest))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. O arquivo é um Parquet legível?
# MAGIC
# MAGIC Tamanho correto não garante arquivo válido. Ler o schema é o teste mais
# MAGIC barato de integridade e já antecipa o que a bronze vai encontrar.

# COMMAND ----------

amostra = spark.read.parquet(config.landing_file(PERIODOS[0]))
print(f"Colunas na origem: {len(amostra.columns)}")
amostra.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Landing pronta. Próximo: **`02_bronze`**, que tabula esses arquivos em
# MAGIC Delta com tipos canônicos e colunas de auditoria.