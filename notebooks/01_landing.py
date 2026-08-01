# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Landing: origem TLC → Volume
# MAGIC
# MAGIC **Etapa 1 de 5.** Baixa os parquets originais de *yellow taxi* da NYC TLC
# MAGIC (jan–mai/2023) para um Volume do Unity Catalog.
# MAGIC
# MAGIC **Contrato desta camada:** o arquivo é copiado byte a byte, sem
# MAGIC interpretação. Nenhum parse, nenhum cast, nenhum filtro. A landing existe
# MAGIC para permitir reprocessar tudo do zero sem depender da origem estar
# MAGIC disponível — e para que qualquer decisão de limpeza feita adiante seja
# MAGIC auditável contra o arquivo original.
# MAGIC
# MAGIC **Tecnologia:** Python puro (`requests`). Spark não entra aqui — é engine
# MAGIC de processamento distribuído, não cliente HTTP. O download aconteceria no
# MAGIC driver de qualquer forma, e usar Spark custaria o controle sobre retry,
# MAGIC checksum e escrita atômica.
# MAGIC
# MAGIC **Idempotente:** reexecutar não baixa de novo o que já está íntegro.

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./_setup

# COMMAND ----------

mostrar_configuracao()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download
# MAGIC
# MAGIC São ~450 MiB por mês, ~2,2 GiB no total.
# MAGIC
# MAGIC Na primeira execução, rode **um mês só** (deixe a lista abaixo com
# MAGIC `("2023-01",)`). O objetivo não é o download em si, é validar que a
# MAGIC escrita no Volume funciona antes de investir a quota do dia. Depois troque
# MAGIC por `config.PERIODS` para os cinco — os já baixados são pulados.

# COMMAND ----------

from src.ingestion.landing import ingest

#PERIODOS = ("2023-01",)  # troque por config.PERIODS após validar
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
# MAGIC barato de integridade — e já antecipa o que a bronze vai encontrar.

# COMMAND ----------

amostra = spark.read.parquet(config.landing_file(PERIODOS[0]))
print(f"Colunas na origem: {len(amostra.columns)}")
amostra.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Landing pronta. Próximo: **`02_bronze`**, que tabula esses arquivos em
# MAGIC Delta com tipos canônicos e colunas de auditoria.