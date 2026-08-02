# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Silver: camada de consumo
# MAGIC
# MAGIC **Etapa 4 de 5.** Transforma a bronze na camada que os usuários finais
# MAGIC consultam por SQL.
# MAGIC
# MAGIC **Regras aplicadas** (derivadas de `docs/achados-eda.md`, não de suposição):
# MAGIC
# MAGIC Volumes medidos na silver, após os descartes.
# MAGIC
# MAGIC | Ação | Critério | Volume |
# MAGIC |---|---|---|
# MAGIC | Descartar | *pickup* fora de jan–mai/2023 | 104 |
# MAGIC | Descartar | duração ≤ 0 | 6.180 |
# MAGIC | Sinalizar | `total_amount` ≤ 0 | 143.792 |
# MAGIC | Sinalizar | duração acima de 24h | 94 |
# MAGIC | Sinalizar | `passenger_count` nulo / zero / acima de 6 | 700.902 |
# MAGIC | Sinalizar | `VendorID` fora do dicionário consultado | 3.209 |
# MAGIC
# MAGIC Descarta apenas o comprovadamente inválido. O suspeito é preservado com
# MAGIC sinalização, porque a silver precisa servir perguntas ainda não formuladas
# MAGIC e decisões de métrica pertencem à gold.
# MAGIC
# MAGIC O que é descartado vai para uma **tabela de quarentena** com o motivo. Dado
# MAGIC descartado silenciosamente é dado perdido.
# MAGIC
# MAGIC **Pré-requisito:** `02_bronze` executado com os cinco meses.

# COMMAND ----------

# MAGIC %run ./_setup

# COMMAND ----------

mostrar_configuracao()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Construção
# MAGIC
# MAGIC A silver é reconstruída por completo a cada execução, por decisão de
# MAGIC escopo. Como as chaves de partição das duas camadas não se correspondem,
# MAGIC qualquer estratégia incremental precisaria ler toda a bronze de qualquer
# MAGIC forma; o ganho estaria só na escrita. Com este volume, a versão simples é
# MAGIC mais previsível. Detalhes na *docstring* de `build_silver`.

# COMMAND ----------

from src.transform.silver import build_silver

resultado = build_silver(spark)

total = resultado["validos"] + resultado["descartados"]
print(f"Válidos     : {resultado['validos']:>12,}".replace(",", "."))
print(f"Quarentena  : {resultado['descartados']:>12,}".replace(",", "."))
print(f"Total       : {total:>12,}".replace(",", "."))
print(f"Descarte    : {100 * resultado['descartados'] / total:.4f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validação

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Conservação de registros
# MAGIC
# MAGIC A soma de silver e quarentena precisa bater exatamente com a bronze. Se
# MAGIC não bater, há registro sumindo na transformação: o tipo de erro que passa
# MAGIC despercebido e corrompe toda análise posterior.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   (SELECT COUNT(*) FROM workspace.bronze.yellow_tripdata)               AS bronze,
# MAGIC   (SELECT COUNT(*) FROM workspace.silver.fact_yellow_trips)             AS silver,
# MAGIC   (SELECT COUNT(*) FROM workspace.silver.fact_yellow_trips_quarantine)  AS quarentena,
# MAGIC   (SELECT COUNT(*) FROM workspace.silver.fact_yellow_trips)
# MAGIC     + (SELECT COUNT(*) FROM workspace.silver.fact_yellow_trips_quarantine)
# MAGIC     - (SELECT COUNT(*) FROM workspace.bronze.yellow_tripdata)           AS diferenca

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. O que foi para a quarentena
# MAGIC
# MAGIC Esperado: 104 fora do escopo e 6.180 de duração não-positiva. A EDA
# MAGIC contou 6.181 registros com duração ≤ 0, mas um deles também está fora do
# MAGIC escopo e recebe apenas o primeiro motivo, já que são mutuamente exclusivos.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT motivo_descarte,
# MAGIC        COUNT(*)                       AS registros,
# MAGIC        MIN(tpep_pickup_datetime)      AS pickup_minimo,
# MAGIC        MAX(tpep_pickup_datetime)      AS pickup_maximo
# MAGIC FROM workspace.silver.fact_yellow_trips_quarantine
# MAGIC GROUP BY motivo_descarte
# MAGIC ORDER BY registros DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. O escopo temporal está correto?
# MAGIC
# MAGIC A silver deve conter exatamente cinco competências, de 2023-01 a 2023-05.
# MAGIC Compare com a bronze, que continha 17 meses distintos.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT pickup_year_month,
# MAGIC        COUNT(*)                            AS corridas,
# MAGIC        MIN(pickup_date)                    AS primeiro_dia,
# MAGIC        MAX(pickup_date)                    AS ultimo_dia,
# MAGIC        COUNT(DISTINCT _ref_period)         AS arquivos_contribuintes
# MAGIC FROM workspace.silver.fact_yellow_trips
# MAGIC GROUP BY pickup_year_month
# MAGIC ORDER BY pickup_year_month

# COMMAND ----------

# MAGIC %md
# MAGIC A coluna `arquivos_contribuintes` maior que 1 confirma o achado da EDA: um
# MAGIC mês recebe corridas de mais de um arquivo, e é por isso que a competência é
# MAGIC derivada da data real do *pickup*, nunca do nome do arquivo.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. As sinalizações
# MAGIC
# MAGIC Nenhuma dessas linhas foi removida. Elas estão na silver, marcadas, à
# MAGIC disposição de quem quiser incluí-las ou não em cada análise.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*)                                                         AS total,
# MAGIC   SUM(CASE WHEN flag_valor_nao_positivo      THEN 1 ELSE 0 END)    AS valor_nao_positivo,
# MAGIC   SUM(CASE WHEN flag_duracao_extrema         THEN 1 ELSE 0 END)    AS duracao_extrema,
# MAGIC   SUM(CASE WHEN flag_passageiros_ausente     THEN 1 ELSE 0 END)    AS passageiros_ausente,
# MAGIC   SUM(CASE WHEN flag_passageiros_zero        THEN 1 ELSE 0 END)    AS passageiros_zero,
# MAGIC   SUM(CASE WHEN flag_passageiros_implausivel THEN 1 ELSE 0 END)    AS passageiros_implausivel,
# MAGIC   SUM(CASE WHEN flag_fornecedor_desconhecido THEN 1 ELSE 0 END)    AS fornecedor_desconhecido
# MAGIC FROM workspace.silver.fact_yellow_trips

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. As colunas exigidas pelo case
# MAGIC
# MAGIC O enunciado exige `VendorID`, `passenger_count`, `total_amount`,
# MAGIC `tpep_pickup_datetime` e `tpep_dropoff_datetime` na camada de consumo.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE workspace.silver.fact_yellow_trips

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Amostra
# MAGIC
# MAGIC Como a tabela se apresenta ao usuário final.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT VendorID, tpep_pickup_datetime, tpep_dropoff_datetime,
# MAGIC        passenger_count, total_amount,
# MAGIC        pickup_year_month, pickup_hour, trip_duration_seconds
# MAGIC FROM workspace.silver.fact_yellow_trips
# MAGIC WHERE pickup_year_month = '2023-05'
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC Silver pronta e consultável por SQL. Próximo: **`05_analise_gold`**, com as
# MAGIC respostas às duas perguntas do case.