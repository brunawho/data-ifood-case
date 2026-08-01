# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Análise: respostas ao case
# MAGIC
# MAGIC **Etapa 5 de 5.** As duas perguntas do enunciado, respondidas sobre a
# MAGIC camada silver.
# MAGIC
# MAGIC > 1. Qual a média de valor total (`total_amount`) recebido em um mês
# MAGIC >    considerando todos os *yellow* táxis da frota?
# MAGIC > 2. Qual a média de passageiros (`passenger_count`) por cada hora do dia
# MAGIC >    que pegaram táxi no mês de maio considerando todos os táxis da frota?
# MAGIC
# MAGIC Cada pergunta é respondida em **SQL e em PySpark**. O case deixa a escolha
# MAGIC livre; as duas versões produzem resultado idêntico e demonstram que a
# MAGIC camada de consumo atende aos dois perfis de usuário.
# MAGIC
# MAGIC **Pré-requisito:** `04_silver` executado.

# COMMAND ----------

# MAGIC %run ./_setup

# COMMAND ----------

mostrar_configuracao()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Materialização da gold
# MAGIC
# MAGIC As agregações são persistidas como tabelas Delta. É aqui — e não na silver
# MAGIC — que ficam as decisões de métrica: excluir estornos de uma média é escolha
# MAGIC analítica, não correção de qualidade.

# COMMAND ----------

from src.transform.gold import build_gold

contagens = build_gold(spark)
print(f"gold.yellow_trips_monthly            : {contagens['monthly']} linhas")
print(f"gold.yellow_trips_hourly_passengers  : {contagens['hourly']} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Pergunta 1 — média de `total_amount` por mês
# MAGIC
# MAGIC ## A ambiguidade do enunciado
# MAGIC
# MAGIC "Média de valor total recebido em um mês considerando todos os yellow táxis
# MAGIC da frota" admite duas leituras, e a diferença entre elas é de seis ordens de
# MAGIC grandeza:
# MAGIC
# MAGIC **(a) Ticket médio por corrida, por mês** — de tudo que a frota recebeu,
# MAGIC quanto vale uma corrida em média. Ordem de grandeza: dezenas de dólares.
# MAGIC
# MAGIC **(b) Faturamento médio mensal da frota** — quanto a frota inteira arrecada
# MAGIC em um mês típico. Ordem de grandeza: dezenas de milhões de dólares.
# MAGIC
# MAGIC Ambas são respondidas abaixo. Escolher uma silenciosamente esconderia uma
# MAGIC decisão que altera o resultado por um fator de milhões.
# MAGIC
# MAGIC ## A decisão sobre estornos
# MAGIC
# MAGIC A base contém 143.792 lançamentos com `total_amount` ≤ 0 — estornos e
# MAGIC ajustes contábeis registrados pela TLC como linhas negativas, e não como
# MAGIC remoção da corrida original. São registros legítimos, preservados na silver.
# MAGIC
# MAGIC Para a métrica de valor recebido por corrida, incluí-los distorce: um
# MAGIC estorno não é uma corrida. Ambas as versões são apresentadas.

# COMMAND ----------

# MAGIC %md
# MAGIC ### SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   pickup_year_month                AS competencia,
# MAGIC   corridas,
# MAGIC   ticket_medio_bruto,
# MAGIC   ticket_medio_sem_estornos,
# MAGIC   faturamento_bruto,
# MAGIC   faturamento_sem_estornos,
# MAGIC   estornos
# MAGIC FROM workspace.gold.yellow_trips_monthly
# MAGIC ORDER BY competencia

# COMMAND ----------

# MAGIC %md
# MAGIC ### PySpark

# COMMAND ----------

from pyspark.sql import functions as F

silver = spark.table(config.TABLE_SILVER)

resposta_1 = (
    silver.groupBy("pickup_year_month")
    .agg(
        F.count("*").alias("corridas"),
        F.round(F.avg("total_amount"), 2).alias("ticket_medio_bruto"),
        F.round(
            F.avg(F.when(~F.col("flag_valor_nao_positivo"), F.col("total_amount"))), 2
        ).alias("ticket_medio_sem_estornos"),
        F.round(F.sum("total_amount"), 2).alias("faturamento_bruto"),
    )
    .orderBy("pickup_year_month")
)

display(resposta_1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### A resposta consolidada
# MAGIC
# MAGIC Média entre as cinco competências, nas duas leituras.
# MAGIC
# MAGIC Note a distinção entre **média simples** e **média ponderada**: a média das
# MAGIC cinco médias mensais atribui peso igual a cada mês, ignorando que maio teve
# MAGIC cerca de 20% mais corridas que fevereiro. A média sobre todas as corridas
# MAGIC pondera pelo volume real. As duas estão corretas — respondem a perguntas
# MAGIC diferentes.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH mensal AS (
# MAGIC   SELECT * FROM workspace.gold.yellow_trips_monthly
# MAGIC )
# MAGIC SELECT
# MAGIC   '(a) Ticket medio por corrida'                        AS leitura,
# MAGIC   ROUND(AVG(ticket_medio_sem_estornos), 2)              AS media_simples,
# MAGIC   ROUND(SUM(faturamento_sem_estornos) / SUM(corridas_faturadas), 2)
# MAGIC                                                          AS media_ponderada
# MAGIC FROM mensal
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   '(b) Faturamento mensal da frota',
# MAGIC   ROUND(AVG(faturamento_sem_estornos), 2),
# MAGIC   NULL
# MAGIC FROM mensal

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Pergunta 2 — média de passageiros por hora (maio/2023)
# MAGIC
# MAGIC ## A decisão sobre `passenger_count` nulo
# MAGIC
# MAGIC 427.771 corridas na silver não têm `passenger_count` registrado — o
# MAGIC taxímetro não capturou a informação. **Nulo não é zero:** significa ausência
# MAGIC de registro, não corrida sem passageiro.
# MAGIC
# MAGIC O comportamento nativo do `AVG` é ignorar nulos, e é o correto aqui.
# MAGIC Substituir por zero puxaria a média para baixo artificialmente. As três
# MAGIC alternativas são exibidas para tornar a decisão auditável.

# COMMAND ----------

# MAGIC %md
# MAGIC ### SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   LPAD(pickup_hour, 2, '0') || 'h'  AS hora,
# MAGIC   corridas,
# MAGIC   media_passageiros,
# MAGIC   media_nulo_como_zero,
# MAGIC   media_apenas_positivos,
# MAGIC   sem_registro
# MAGIC FROM workspace.gold.yellow_trips_hourly_passengers
# MAGIC ORDER BY pickup_hour

# COMMAND ----------

# MAGIC %md
# MAGIC ### PySpark

# COMMAND ----------

resposta_2 = (
    silver.filter(F.col("pickup_year_month") == "2023-05")
    .groupBy("pickup_hour")
    .agg(
        F.count("*").alias("corridas"),
        F.round(F.avg("passenger_count"), 4).alias("media_passageiros"),
    )
    .orderBy("pickup_hour")
)

display(resposta_2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualização
# MAGIC
# MAGIC A distribuição horária revela o padrão de ocupação ao longo do dia.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT pickup_hour, media_passageiros
# MAGIC FROM workspace.gold.yellow_trips_hourly_passengers
# MAGIC ORDER BY pickup_hour

# COMMAND ----------

# MAGIC %md
# MAGIC > **Dica:** no seletor de visualização acima, escolha *Bar* com
# MAGIC > `pickup_hour` no eixo X e `media_passageiros` no eixo Y.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Contexto: volume por hora
# MAGIC
# MAGIC A média de passageiros isolada pode enganar — uma hora com poucas corridas
# MAGIC produz média mais volátil. Cruzar com o volume mostra onde a métrica é mais
# MAGIC confiável.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT pickup_hour,
# MAGIC        corridas,
# MAGIC        media_passageiros,
# MAGIC        ROUND(100.0 * sem_registro / corridas, 2) AS pct_sem_registro
# MAGIC FROM workspace.gold.yellow_trips_hourly_passengers
# MAGIC ORDER BY corridas DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Consulta pelo usuário final
# MAGIC
# MAGIC A silver é consultável diretamente por SQL, sem depender das agregações
# MAGIC pré-materializadas. Exemplo de pergunta não prevista pelo case, respondida
# MAGIC sem alterar o pipeline:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Ticket medio por dia da semana (1 = domingo)
# MAGIC SELECT pickup_day_of_week,
# MAGIC        COUNT(*)                                AS corridas,
# MAGIC        ROUND(AVG(total_amount), 2)             AS ticket_medio,
# MAGIC        ROUND(AVG(trip_duration_seconds) / 60, 1) AS duracao_media_min
# MAGIC FROM workspace.silver.fact_yellow_trips
# MAGIC WHERE NOT flag_valor_nao_positivo
# MAGIC GROUP BY pickup_day_of_week
# MAGIC ORDER BY pickup_day_of_week

# COMMAND ----------

# MAGIC %md
# MAGIC Essa é a justificativa arquitetural de preservar anomalias com sinalização
# MAGIC em vez de filtrá-las: a camada de consumo responde perguntas que não foram
# MAGIC formuladas quando o pipeline foi construído.
