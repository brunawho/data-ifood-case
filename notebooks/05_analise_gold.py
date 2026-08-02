# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Análise: respostas ao case
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
# MAGIC As agregações são persistidas como tabelas Delta. As decisões de métrica
# MAGIC ficam aqui, e não na silver: excluir registros com `total_amount <= 0`
# MAGIC analítica, não correção de qualidade.

# COMMAND ----------

from src.transform.gold import build_gold

contagens = build_gold(spark)
print(f"gold.yellow_trips_monthly            : {contagens['monthly']} linhas")
print(f"gold.yellow_trips_hourly_passengers  : {contagens['hourly']} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Pergunta 1: média de `total_amount` por mês
# MAGIC
# MAGIC ## A ambiguidade do enunciado
# MAGIC
# MAGIC "Média de valor total recebido em um mês considerando todos os yellow táxis
# MAGIC da frota" admite duas leituras, e a diferença entre elas é de seis ordens de
# MAGIC grandeza:
# MAGIC
# MAGIC **(a) Ticket médio por corrida, por mês.** Dezenas de dólares.
# MAGIC
# MAGIC **(b) Faturamento médio mensal da frota.** Dezenas de milhões de dólares.
# MAGIC
# MAGIC Ambas são respondidas abaixo. Escolher uma silenciosamente esconderia uma
# MAGIC decisão que altera o resultado por um fator de milhões.
# MAGIC
# MAGIC ## A decisão sobre lançamentos não-positivos
# MAGIC
# MAGIC A camada silver contém 143.792 lançamentos com `total_amount` ≤ 0. A
# MAGIC interpretação usual é que sejam reversões e ajustes contábeis registrados
# MAGIC pela TLC como linhas negativas, mas **o dataset não comprova isso**: não há
# MAGIC campo que identifique reversão nem chave que permita parear um lançamento
# MAGIC negativo com a corrida que ele corrigiria.
# MAGIC
# MAGIC Por isso a resposta oficial inclui todos os registros. A versão que os
# MAGIC exclui aparece como análise de sensibilidade, com a hipótese declarada.

# COMMAND ----------

# MAGIC %md
# MAGIC ### SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC -- A gold guarda os valores sem arredondamento, para que a consolidação do
# MAGIC -- período não propague erro de truncamento. O ROUND fica na apresentação.
# MAGIC SELECT
# MAGIC   pickup_year_month                             AS competencia,
# MAGIC   corridas,
# MAGIC   ROUND(ticket_medio_bruto, 2)                  AS ticket_medio_bruto,
# MAGIC   ROUND(ticket_medio_sem_nao_positivos, 2)      AS ticket_medio_sem_nao_positivos,
# MAGIC   ROUND(faturamento_bruto, 2)                   AS faturamento_bruto,
# MAGIC   ROUND(faturamento_sem_nao_positivos, 2)       AS faturamento_sem_nao_positivos,
# MAGIC   lancamentos_nao_positivos
# MAGIC FROM workspace.gold.yellow_trips_monthly
# MAGIC ORDER BY competencia

# COMMAND ----------

# MAGIC %md
# MAGIC ### PySpark

# COMMAND ----------

from pyspark.sql import functions as F

silver = spark.table(config.TABLE_SILVER)
positivo = ~F.col("flag_valor_nao_positivo")

resposta_1 = (
    silver.groupBy("pickup_year_month")
    .agg(
        F.count("*").alias("corridas"),
        F.round(F.avg("total_amount"), 2).alias("ticket_medio_bruto"),
        F.round(F.avg(F.when(positivo, F.col("total_amount"))), 2).alias(
            "ticket_medio_sem_nao_positivos"
        ),
        F.round(F.sum("total_amount"), 2).alias("faturamento_bruto"),
        F.round(F.sum(F.when(positivo, F.col("total_amount"))), 2).alias(
            "faturamento_sem_nao_positivos"
        ),
        F.sum(F.when(F.col("flag_valor_nao_positivo"), 1).otherwise(0)).alias(
            "lancamentos_nao_positivos"
        ),
    )
    .withColumnRenamed("pickup_year_month", "competencia")
    .orderBy("competencia")
)

display(resposta_1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### As duas versões concordam?
# MAGIC
# MAGIC Apresentar duas implementações só tem valor se elas produzirem o mesmo
# MAGIC resultado. A verificação abaixo compara a agregação materializada na gold
# MAGIC com o cálculo feito sobre a silver nos dois sentidos: `exceptAll` em uma
# MAGIC direção só não detectaria linhas sobrando na outra.

# COMMAND ----------

# Arredonda com a mesma precisão da versão PySpark: a gold guarda valores
# brutos, então a comparação precisa aplicar o mesmo tratamento nos dois lados.
resposta_1_sql = spark.sql("""
    SELECT pickup_year_month                        AS competencia,
           corridas,
           ROUND(ticket_medio_bruto, 2)             AS ticket_medio_bruto,
           ROUND(ticket_medio_sem_nao_positivos, 2) AS ticket_medio_sem_nao_positivos,
           ROUND(faturamento_bruto, 2)              AS faturamento_bruto,
           ROUND(faturamento_sem_nao_positivos, 2)  AS faturamento_sem_nao_positivos,
           lancamentos_nao_positivos
    FROM workspace.gold.yellow_trips_monthly
""")

divergencias = (
    resposta_1_sql.exceptAll(resposta_1).count()
    + resposta_1.exceptAll(resposta_1_sql).count()
)

print(f"Divergências entre SQL e PySpark: {divergencias}")
assert divergencias == 0, "As duas implementações produzem resultados diferentes"

# COMMAND ----------

# MAGIC %md
# MAGIC ### A resposta consolidada
# MAGIC
# MAGIC Média entre as cinco competências, nas duas leituras.
# MAGIC
# MAGIC **A resposta oficial é a literal**, sobre todos os registros da camada de
# MAGIC consumo (colunas `resposta_*`). As colunas `sensib_*` excluem os
# MAGIC lançamentos não-positivos e dependem de uma hipótese de negócio que os
# MAGIC dados não comprovam.
# MAGIC
# MAGIC Note a distinção entre **média simples** e **média ponderada** na leitura
# MAGIC (a): a média das cinco médias mensais atribui peso igual a cada mês,
# MAGIC ignorando que maio teve cerca de 20% mais corridas que fevereiro. A média
# MAGIC sobre todas as corridas pondera pelo volume real. As duas estão corretas
# MAGIC e respondem a perguntas diferentes.
# MAGIC
# MAGIC A leitura (b) não admite versão ponderada: a média de faturamento entre
# MAGIC cinco meses é uma média simples por definição, já que a unidade de
# MAGIC agregação é o próprio mês.
# MAGIC
# MAGIC As duas implementações já foram demonstradas acima. As consolidações a
# MAGIC seguir usam SQL por concisão; o resultado independe da API escolhida.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Leitura (a): ticket medio por corrida.
# MAGIC -- `resposta_*` sao a resposta oficial (todos os registros). As colunas
# MAGIC -- `sensib_*` excluem os lancamentos nao-positivos e dependem da hipotese
# MAGIC -- de que sejam lancamentos_nao_positivos, que os dados nao comprovam.
# MAGIC SELECT
# MAGIC   ROUND(AVG(ticket_medio_bruto), 2)                                 AS resposta_media_simples,
# MAGIC   ROUND(SUM(faturamento_bruto) / SUM(corridas), 2)                  AS resposta_media_ponderada,
# MAGIC   ROUND(AVG(ticket_medio_sem_nao_positivos), 2)                          AS sensib_media_simples,
# MAGIC   ROUND(SUM(faturamento_sem_nao_positivos) / SUM(corridas_faturadas), 2) AS sensib_media_ponderada
# MAGIC FROM workspace.gold.yellow_trips_monthly

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Leitura (b): faturamento mensal da frota
# MAGIC SELECT
# MAGIC   ROUND(AVG(faturamento_bruto), 2)        AS resposta_faturamento_medio,
# MAGIC   ROUND(MIN(faturamento_bruto), 2)        AS menor_mes,
# MAGIC   ROUND(MAX(faturamento_bruto), 2)        AS maior_mes,
# MAGIC   ROUND(SUM(faturamento_bruto), 2)        AS total_no_periodo,
# MAGIC   ROUND(AVG(faturamento_sem_nao_positivos), 2) AS sensib_faturamento_medio
# MAGIC FROM workspace.gold.yellow_trips_monthly

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Pergunta 2: média de passageiros por hora (maio/2023)
# MAGIC
# MAGIC ## A decisão sobre `passenger_count` nulo
# MAGIC
# MAGIC 427.771 corridas na silver não têm `passenger_count` registrado: o
# MAGIC taxímetro não capturou a informação. **Nulo não é zero**, e substituir por
# MAGIC zero puxaria a média para baixo artificialmente.
# MAGIC
# MAGIC O comportamento nativo do `AVG`, ignorar nulos, é o correto aqui. As três
# MAGIC alternativas são exibidas para tornar a decisão auditável.

# COMMAND ----------

# MAGIC %md
# MAGIC ### SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   LPAD(pickup_hour, 2, '0') || 'h'  AS hora,
# MAGIC   corridas,
# MAGIC   ROUND(media_passageiros, 4)      AS media_passageiros,
# MAGIC   ROUND(media_nulo_como_zero, 4)   AS media_nulo_como_zero,
# MAGIC   ROUND(media_apenas_positivos, 4) AS media_apenas_positivos,
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
        F.round(F.avg(F.coalesce("passenger_count", F.lit(0))), 4).alias(
            "media_nulo_como_zero"
        ),
        F.round(
            F.avg(F.when(F.col("passenger_count") > 0, F.col("passenger_count"))), 4
        ).alias("media_apenas_positivos"),
        F.sum(F.when(F.col("flag_passageiros_ausente"), 1).otherwise(0)).alias(
            "sem_registro"
        ),
    )
    .orderBy("pickup_hour")
)

display(resposta_2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### As duas versões concordam?

# COMMAND ----------

resposta_2_sql = spark.sql("""
    SELECT pickup_hour, corridas,
           ROUND(media_passageiros, 4)      AS media_passageiros,
           ROUND(media_nulo_como_zero, 4)   AS media_nulo_como_zero,
           ROUND(media_apenas_positivos, 4) AS media_apenas_positivos,
           sem_registro
    FROM workspace.gold.yellow_trips_hourly_passengers
""")

divergencias_2 = (
    resposta_2_sql.exceptAll(resposta_2).count()
    + resposta_2.exceptAll(resposta_2_sql).count()
)

print(f"Divergências entre SQL e PySpark: {divergencias_2}")
assert divergencias_2 == 0, "As duas implementações produzem resultados diferentes"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualização
# MAGIC
# MAGIC A distribuição horária revela o padrão de ocupação ao longo do dia.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT pickup_hour, ROUND(media_passageiros, 4) AS media_passageiros
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
# MAGIC A média isolada pode enganar: uma hora com poucas corridas produz média
# MAGIC mais volátil. Cruzar com o volume mostra onde a métrica é mais confiável.

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