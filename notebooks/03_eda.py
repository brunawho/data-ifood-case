# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Análise Exploratória (EDA)
# MAGIC
# MAGIC **Etapa 3 de 5.** Investigação da bronze para dimensionar os problemas de
# MAGIC qualidade antes de decidir como tratá-los.
# MAGIC
# MAGIC Este notebook não transforma nada, apenas mede. Cada seção termina com uma
# MAGIC decisão justificada pelo número que a query devolveu: uma regra que
# MAGIC descarta 0,001% da base e uma que descarta 10% exigem níveis muito
# MAGIC diferentes de justificativa.
# MAGIC
# MAGIC **Pré-requisito:** `02_bronze` executado com os cinco meses.
# MAGIC
# MAGIC > Fica fora de qualquer execução automatizada: EDA é investigação humana
# MAGIC > que fundamenta o pipeline, não etapa dele.

# COMMAND ----------

# MAGIC %run ./_setup

# COMMAND ----------

mostrar_configuracao()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Panorama
# MAGIC
# MAGIC Ponto de partida: quanto dado existe e qual o escopo temporal real.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)                                    AS registros,
# MAGIC        COUNT(DISTINCT _ref_period)                 AS competencias,
# MAGIC        MIN(tpep_pickup_datetime)                   AS pickup_minimo,
# MAGIC        MAX(tpep_pickup_datetime)                   AS pickup_maximo,
# MAGIC        COUNT(DISTINCT date_format(tpep_pickup_datetime, 'yyyy-MM')) AS meses_distintos
# MAGIC FROM workspace.bronze.yellow_tripdata

# COMMAND ----------

# MAGIC %md
# MAGIC O contraste entre `competencias` (5 arquivos) e `meses_distintos` é a
# MAGIC primeira evidência do problema: se houvesse correspondência perfeita entre
# MAGIC arquivo e mês, os dois números seriam iguais.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1. Integridade temporal
# MAGIC
# MAGIC Já sabemos que existem corridas fora do mês de referência, nas duas
# MAGIC direções, de 2001 até setembro/2023. Falta dimensionar o quadro completo.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Onde as corridas realmente caem
# MAGIC
# MAGIC Agrupando pela data real do *pickup*, e não pelo arquivo de origem.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT date_format(tpep_pickup_datetime, 'yyyy-MM') AS mes_real,
# MAGIC        COUNT(*)                                     AS registros,
# MAGIC        COLLECT_SET(_ref_period)                     AS arquivos_de_origem
# MAGIC FROM workspace.bronze.yellow_tripdata
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------

# MAGIC %md
# MAGIC Meses fora da janela jan–mai/2023 aparecem com contagem baixíssima: são os
# MAGIC registros corrompidos. Os cinco meses do escopo concentram praticamente
# MAGIC toda a base.
# MAGIC
# MAGIC A coluna `arquivos_de_origem` confirma que a corrida de 1º de fevereiro
# MAGIC no arquivo de janeiro é artefato de fronteira, não corrupção.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Duração da corrida
# MAGIC
# MAGIC Corrida com *dropoff* anterior ao *pickup* tem duração negativa, é
# MAGIC impossível. Duração zero e durações absurdas também merecem atenção.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH duracoes AS (
# MAGIC   SELECT timestampdiff(SECOND, tpep_pickup_datetime, tpep_dropoff_datetime) AS segundos
# MAGIC   FROM workspace.bronze.yellow_tripdata
# MAGIC )
# MAGIC SELECT
# MAGIC   COUNT(*)                                                      AS total,
# MAGIC   SUM(CASE WHEN segundos IS NULL   THEN 1 ELSE 0 END)           AS sem_duracao,
# MAGIC   SUM(CASE WHEN segundos <  0      THEN 1 ELSE 0 END)           AS duracao_negativa,
# MAGIC   SUM(CASE WHEN segundos =  0      THEN 1 ELSE 0 END)           AS duracao_zero,
# MAGIC   SUM(CASE WHEN segundos > 86400   THEN 1 ELSE 0 END)           AS acima_de_24h,
# MAGIC   ROUND(AVG(segundos) / 60, 1)                                  AS media_minutos,
# MAGIC   ROUND(percentile_approx(segundos, 0.5) / 60, 1)               AS mediana_minutos,
# MAGIC   ROUND(MAX(segundos) / 3600, 1)                                AS maximo_horas
# MAGIC FROM duracoes

# COMMAND ----------

# MAGIC %md
# MAGIC Média muito maior que a mediana indica outliers puxando o resultado, que
# MAGIC é exatamente o risco da pergunta 1 do case.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. `total_amount`, a coluna da pergunta 1
# MAGIC
# MAGIC O case pede a média de `total_amount` por mês. Antes de calcular qualquer
# MAGIC média é preciso saber o que existe nessa coluna.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Valores não-positivos
# MAGIC
# MAGIC Valores negativos nesta base costumam ser **estornos e ajustes contábeis**:
# MAGIC a TLC registra a correção como uma linha de valor negativo, e não removendo
# MAGIC a corrida original. Não são erro de leitura, e sim semântica do dado.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*)                                                   AS total,
# MAGIC   SUM(CASE WHEN total_amount IS NULL THEN 1 ELSE 0 END)      AS nulos,
# MAGIC   SUM(CASE WHEN total_amount <  0    THEN 1 ELSE 0 END)      AS negativos,
# MAGIC   SUM(CASE WHEN total_amount =  0    THEN 1 ELSE 0 END)      AS zerados,
# MAGIC   ROUND(100.0 * SUM(CASE WHEN total_amount <= 0 THEN 1 ELSE 0 END) / COUNT(*), 4)
# MAGIC                                                              AS pct_nao_positivos,
# MAGIC   ROUND(SUM(CASE WHEN total_amount < 0 THEN total_amount ELSE 0 END), 2)
# MAGIC                                                              AS soma_dos_negativos
# MAGIC FROM workspace.bronze.yellow_tripdata

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Distribuição e outliers
# MAGIC
# MAGIC Percentis revelam a forma da distribuição melhor que média e desvio padrão,
# MAGIC porque não são afetados por valores extremos.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   ROUND(MIN(total_amount), 2)                            AS minimo,
# MAGIC   ROUND(percentile_approx(total_amount, 0.01), 2)        AS p01,
# MAGIC   ROUND(percentile_approx(total_amount, 0.25), 2)        AS p25,
# MAGIC   ROUND(percentile_approx(total_amount, 0.50), 2)        AS mediana,
# MAGIC   ROUND(AVG(total_amount), 2)                            AS media,
# MAGIC   ROUND(percentile_approx(total_amount, 0.75), 2)        AS p75,
# MAGIC   ROUND(percentile_approx(total_amount, 0.99), 2)        AS p99,
# MAGIC   ROUND(percentile_approx(total_amount, 0.9999), 2)      AS p9999,
# MAGIC   ROUND(MAX(total_amount), 2)                            AS maximo
# MAGIC FROM workspace.bronze.yellow_tripdata

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Quanto o extremo pesa na média
# MAGIC
# MAGIC A pergunta prática: o outlier distorce a resposta o suficiente para
# MAGIC justificar removê-lo? Esta query compara a média sob diferentes recortes.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   ROUND(AVG(total_amount), 4)                                              AS media_bruta,
# MAGIC   ROUND(AVG(CASE WHEN total_amount > 0 THEN total_amount END), 4)          AS media_positivos,
# MAGIC   ROUND(AVG(CASE WHEN total_amount BETWEEN 0.01 AND 1000 THEN total_amount END), 4)
# MAGIC                                                                            AS media_ate_1000,
# MAGIC   COUNT(CASE WHEN total_amount > 1000 THEN 1 END)                          AS acima_de_1000
# MAGIC FROM workspace.bronze.yellow_tripdata

# COMMAND ----------

# MAGIC %md
# MAGIC Se as três médias forem próximas, os extremos não distorcem o resultado e o
# MAGIC tratamento pode ser conservador. Se divergirem, a escolha do filtro precisa
# MAGIC ser justificada com cuidado e documentada, porque muda a resposta final.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. `passenger_count`, a coluna da pergunta 2
# MAGIC
# MAGIC A pergunta 2 pede a média de passageiros por hora do dia em maio. Nulo e
# MAGIC zero têm tratamentos diferentes e mudam a resposta.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   passenger_count,
# MAGIC   COUNT(*)                                        AS registros,
# MAGIC   ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 4) AS pct
# MAGIC FROM workspace.bronze.yellow_tripdata
# MAGIC GROUP BY passenger_count
# MAGIC ORDER BY passenger_count NULLS FIRST

# COMMAND ----------

# MAGIC %md
# MAGIC Três categorias exigem decisão:
# MAGIC
# MAGIC - **`NULL`**: o taxímetro não registrou. Ausência de informação, não zero
# MAGIC   passageiro. Incluir como zero puxaria a média para baixo.
# MAGIC - **`0`**: erro de digitação do motorista ou corrida cancelada.
# MAGIC - **7, 8, 9**: acima da capacidade legal de um táxi de NY (4 passageiros,
# MAGIC   5 em minivans).

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 O impacto na resposta da pergunta 2
# MAGIC
# MAGIC Comparação direta entre os tratamentos possíveis, restrita a maio, que é
# MAGIC o escopo da pergunta.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   ROUND(AVG(passenger_count), 4)                                       AS media_ignorando_nulos,
# MAGIC   ROUND(AVG(COALESCE(passenger_count, 0)), 4)                          AS media_nulo_como_zero,
# MAGIC   ROUND(AVG(CASE WHEN passenger_count > 0 THEN passenger_count END), 4) AS media_so_positivos,
# MAGIC   COUNT(*)                                                             AS corridas_maio
# MAGIC FROM workspace.bronze.yellow_tripdata
# MAGIC WHERE date_format(tpep_pickup_datetime, 'yyyy-MM') = '2023-05'

# COMMAND ----------

# MAGIC %md
# MAGIC A diferença entre essas médias é a medida exata de quanto a escolha de
# MAGIC tratamento afeta a resposta entregue ao case. Seja qual for a decisão, ela
# MAGIC precisa estar declarada.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4. Duplicatas
# MAGIC
# MAGIC A origem não fornece chave primária. Uma corrida é razoavelmente
# MAGIC identificada pela combinação de fornecedor, horários e locais; registros
# MAGIC idênticos nesses campos são candidatos a duplicata.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH candidatas AS (
# MAGIC   SELECT VendorID, tpep_pickup_datetime, tpep_dropoff_datetime,
# MAGIC          PULocationID, DOLocationID, total_amount,
# MAGIC          COUNT(*) AS ocorrencias
# MAGIC   FROM workspace.bronze.yellow_tripdata
# MAGIC   GROUP BY ALL
# MAGIC   HAVING COUNT(*) > 1
# MAGIC )
# MAGIC SELECT COUNT(*)                    AS combinacoes_repetidas,
# MAGIC        SUM(ocorrencias)            AS registros_envolvidos,
# MAGIC        SUM(ocorrencias - 1)        AS excedentes,
# MAGIC        MAX(ocorrencias)            AS maior_repeticao
# MAGIC FROM candidatas

# COMMAND ----------

# MAGIC %md
# MAGIC **Cuidado na interpretação:** duas corridas podem coincidir legitimamente
# MAGIC em todos esses campos. É improvável, mas com 16 milhões de registros o
# MAGIC improvável acontece. Sem chave primária na origem não há como distinguir
# MAGIC duplicata real de coincidência, então volume baixo justifica manter.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5. Completude das colunas obrigatórias
# MAGIC
# MAGIC O case exige `VendorID`, `passenger_count`, `total_amount`,
# MAGIC `tpep_pickup_datetime` e `tpep_dropoff_datetime` na camada de consumo.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*)                                                            AS total,
# MAGIC   SUM(CASE WHEN VendorID              IS NULL THEN 1 ELSE 0 END)      AS vendor_nulo,
# MAGIC   SUM(CASE WHEN tpep_pickup_datetime  IS NULL THEN 1 ELSE 0 END)      AS pickup_nulo,
# MAGIC   SUM(CASE WHEN tpep_dropoff_datetime IS NULL THEN 1 ELSE 0 END)      AS dropoff_nulo,
# MAGIC   SUM(CASE WHEN passenger_count       IS NULL THEN 1 ELSE 0 END)      AS passageiros_nulo,
# MAGIC   SUM(CASE WHEN total_amount          IS NULL THEN 1 ELSE 0 END)      AS valor_nulo
# MAGIC FROM workspace.bronze.yellow_tripdata

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 `VendorID`: domínio esperado
# MAGIC
# MAGIC O dicionário de dados da TLC define apenas dois fornecedores: `1` (Creative
# MAGIC Mobile Technologies) e `2` (VeriFone). Qualquer outro valor está fora do
# MAGIC domínio documentado.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT VendorID, COUNT(*) AS registros
# MAGIC FROM workspace.bronze.yellow_tripdata
# MAGIC GROUP BY VendorID
# MAGIC ORDER BY registros DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 6. A ambiguidade da pergunta 1
# MAGIC
# MAGIC > *"Qual a média de valor total (`total_amount`) recebido em um mês
# MAGIC > considerando todos os yellow táxis da frota?"*
# MAGIC
# MAGIC O enunciado admite duas leituras, com respostas em ordens de grandeza
# MAGIC completamente diferentes:
# MAGIC
# MAGIC **(a) Ticket médio por corrida, agrupado por mês.** Resultado na casa das
# MAGIC dezenas de dólares.
# MAGIC
# MAGIC **(b) Faturamento médio mensal da frota.** Resultado na casa das dezenas
# MAGIC de milhões.
# MAGIC
# MAGIC A query abaixo calcula as duas. A entrega final apresenta ambas com a
# MAGIC leitura explicitada, porque escolher uma silenciosamente esconderia uma
# MAGIC decisão que muda o resultado por seis ordens de grandeza.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH por_mes AS (
# MAGIC   SELECT date_format(tpep_pickup_datetime, 'yyyy-MM') AS mes,
# MAGIC          SUM(total_amount)   AS faturamento,
# MAGIC          AVG(total_amount)   AS ticket_medio,
# MAGIC          COUNT(*)            AS corridas
# MAGIC   FROM workspace.bronze.yellow_tripdata
# MAGIC   WHERE date_format(tpep_pickup_datetime, 'yyyy-MM') BETWEEN '2023-01' AND '2023-05'
# MAGIC   GROUP BY 1
# MAGIC )
# MAGIC SELECT mes,
# MAGIC        corridas,
# MAGIC        ROUND(ticket_medio, 2) AS ticket_medio_leitura_a,
# MAGIC        ROUND(faturamento, 2)  AS faturamento_do_mes
# MAGIC FROM por_mes
# MAGIC UNION ALL
# MAGIC SELECT 'MÉDIA DOS MESES',
# MAGIC        ROUND(AVG(corridas)),
# MAGIC        ROUND(AVG(ticket_medio), 2),
# MAGIC        ROUND(AVG(faturamento), 2)
# MAGIC FROM por_mes
# MAGIC ORDER BY mes

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Síntese: regras para a silver
# MAGIC
# MAGIC | # | Achado | Volume | % da base | Decisão |
# MAGIC |---|---|---|---|---|
# MAGIC | 1 | Corridas fora de jan–mai/2023 | 104 | 0,00064% | Descartar |
# MAGIC | 2 | Duração ≤ 0 | 6.181 | 0,0382% | Descartar |
# MAGIC | 3 | Duração acima de 24h | 94 | 0,0006% | Manter, sinalizar |
# MAGIC | 4 | `total_amount` negativo (estorno) | 141.407 | 0,8736% | Manter, sinalizar |
# MAGIC | 5 | `total_amount` zero | 2.739 | 0,0169% | Manter, sinalizar |
# MAGIC | 6 | `total_amount` extremo (acima de US$ 1.000) | 11 | 0,00007% | Manter, sem corte |
# MAGIC | 7 | `passenger_count` nulo | 428.665 | 2,6483% | Manter; `AVG` ignora |
# MAGIC | 8 | `passenger_count` zero | 273.481 | 1,6896% | Manter, sinalizar |
# MAGIC | 9 | `passenger_count` 7–9 | 112 | 0,0007% | Manter, sinalizar |
# MAGIC | 10 | Duplicatas aparentes | 2 | ~0% | Manter |
# MAGIC | 11 | `VendorID` = 6 | 3.983 | 0,0246% | Manter, sinalizar |
# MAGIC
# MAGIC Total descartado: **6.284 registros (0,039%)**. A soma das linhas 1 e 2 dá
# MAGIC 6.285 porque um registro apresenta os dois problemas e é classificado
# MAGIC apenas pelo primeiro critério.
# MAGIC
# MAGIC A silver preserva a granularidade da corrida e descarta apenas o
# MAGIC comprovadamente inválido. Decisões de métrica, como excluir estorno de uma
# MAGIC média, pertencem à gold: assim a camada de consumo continua servindo
# MAGIC perguntas que ainda não foram feitas.
# MAGIC
# MAGIC Justificativa detalhada de cada decisão em `docs/achados-eda.md`.
# MAGIC
# MAGIC Próximo: **`04_silver`**.