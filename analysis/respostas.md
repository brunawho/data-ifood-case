# Respostas às perguntas do case

Resultados obtidos sobre a camada silver (`workspace.silver.fact_yellow_trips`),
com 16.180.102 corridas de *yellow taxi* entre janeiro e maio de 2023.

As consultas que produzem estes números estão em
[`notebooks/05_analise.py`](../notebooks/05_analise.py), em SQL e PySpark, com
verificação automática de que as duas implementações concordam.

---

## Pergunta 1

> *Qual a média de valor total (`total_amount`) recebido em um mês considerando
> todos os yellow táxis da frota?*

### O enunciado admite duas leituras

"Média de valor total recebido em um mês" pode significar:

- **(a)** o valor médio de uma corrida, agrupado por mês — ordem de grandeza:
  dezenas de dólares;
- **(b)** o valor médio arrecadado pela frota inteira em um mês — ordem de
  grandeza: dezenas de milhões de dólares.

As duas são respondidas abaixo. Escolher uma silenciosamente esconderia uma
decisão que altera o resultado por um fator de milhões.

### Tratamento de estornos

A base contém 143.792 lançamentos com `total_amount` ≤ 0. Não são erro: a TLC
registra estornos e ajustes contábeis como linhas de valor negativo, mantendo a
corrida original. São preservados na camada silver.

Para a métrica de valor recebido por corrida, porém, um estorno não é uma
corrida. Os números abaixo apresentam as duas versões, e a versão **sem
estornos** é a adotada como resposta.

### Resultado por competência

| Competência | Corridas | Ticket médio (bruto) | Ticket médio (sem estornos) | Faturamento (bruto) | Faturamento (sem estornos) | Estornos |
|---|---|---|---|---|---|---|
| 2023-01 | 3.065.605 | US$ 27,02 | **US$ 27,45** | US$ 82.835.941,98 | **US$ 83.440.925,19** | 25.694 |
| 2023-02 | 2.912.738 | US$ 26,90 | **US$ 27,34** | US$ 78.341.799,94 | **US$ 78.935.054,89** | 25.392 |
| 2023-03 | 3.402.207 | US$ 27,80 | **US$ 28,27** | US$ 94.584.847,02 | **US$ 95.313.833,75** | 30.296 |
| 2023-04 | 3.287.076 | US$ 28,27 | **US$ 28,76** | US$ 92.921.264,98 | **US$ 93.674.019,97** | 30.205 |
| 2023-05 | 3.512.476 | US$ 28,96 | **US$ 29,46** | US$ 101.730.574,60 | **US$ 102.538.638,06** | 32.205 |

### Resposta

**Leitura (a) — ticket médio por corrida**

| Cálculo | Valor |
|---|---|
| Média simples das cinco competências | **US$ 28,26** |
| Média ponderada pelo volume de corridas | **US$ 28,30** |

A distinção não é preciosismo: a média simples atribui peso igual a cada mês,
ignorando que maio teve 20,6% mais corridas que fevereiro. A ponderada divide o
faturamento total pelo número total de corridas. As duas estão corretas e
respondem a perguntas diferentes — a proximidade entre elas (4 centavos) indica
que as competências são homogêneas entre si.

**Leitura (b) — faturamento mensal da frota**

| Métrica | Valor |
|---|---|
| **Faturamento médio mensal** | **US$ 90.780.494,37** |
| Menor mês (fevereiro) | US$ 78.935.054,89 |
| Maior mês (maio) | US$ 102.538.638,06 |
| Total no período | US$ 453.902.471,86 |

### Observação: o crescimento no período

Maio faturou **29,9% mais que fevereiro**. O crescimento vem de dois efeitos
somados, e vale distingui-los:

- **Volume** — 3,51 milhões de corridas em maio contra 2,91 milhões em fevereiro
  (+20,6%). Fevereiro tem 28 dias, e o inverno de Nova York reduz a circulação.
- **Ticket** — US$ 29,46 contra US$ 27,34 (+7,8%). O valor médio da corrida
  também subiu, o que não se explica por sazonalidade de calendário.

O ticket médio cresce de forma monotônica ao longo dos cinco meses, sugerindo
tendência e não apenas variação sazonal. Confirmar exigiria uma série histórica
mais longa que a do escopo deste case.

---

## Pergunta 2

> *Qual a média de passageiros (`passenger_count`) por cada hora do dia que
> pegaram táxi no mês de maio considerando todos os táxis da frota?*

### Tratamento de valores ausentes

427.771 corridas na base não têm `passenger_count` registrado — o taxímetro não
capturou a informação. **Nulo não é zero:** significa ausência de registro, não
corrida sem passageiro. Substituir por zero puxaria a média para baixo
artificialmente.

O comportamento nativo do `AVG` — ignorar nulos — é o correto e é o adotado. As
alternativas são exibidas na tabela para tornar a decisão auditável.

### Resultado — maio/2023

| Hora | Corridas | **Média de passageiros** | Nulo como zero | Apenas positivos | Sem registro |
|---|---|---|---|---|---|
| 00h | 94.043 | **1,4109** | 1,3618 | 1,4269 | 3.274 |
| 01h | 61.322 | **1,4204** | 1,3671 | 1,4362 | 2.304 |
| 02h | 39.675 | **1,4367** | 1,3781 | 1,4542 | 1.616 |
| 03h | 26.076 | **1,4355** | 1,3668 | 1,4508 | 1.249 |
| 04h | 18.018 | **1,3885** | 1,2564 | 1,4037 | 1.715 |
| 05h | 20.290 | **1,2649** | 1,1685 | 1,2836 | 1.547 |
| 06h | 49.464 | **1,2348** | 1,1694 | 1,2610 | 2.619 |
| 07h | 99.551 | **1,2525** | 1,1883 | 1,2816 | 5.103 |
| 08h | 135.500 | **1,2658** | 1,2064 | 1,2954 | 6.365 |
| 09h | 150.125 | **1,2833** | 1,2389 | 1,3118 | 5.199 |
| 10h | 162.416 | **1,3187** | 1,2832 | 1,3472 | 4.375 |
| 11h | 176.497 | **1,3336** | 1,3007 | 1,3618 | 4.354 |
| 12h | 190.181 | **1,3483** | 1,3154 | 1,3756 | 4.635 |
| 13h | 194.754 | **1,3560** | 1,3235 | 1,3848 | 4.664 |
| 14h | 211.863 | **1,3612** | 1,3280 | 1,3898 | 5.168 |
| 15h | 216.478 | **1,3735** | 1,3393 | 1,4017 | 5.377 |
| 16h | 216.328 | **1,3723** | 1,3383 | 1,3987 | 5.362 |
| 17h | 236.126 | **1,3651** | 1,3305 | 1,3899 | 5.980 |
| 18h | 250.413 | **1,3604** | 1,3270 | 1,3837 | 6.134 |
| 19h | 224.233 | **1,3703** | 1,3398 | 1,3925 | 5.000 |
| 20h | 198.769 | **1,3814** | 1,3518 | 1,4010 | 4.255 |
| 21h | 203.255 | **1,4020** | 1,3684 | 1,4195 | 4.864 |
| 22h | 188.972 | **1,4109** | 1,3692 | 1,4272 | 5.590 |
| 23h | 148.127 | **1,4066** | 1,3595 | 1,4221 | 4.954 |

*Total: 3.512.476 corridas — todas as corridas de maio na camada silver.*

### O que os números mostram

**Máximo às 2h (1,4367). Mínimo às 6h (1,2348).** A amplitude é de 16,4% entre
os extremos.

O padrão tem três fases bem definidas:

- **Madrugada (0h–3h)** — a maior ocupação do dia. Compatível com vida noturna:
  grupos retornando juntos de bares, restaurantes e eventos.
- **Vale (5h–8h)** — a menor ocupação. Compatível com deslocamento individual
  para o trabalho.
- **Recuperação progressiva (9h–23h)** — a média sobe de forma contínua ao longo
  do dia, com platô à tarde e retomada da alta à noite, quando volta o uso
  social.

### Observação: ocupação e volume são inversamente relacionados

As horas de maior ocupação são as de menor movimento:

| | Hora de pico | Valor |
|---|---|---|
| Maior média de passageiros | 02h | 1,4367 (39.675 corridas) |
| Maior volume de corridas | 18h | 250.413 corridas (1,3604) |
| Menor média de passageiros | 06h | 1,2348 (49.464 corridas) |
| Menor volume de corridas | 04h | 18.018 corridas (1,3885) |

Quando a cidade mais usa táxi, usa sozinha. O pico de demanda das 18h é
individual e pendular; o de ocupação da madrugada é social e compartilhado.

### Observação: qualidade do dado varia com a hora

A proporção de corridas sem `passenger_count` registrado não é uniforme:

| Hora | Sem registro | % das corridas |
|---|---|---|
| 04h | 1.715 | **9,52%** |
| 05h | 1.547 | 7,62% |
| 06h | 2.619 | 5,29% |
| 12h | 4.635 | 2,44% |
| 18h | 6.134 | 2,45% |

A madrugada tem quase **quatro vezes mais** ausência de registro que o horário
comercial. Isso importa para a interpretação: são justamente as horas de maior
ocupação aparente que têm a base amostral menos completa.

### Ressalva metodológica

Os números agregam maio inteiro, sem distinguir dias úteis de fins de semana. O
pico da madrugada provavelmente concentra-se em sextas e sábados, e a média de
uma terça de madrugada deve ser bem inferior à apresentada. O case não solicitou
esse recorte, mas a coluna `pickup_day_of_week` está disponível na camada silver
para quem quiser aprofundar.

---

## Reprodutibilidade

Todos os números acima são reproduzíveis executando os notebooks na ordem:

```
01_landing → 02_bronze → 04_silver → 05_analise
```

O notebook `03_eda` é opcional: documenta a investigação que fundamentou as
regras de limpeza, cujos achados estão consolidados em
[`docs/achados-eda.md`](../docs/achados-eda.md).
