# Achados da Análise Exploratória

Consolidação dos resultados do notebook `03_eda`, executado sobre os cinco meses
de *yellow taxi* (jan–mai/2023) na camada bronze.

**Base analisada:** 16.186.386 registros, 5 arquivos de origem.

Cada regra da camada silver está ancorada em um número medido. Nenhuma decisão
de limpeza foi tomada por suposição.

---

## 1. Integridade temporal

### Escopo real dos dados

Os cinco arquivos mensais contêm corridas de **17 meses distintos**, indo de
janeiro/2001 a setembro/2023. A sujeira ocorre nas duas direções, passado e
futuro em relação ao arquivo.

| Métrica | Valor |
|---|---|
| Registros na janela jan–mai/2023 | 16.186.282 |
| Registros fora da janela | **104** (0,00064%) |
| *Pickup* mínimo | 2001-01-01 00:06:49 |
| *Pickup* máximo | 2023-09-05 18:20:48 |

Duas categorias distintas foram observadas e **não devem ser confundidas**:

- **Corrupção**: datas de 2001, 2002, 2008 e setembro/2023. São 104 registros.
- **Fronteira legítima**: corridas iniciadas logo após a virada do mês e
  transmitidas com o lote anterior (ex.: 2023-02-01 00:56 no arquivo de janeiro).
  Não são erro: pertencem ao mês seguinte.

Contando "fora do mês do arquivo" o total seria 437 registros, número que mistura
as duas categorias. O critério correto é a **janela do escopo**, não o arquivo.

> **Decisão:** filtrar por `tpep_pickup_datetime` dentro de jan–mai/2023, e
> derivar a competência da data real da corrida, nunca do arquivo de origem.

### Duração da corrida

| Situação | Registros | % |
|---|---|---|
| Duração negativa (*dropoff* < *pickup*) | 795 | 0,0049% |
| Duração zero | 5.386 | 0,0333% |
| Acima de 24h | 94 | 0,0006% |
| Duração média | 16,8 min | |
| Duração mediana | 12,2 min | |
| Duração máxima | 167,2 h (~7 dias) | |

> **Decisão:** descartar duração ≤ 0 (6.181 registros, 0,038%), não é corrida.
> Manter as acima de 24h: são implausíveis, mas não impossíveis (taxímetro
> esquecido ligado), e não há critério objetivo para descartá-las.

---

## 2. `total_amount` — coluna da pergunta 1

### Valores não-positivos

| Situação | Registros | % |
|---|---|---|
| Nulos | 0 | 0% |
| Negativos | 141.407 | 0,8736% |
| Zerados | 2.739 | 0,0169% |
| **Não-positivos** | **144.146** | **0,8905%** |
| Soma dos negativos | −US$ 3.488.304,77 | |

Os valores não-positivos podem representar reversões, ajustes contábeis ou
outros eventos operacionais. **A base não permite determinar sua natureza com
segurança:** não há campo que identifique reversão nem chave que permita parear
um lançamento negativo com a corrida que ele corrigiria.

> **Decisão:** preservar na silver e sinalizar com `flag_valor_nao_positivo`.
> Descartá-los na camada de consumo destruiria informação cuja natureza não foi
> determinada. A métrica literal, que é a resposta oficial do case, inclui todos
> os registros; a versão que os exclui é apresentada apenas como análise de
> sensibilidade, com a interpretação declarada.

### Distribuição

| Percentil | Valor (US$) |
|---|---|
| Mínimo | −982,95 |
| p01 | 4,50 |
| p25 | 15,70 |
| **Mediana** | **20,60** |
| **Média** | **27,84** |
| p75 | 29,80 |
| p99 | 102,71 |
| Máximo | 6.304,90 |

Média acima da mediana indica assimetria à direita, esperada em dados de tarifa.

### Outliers não requerem tratamento

| Recorte | Média |
|---|---|
| Bruta (tudo) | 27,8385 |
| Apenas positivos | 28,3061 |
| Positivos até US$ 1.000 | 28,3048 |
| Corridas acima de US$ 1.000 | **11 registros** |

A diferença entre "positivos" e "positivos até 1.000" é de **0,0013**, quarta
casa decimal. Onze corridas extremas em 16,2 milhões não movem a média.

> **Decisão:** **não aplicar corte de outlier.** Um teto arbitrário seria
> intervenção sem efeito mensurável, com o custo de descartar dados possivelmente
> válidos. A diferença relevante (27,84 → 28,31, cerca de 1,7%) vem dos não-positivos,
> não dos extremos.

*Ressalva metodológica:* `percentile_approx` no percentil 0,9999 retornou valor
idêntico ao máximo, indicando perda de precisão da aproximação na cauda. Percentis
extremos não devem ser citados como exatos.

---

## 3. `passenger_count` — coluna da pergunta 2

| Passageiros | Registros | % |
|---|---|---|
| `NULL` | 428.665 | 2,6483% |
| 0 | 273.481 | 1,6896% |
| 1 | 11.894.120 | 73,4822% |
| 2 | 2.356.679 | 14,5596% |
| 3 | 577.606 | 3,5685% |
| 4 | 300.125 | 1,8542% |
| 5 | 217.068 | 1,3411% |
| 6 | 138.530 | 0,8558% |
| 7 | 28 | 0,0002% |
| 8 | 65 | 0,0004% |
| 9 | 19 | 0,0001% |

Valores 1 a 6 são plausíveis: táxis de NY comportam 4 passageiros, e minivans
autorizadas comportam 5, com criança de colo permitida adicionalmente. Os
valores 7 a 9 (112 registros no total) excedem qualquer configuração legal.

### Impacto na resposta (maio/2023, 3.513.645 corridas)

| Tratamento | Média |
|---|---|
| Ignorando nulos (`AVG` nativo) | 1,3588 |
| Nulo tratado como zero | 1,3194 |
| Apenas positivos | 1,3830 |

A escolha desloca a resposta em até 4,8%.

> **Decisão:** `NULL` significa ausência de registro, não zero passageiro;
> tratar como zero introduziria viés para baixo. Adota-se o comportamento nativo
> do `AVG`, que ignora nulos.
>
> **Com uma ressalva:** a taxa de ausência varia por hora (2,4% às 12h contra
> 9,5% às 4h). Como a média é calculada por hora, essa variação não implica viés
> por si só — o agrupamento já separa os grupos. A condição relevante seria a
> ausência ser independente do número de passageiros *dentro* de cada hora, e
> isso não é verificável com as colunas disponíveis. O viés permanece
> desconhecido, e é declarado como tal.
> Registros com `passenger_count = 0` são preservados na silver e sinalizados,
> por não haver como distinguir erro de digitação de corrida cancelada.

---

## 4. Duplicatas

A origem não fornece chave primária. Usando a combinação de `VendorID`, horários,
zonas e valor como identificador aproximado:

| Métrica | Valor |
|---|---|
| Combinações repetidas | 2 |
| Registros envolvidos | 4 |
| Excedentes | 2 |
| Maior repetição | 2 |

> **Decisão:** manter. Dois registros excedentes em 16,2 milhões estão dentro do
> que se espera por coincidência legítima, dois táxis do mesmo fornecedor
> partindo da mesma zona para a mesma zona, no mesmo instante, com a mesma
> tarifa. Sem chave primária, não há como distinguir duplicata de coincidência,
> e o volume não justifica a remoção.

---

## 5. Completude e domínio

Nenhum nulo nas colunas críticas:

| Coluna | Nulos |
|---|---|
| `VendorID` | 0 |
| `tpep_pickup_datetime` | 0 |
| `tpep_dropoff_datetime` | 0 |
| `total_amount` | 0 |
| `passenger_count` | 428.665 |

### `VendorID` fora do dicionário consultado

| VendorID | Registros |
|---|---|
| 2 | 11.809.794 |
| 1 | 4.372.609 |
| **6** | **3.983** |

O dicionário de dados da TLC consultado para os arquivos de 2023 lista apenas os
fornecedores 1 (Creative Mobile Technologies) e 2 (VeriFone). O valor 6 aparece
em 3.983 registros sem correspondência nessa versão.

**Isso não significa que o valor seja inválido.** O domínio pode ter evoluído
sem que a versão consultada refletisse a mudança, e não há como distinguir, com
o material disponível, um código novo de um erro de gravação.

> **Decisão:** manter e sinalizar como anomalia a investigar. Ausência no
> dicionário consultado não implica corrida inválida, e as demais colunas desses
> registros são consistentes. Sinalizar preserva a informação e a torna visível a
> quem consome, sem afirmar que o valor está errado.

#### Achado adicional: o fornecedor 6 concentra desproporcionalmente os descartes

Comparando bronze e silver após a aplicação das regras:

| | Bronze | Silver | Descartados |
|---|---|---|---|
| `VendorID = 6` | 3.983 | 3.209 | **774** |

Os 774 registros descartados do fornecedor 6 representam **12,3% de todo o
descarte** (6.284 registros), embora esse fornecedor responda por apenas 0,025%
da base.

Em taxa: **19,4%** dos registros do fornecedor 6 foram descartados por invalidez,
contra **0,039%** da base geral, cerca de 500 vezes mais.

A lacuna de dicionário, portanto, coincide com qualidade de dados
mensuravelmente inferior. Isso reforça a decisão de sinalizar em vez de ignorar:
quem exija alta confiabilidade pode excluir esses registros conscientemente.

Aprofundar exigiria cruzar `VendorID = 6` com `payment_type`, horários e padrões
de valor, para verificar se corresponde a um perfil operacional distinto — como
viagens anuladas — ou a falha de gravação. Não foi feito neste case.

---

## 6. Ambiguidade da pergunta 1

> *"Qual a média de valor total (`total_amount`) recebido em um mês considerando
> todos os yellow táxis da frota?"*

O enunciado admite duas leituras com respostas separadas por seis ordens de
grandeza. Valores calculados sobre a bronze (sem limpeza), agrupados pela data
real da corrida:

| Competência | Corridas | Ticket médio | Faturamento |
|---|---|---|---|
| 2023-01 | 3.066.726 | US$ 27,02 | US$ 82.863.594,11 |
| 2023-02 | 2.914.003 | US$ 26,90 | US$ 78.381.600,43 |
| 2023-03 | 3.403.660 | US$ 27,80 | US$ 94.634.027,66 |
| 2023-04 | 3.288.248 | US$ 28,27 | US$ 92.956.843,11 |
| 2023-05 | 3.513.645 | US$ 28,96 | US$ 101.765.282,92 |
| **Média dos meses** | **3.237.256** | **US$ 27,79** | **US$ 90.120.269,65** |

> **Estes números são da bronze, não da resposta final.** Servem para dimensionar
> a ambiguidade do enunciado nesta etapa exploratória. A resposta entregue é
> calculada sobre a silver, após o descarte dos 6.284 registros inválidos, e está
> em [`analysis/respostas.md`](../analysis/respostas.md): ticket médio de
> US$ 27,79 e faturamento médio mensal de US$ 90.082.885,70. O ticket coincide
> por arredondamento; o faturamento não, porque os registros descartados
> contribuíam com valor.

### Média simples e média ponderada

A média das cinco médias mensais é **US$ 27,79**, enquanto a média sobre todas as
corridas é **US$ 27,84**. A diferença não é erro: a primeira atribui peso igual a
cada mês, ignorando que maio teve 20% mais corridas que fevereiro; a segunda
pondera pelo volume real.

> **Decisão:** apresentar as duas leituras na entrega final, com a interpretação
> explicitada. Escolher uma silenciosamente esconderia uma decisão que altera a
> resposta em seis ordens de grandeza.

---

## Síntese das regras da silver

| # | Achado | Volume | % da base | Decisão |
|---|---|---|---|---|
| 1 | Corridas fora de jan–mai/2023 | 104 | 0,00064% | Descartar |
| 2 | Duração ≤ 0 | 6.181 | 0,0382% | Descartar |
| 3 | Duração acima de 24h | 94 | 0,0006% | Manter, sinalizar |
| 4 | `total_amount` negativo | 141.407 | 0,8736% | Manter, sinalizar; exclusão só em sensibilidade |
| 5 | `total_amount` zero | 2.739 | 0,0169% | Manter, sinalizar; exclusão só em sensibilidade |
| 6 | `total_amount` extremo | 11 | 0,00007% | Manter — sem efeito mensurável |
| 7 | `passenger_count` nulo | 428.665 | 2,6483% | Manter; `AVG` ignora nativamente |
| 8 | `passenger_count` zero | 273.481 | 1,6896% | Manter, sinalizar |
| 9 | `passenger_count` 7–9 | 112 | 0,0007% | Manter, sinalizar |
| 10 | Duplicatas aparentes | 2 | ~0% | Manter |
| 11 | `VendorID` = 6 | 3.983 | 0,0246% | Manter, sinalizar |

**Volume total descartado: 6.284 registros (0,039%).**

*Nota sobre a contagem:* somando as categorias isoladamente chega-se a 6.285.
A diferença é um registro que apresenta **os dois problemas**, está fora do
escopo temporal e tem duração não-positiva. Como os motivos de descarte são
mutuamente exclusivos e avaliados em ordem, ele é classificado apenas como
`fora_do_escopo_temporal`. Verificado na execução: quarentena registrou 104
fora de escopo e 6.180 de duração não-positiva.

### Princípio adotado

A silver descarta **apenas o que é comprovadamente inválido**, corrida fora do
escopo temporal e corrida com duração não-positiva. Todo o resto é preservado
com colunas de sinalização (`flag_*`), deixando a decisão para o consumidor.

Decisões de métrica, como excluir `total_amount <= 0` de uma média, pertencem à
gold. Assim a camada de consumo continua servindo perguntas que ainda não foram
formuladas, em vez de responder apenas às duas do case.
