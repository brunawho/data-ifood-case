# Case Técnico — Data Architect (iFood)

Pipeline de ingestão, disponibilização e análise dos dados de corridas de
*yellow taxi* da NYC TLC (janeiro a maio de 2023).

> **Status:** em desenvolvimento. Etapas concluídas: landing e bronze.

## Arquitetura

Arquitetura *medallion* sobre Delta Lake, com metadados no Unity Catalog.

| Camada | Objeto | Responsabilidade |
|---|---|---|
| Landing | `/Volumes/workspace/raw/landing` | Arquivos originais, imutáveis, sem interpretação. Permite reprocessar sem depender da origem. |
| Bronze | `workspace.bronze.yellow_tripdata` | Schema da origem com tipos canonizados + colunas de auditoria. Nenhum registro filtrado. |
| Silver | `workspace.silver.fact_yellow_trips` | Camada de consumo: dados limpos e tipados, com as colunas exigidas pelo case. |
| Gold | `workspace.gold.*` | Agregações que respondem às perguntas analíticas. |

## Decisões técnicas

**Databricks Free Edition como runtime.** O case recomenda o Community
Edition, que foi aposentado no fim de 2025. O substituto é o Free Edition,
serverless e com Unity Catalog nativo.

**Catálogo `workspace`.** O Free Edition não permite criar catálogo novo (falta
storage credential). Em ambiente real haveria catálogo por domínio e por
ambiente; aqui os schemas ficam sob `workspace`.

**Partição nomeada `_ref_period`, não `data da corrida`.** Cada arquivo mensal
da TLC contém corridas com *pickup* fora do próprio mês. Nomear a partição como
"referência do arquivo" mantém essa distinção explícita desde a landing.

**CAST no plano do Spark, não no leitor de Parquet.** Os parquets mensais não
têm schema estável (mesma coluna como `int32` num mês e `int64` noutro). A
leitura é feita arquivo por arquivo, com conversão posterior para o tipo
canônico — o que evita `SchemaColumnConvertNotSupportedException`.

**Idempotência em todas as camadas.** A landing compara tamanho com a origem
antes de baixar; a bronze usa `replaceWhere` por partição. Reexecutar o
pipeline não duplica dado, e reprocessar um mês não afeta os outros.

## Estrutura

```
├─ notebooks/     # Orquestradores executados no Databricks
├─ src/
│  ├─ config.py         # Configuração central (isola local x Databricks)
│  ├─ ingestion/        # Origem -> landing
│  ├─ transform/        # Landing -> bronze -> silver -> gold
│  └─ utils/            # SparkSession e namespaces
├─ analysis/      # Respostas às perguntas do case
├─ tests/
└─ requirements.txt
```

## Execução

### Databricks (recomendado)

1. **Workspace → Create → Git folder**, apontando para este repositório
2. Executar `notebooks/01_landing_bronze.py`

Os schemas e o Volume são criados pelo próprio pipeline, de forma idempotente.

### Local

Requer JDK 8, 11 ou 17 (não 21+) e ~6 GiB de disco.

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python -m src.ingestion.landing     # origem -> landing
python -m src.transform.bronze      # landing -> bronze
```

### Variáveis de ambiente

| Variável | Default | Descrição |
|---|---|---|
| `IFOOD_ENV` | detectado | `databricks` ou `local` |
| `IFOOD_CATALOG` | `workspace` / `spark_catalog` | Catálogo do metastore |
| `IFOOD_LOCAL_ROOT` | `./data` | Raiz de dados em execução local |

## Fonte dos dados

NYC Taxi & Limousine Commission — *TLC Trip Record Data*:
<https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page>
