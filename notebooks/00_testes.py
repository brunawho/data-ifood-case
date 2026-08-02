# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Testes
# MAGIC
# MAGIC Executa a suíte de testes dentro do workspace.
# MAGIC
# MAGIC Os testes de `test_silver_regras.py` precisam de uma `SparkSession`, e a
# MAGIC fixture reaproveita a sessão do runtime. Rodá-los aqui valida as regras no
# MAGIC mesmo ambiente em que o pipeline executa.
# MAGIC
# MAGIC Não é etapa do pipeline: rode ao alterar qualquer regra em `src/`.

# COMMAND ----------

# MAGIC %pip install pytest

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os
import sys

# O filesystem do Workspace não permite criar diretórios `__pycache__`, e o
# Python tenta gravar bytecode ao importar cada módulo de teste. Sem isto, a
# execução falha com OSError [Errno 95] antes de rodar qualquer asserção.
sys.dont_write_bytecode = True
os.environ["PYTHONDONTWRITEBYTECODE"] = "1"

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
os.chdir(REPO_ROOT)
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

os.environ["IFOOD_ENV"] = "databricks"
os.environ["IFOOD_CATALOG"] = "workspace"

print(f"Raiz do repositório: {REPO_ROOT}")

# COMMAND ----------

import pytest

codigo = pytest.main(["-v", "--no-header", "-p", "no:cacheprovider", "tests/"])
print(f"\nCódigo de saída: {codigo}  (0 = todos passaram)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Só os testes que não precisam de Spark
# MAGIC
# MAGIC Mais rápidos. Úteis ao alterar configuração ou a lógica de ingestão.

# COMMAND ----------

pytest.main(["-q", "-p", "no:cacheprovider", "tests/test_config_e_ingestao.py"])
