# Databricks notebook source
# MAGIC %md
# MAGIC # `_setup` — configuração compartilhada
# MAGIC
# MAGIC Notebook auxiliar. Não é executado diretamente: os demais o chamam com
# MAGIC `%run ./_setup` na primeira célula.
# MAGIC
# MAGIC Existe para que a configuração de ambiente fique num lugar só. Com um
# MAGIC notebook por etapa, repetir estas linhas em cada um seria duplicação
# MAGIC que sai de sincronia na primeira alteração.
# MAGIC
# MAGIC Após o `%run`, o notebook chamador tem acesso a `config`, `spark`, `os`,
# MAGIC `sys` e `logging` já prontos.

# COMMAND ----------

import os
import sys

# Precisa vir ANTES do import de src.config: o módulo resolve caminhos e nomes
# de tabela no momento do import, lendo estas variáveis.
os.environ["IFOOD_ENV"] = "databricks"
os.environ["IFOOD_CATALOG"] = "workspace"

# Em Git folder a raiz do repo já entra no sys.path, mas ser explícito evita
# ImportError silencioso se o notebook for movido de pasta.
REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import logging

logging.basicConfig(
    level=logging.INFO, format="%(levelname)-7s | %(message)s", force=True
)

from src import config


def mostrar_configuracao() -> None:
    """Imprime a configuração resolvida. Checkpoint antes de gastar compute."""
    print(f"Ambiente : {config.ENV}")
    print(f"Catálogo : {config.CATALOG}")
    print(f"Landing  : {config.landing_root()}")
    print(f"Bronze   : {config.TABLE_BRONZE}")
    print(f"Silver   : {config.TABLE_SILVER}")
    print(f"Períodos : {', '.join(config.PERIODS)}")
