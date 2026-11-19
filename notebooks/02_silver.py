# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver
# MAGIC Limpa, tipa, valida e deduplica os dados da bronze. Linhas inválidas vão para a quarentena.
# MAGIC A lógica está em `src/anp_pipeline/silver.py` e `quality.py`.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath("../src"))

from anp_pipeline import silver
from anp_pipeline.config import Tables

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "anp_combustiveis")
dbutils.widgets.dropdown("full_refresh", "false", ["false", "true"])
tables = Tables(dbutils.widgets.get("catalog"), dbutils.widgets.get("schema"))
full_refresh = dbutils.widgets.get("full_refresh") == "true"

# COMMAND ----------

result = silver.run(spark, tables.bronze, tables.silver, tables.quarantine, full_refresh)
print(result)
