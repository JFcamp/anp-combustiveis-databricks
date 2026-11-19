# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze
# MAGIC Lê os CSVs brutos do Volume e grava a tabela bronze (valores originais + metadados).
# MAGIC A lógica está em `src/anp_pipeline/bronze.py`.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath("../src"))

from anp_pipeline import bronze
from anp_pipeline.config import Tables

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "anp_combustiveis")
tables = Tables(dbutils.widgets.get("catalog"), dbutils.widgets.get("schema"))

# COMMAND ----------

n_files = bronze.ingest(spark, tables.raw_glob, tables.bronze)
print(f"{n_files} arquivo(s) ingerido(s) em {tables.bronze}")
