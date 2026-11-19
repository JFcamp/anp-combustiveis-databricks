# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Gold
# MAGIC Gera as tabelas analíticas a partir da silver. A lógica está em `src/anp_pipeline/gold.py`.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath("../src"))

from anp_pipeline import gold
from anp_pipeline.config import Tables

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "anp_combustiveis")
tables = Tables(dbutils.widgets.get("catalog"), dbutils.widgets.get("schema"))

# COMMAND ----------

for table, n_rows in gold.run(spark, tables).items():
    print(f"{table}: {n_rows} linhas")
