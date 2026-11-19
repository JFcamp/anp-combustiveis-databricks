# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Setup (opcional)
# MAGIC Cria schema e Volume caso você rode o pipeline sem o Databricks Asset Bundle.
# MAGIC Com `databricks bundle deploy`, o `databricks.yml` já cria os dois.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "anp_combustiveis")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.`raw`")
print(f"Pronto. Envie os CSVs para /Volumes/{catalog}/{schema}/raw/anp/<ano>/")
