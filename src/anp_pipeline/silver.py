"""Camada silver: dados limpos, tipados, sem duplicados e validados.

Fluxo: bronze (novos arquivos) -> `clean_and_type` -> regras de qualidade -> `deduplicate`.
Linhas válidas entram na silver por MERGE; inválidas vão para a quarentena com o motivo.
"""

from __future__ import annotations

import re

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from anp_pipeline.bronze import INGESTED_AT_COL, SOURCE_FILE_COL, build_replace_where
from anp_pipeline.quality import DQ_FAILURES_COL, apply_quality_rules

# bronze -> silver
COLUMN_MAP: dict[str, str] = {
    "regiao_sigla": "regiao",
    "estado_sigla": "uf",
    "municipio": "municipio",
    "revenda": "revenda",
    "cnpj_da_revenda": "cnpj",
    "nome_da_rua": "logradouro",
    "numero_rua": "numero",
    "complemento": "complemento",
    "bairro": "bairro",
    "cep": "cep",
    "produto": "produto",
    "data_da_coleta": "data_coleta",
    "valor_de_venda": "valor_venda",
    "valor_de_compra": "valor_compra",
    "unidade_de_medida": "unidade_medida",
    "bandeira": "bandeira",
}
SILVER_COLUMNS: list[str] = [*COLUMN_MAP.values(), SOURCE_FILE_COL, INGESTED_AT_COL]
BUSINESS_KEY: tuple[str, ...] = ("cnpj", "produto", "data_coleta")
DATE_FORMAT = "dd/MM/yyyy"
PRICE_TYPE = "decimal(10,4)"
RAW_DATE_COL = "raw_data_coleta"
RAW_PRICE_COL = "raw_valor_venda"

_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_]+(\.[A-Za-z0-9_]+){0,2}$")


def clean_string(col: Column) -> Column:
    """Apara, colapsa espaços repetidos, põe em maiúsculas e troca vazio por nulo."""
    cleaned = F.upper(F.regexp_replace(F.trim(col), r"\s+", " "))
    return F.when(cleaned == "", F.lit(None)).otherwise(cleaned)


def clean_cnpj(col: Column) -> Column:
    """Mantém só os dígitos. CNPJ que não tem 14 dígitos vira nulo e reprova na qualidade."""
    digits = F.regexp_replace(col, r"[^0-9]", "")
    return F.when(F.length(digits) == 14, digits).otherwise(F.lit(None))


def clean_and_type(bronze: DataFrame) -> DataFrame:
    """Renomeia, padroniza strings e converte tipos sem nunca lançar erro.

    Datas e preços inválidos viram nulo (parse seguro, compatível com ANSI mode) e os
    valores originais ficam em `raw_data_coleta` e `raw_valor_venda` para a quarentena.
    """
    df = bronze
    for old, new in COLUMN_MAP.items():
        df = df.withColumnRenamed(old, new)

    df = df.withColumn(RAW_DATE_COL, F.col("data_coleta")).withColumn(
        RAW_PRICE_COL, F.col("valor_venda")
    )

    special = ("cnpj", "data_coleta", "valor_venda", "valor_compra")
    for name in [c for c in COLUMN_MAP.values() if c not in special]:
        df = df.withColumn(name, clean_string(F.col(name)))

    df = df.withColumn("cnpj", clean_cnpj(F.col("cnpj")))
    df = df.withColumn(
        "data_coleta",
        F.expr(f"try_to_timestamp(trim(raw_data_coleta), '{DATE_FORMAT}')").cast("date"),
    )
    for name in ("valor_venda", "valor_compra"):
        df = df.withColumn(
            name,
            F.expr(f"try_cast(replace(trim({name}), ',', '.') AS {PRICE_TYPE})"),
        )
    return df


def deduplicate(df: DataFrame, keys: tuple[str, ...] = BUSINESS_KEY) -> DataFrame:
    """Mantém uma linha por chave de negócio: a ingestão mais recente.

    Desempate determinístico por arquivo de origem e preço, para o resultado não depender
    da ordem de leitura.
    """
    window = Window.partitionBy(*keys).orderBy(
        F.col(INGESTED_AT_COL).desc(),
        F.col(SOURCE_FILE_COL).desc(),
        F.col("valor_venda").desc_nulls_last(),
    )
    return df.withColumn("_rn", F.row_number().over(window)).filter("_rn = 1").drop("_rn")


def build_silver(bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Devolve (silver_válida_sem_duplicados, quarentena)."""
    typed = clean_and_type(bronze)
    valid, invalid = apply_quality_rules(typed)
    silver = deduplicate(valid).select(*SILVER_COLUMNS)
    quarantine = invalid.select(
        *SILVER_COLUMNS, RAW_DATE_COL, RAW_PRICE_COL, F.col(DQ_FAILURES_COL)
    )
    return silver, quarantine


# --------------------------------------------------------------------------------------
# I/O (Delta). Mantido fino para que a lógica acima seja testável sem Delta.
# --------------------------------------------------------------------------------------


def _check_identifier(name: str) -> str:
    if not _IDENTIFIER_RE.match(name):
        raise ValueError(f"Nome de tabela inválido: {name!r}")
    return name


def build_merge_sql(
    target: str,
    source_view: str,
    keys: tuple[str, ...] = BUSINESS_KEY,
    columns: list[str] | None = None,
) -> str:
    """MERGE INTO idempotente. Só atualiza se a origem for tão nova quanto o destino."""
    _check_identifier(target)
    _check_identifier(source_view)
    columns = columns or SILVER_COLUMNS
    on = " AND ".join(f"t.{k} <=> s.{k}" for k in keys)
    updates = ", ".join(f"t.{c} = s.{c}" for c in columns)
    names = ", ".join(columns)
    values = ", ".join(f"s.{c}" for c in columns)
    return (
        f"MERGE INTO {target} AS t USING {source_view} AS s ON {on} "
        f"WHEN MATCHED AND s.{INGESTED_AT_COL} >= t.{INGESTED_AT_COL} THEN UPDATE SET {updates} "
        f"WHEN NOT MATCHED THEN INSERT ({names}) VALUES ({values})"
    )


def merge_silver(spark: SparkSession, df: DataFrame, table: str) -> None:
    _check_identifier(table)
    if not spark.catalog.tableExists(table):
        df.write.format("delta").saveAsTable(table)
        return
    view = "_silver_updates"
    df.createOrReplaceTempView(view)
    spark.sql(build_merge_sql(table, view))


def write_quarantine(
    spark: SparkSession, df: DataFrame, table: str, source_files: list[str]
) -> None:
    """Substitui a quarentena dos arquivos processados (idempotente, sem duplicar)."""
    _check_identifier(table)
    if spark.catalog.tableExists(table):
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", build_replace_where(source_files))
            .saveAsTable(table)
        )
    else:
        df.write.format("delta").saveAsTable(table)


def select_new_bronze(
    spark: SparkSession, bronze_table: str, silver_table: str, full_refresh: bool = False
) -> DataFrame:
    """Lê da bronze só o que foi ingerido depois da última carga da silver."""
    _check_identifier(bronze_table)
    _check_identifier(silver_table)
    bronze = spark.read.table(bronze_table)
    if full_refresh or not spark.catalog.tableExists(silver_table):
        return bronze
    watermark = spark.read.table(silver_table).agg(F.max(INGESTED_AT_COL)).collect()[0][0]
    if watermark is None:
        return bronze
    return bronze.filter(F.col(INGESTED_AT_COL) > F.lit(watermark))


def run(
    spark: SparkSession,
    bronze_table: str,
    silver_table: str,
    quarantine_table: str,
    full_refresh: bool = False,
) -> dict[str, int]:
    """Ponto de entrada do notebook. Devolve contagens para log."""
    batch = select_new_bronze(spark, bronze_table, silver_table, full_refresh)
    files = [r[SOURCE_FILE_COL] for r in batch.select(SOURCE_FILE_COL).distinct().collect()]
    if not files:
        return {"arquivos": 0, "silver": 0, "quarentena": 0}
    silver, quarantine = build_silver(batch)
    merge_silver(spark, silver, silver_table)
    write_quarantine(spark, quarantine, quarantine_table, files)
    return {
        "arquivos": len(files),
        "silver": silver.count(),
        "quarentena": quarantine.count(),
    }
