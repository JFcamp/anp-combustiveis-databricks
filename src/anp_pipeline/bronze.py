"""Camada bronze: CSVs brutos da ANP em Delta, sem alterar os valores.

Todas as colunas ficam como string. Só os nomes das colunas são normalizados (Delta não
aceita espaços e caracteres como "-" nos nomes) e duas colunas de metadados são adicionadas:
`_ingested_at` e `_source_file`.
"""

from __future__ import annotations

import glob
import re
import unicodedata
from datetime import UTC, datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

SOURCE_FILE_COL = "_source_file"
INGESTED_AT_COL = "_ingested_at"

# Colunas esperadas no CSV da ANP, já normalizadas por `normalize_column_name`.
EXPECTED_COLUMNS: tuple[str, ...] = (
    "regiao_sigla",
    "estado_sigla",
    "municipio",
    "revenda",
    "cnpj_da_revenda",
    "nome_da_rua",
    "numero_rua",
    "complemento",
    "bairro",
    "cep",
    "produto",
    "data_da_coleta",
    "valor_de_venda",
    "valor_de_compra",
    "unidade_de_medida",
    "bandeira",
)


def normalize_column_name(name: str) -> str:
    """'Regiao - Sigla' -> 'regiao_sigla'. Remove BOM e acentos."""
    name = name.replace("\ufeff", "").strip()
    name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def list_source_files(glob_path: str) -> list[str]:
    """Lista os arquivos que casam com o glob.

    Usa o `glob` do Python: os Volumes do Unity Catalog são montados como caminhos POSIX
    (/Volumes/...), e isso funciona também em compute serverless (Spark Connect), onde não
    há acesso à JVM.
    """
    return sorted(glob.glob(glob_path, recursive=True))


def read_raw_csv(spark: SparkSession, path: str) -> DataFrame:
    """Lê um CSV da ANP como strings, normaliza nomes e valida o conjunto de colunas."""
    df = (
        spark.read.option("header", True)
        .option("sep", ";")
        .option("encoding", "UTF-8")
        .option("inferSchema", False)
        .csv(path)
    )
    df = df.toDF(*[normalize_column_name(c) for c in df.columns])
    missing = set(EXPECTED_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(f"Arquivo {path} sem as colunas esperadas: {sorted(missing)}")
    return df.select(*EXPECTED_COLUMNS, F.col("_metadata.file_name").alias(SOURCE_FILE_COL))


def read_raw_files(spark: SparkSession, paths: list[str]) -> DataFrame:
    """Lê vários arquivos, validando as colunas de cada um separadamente."""
    if not paths:
        raise ValueError("Nenhum arquivo CSV encontrado para ingestão.")
    frames = [read_raw_csv(spark, p) for p in paths]
    result = frames[0]
    for frame in frames[1:]:
        result = result.unionByName(frame)
    return result


def add_ingestion_metadata(df: DataFrame, ingested_at: datetime | None = None) -> DataFrame:
    """Adiciona `_ingested_at` (UTC). `_source_file` já vem de `read_raw_csv`."""
    moment = ingested_at or datetime.now(UTC)
    return df.withColumn(INGESTED_AT_COL, F.lit(moment).cast("timestamp"))


def build_replace_where(files: list[str]) -> str:
    """Condição do replaceWhere do Delta: só as linhas desses arquivos são substituídas."""
    escaped = ", ".join("'" + f.replace("'", "''") + "'" for f in sorted(set(files)))
    return f"{SOURCE_FILE_COL} IN ({escaped})"


def write_bronze(spark: SparkSession, df: DataFrame, table: str) -> None:
    """Escrita idempotente: reingerir um arquivo substitui as linhas dele, sem duplicar."""
    files = [r[SOURCE_FILE_COL] for r in df.select(SOURCE_FILE_COL).distinct().collect()]
    if spark.catalog.tableExists(table):
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", build_replace_where(files))
            .saveAsTable(table)
        )
    else:
        df.write.format("delta").saveAsTable(table)


def ingest(spark: SparkSession, source_glob: str, table: str) -> int:
    """Ponto de entrada usado pelo notebook. Devolve o número de arquivos ingeridos."""
    paths = list_source_files(source_glob)
    df = add_ingestion_metadata(read_raw_files(spark, paths))
    write_bronze(spark, df, table)
    return len(paths)
