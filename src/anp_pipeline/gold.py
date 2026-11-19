"""Camada gold: tabelas analíticas calculadas a partir da silver.

As funções de transformação são puras (DataFrame -> DataFrame). A escrita é um overwrite
completo da tabela: os agregados são determinísticos, então rodar de novo dá o mesmo
resultado (idempotente). Variação e ranking usam janelas sobre vários meses, por isso a
recomputação completa é mais simples e segura do que um merge parcial.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from anp_pipeline.config import Tables

ETHANOL_PARITY = 0.70  # regra prática: etanol compensa se custa até 70% da gasolina
MIN_STATIONS_PER_CITY = 3
LITER_UNIT = "R$ / LITRO"


def monthly_average_price(silver: DataFrame) -> DataFrame:
    """Preço médio mensal por UF, produto e unidade de medida."""
    return (
        silver.withColumn("mes", F.trunc("data_coleta", "month"))
        .groupBy("mes", "uf", "produto", "unidade_medida")
        .agg(
            F.round(F.avg("valor_venda"), 4).alias("preco_medio"),
            F.min("valor_venda").alias("preco_min"),
            F.max("valor_venda").alias("preco_max"),
            F.count("*").alias("qtd_coletas"),
            F.countDistinct("cnpj").alias("qtd_postos"),
        )
    )


def monthly_variation(monthly: DataFrame) -> DataFrame:
    """Variação % do preço médio contra o mês imediatamente anterior.

    Se o mês anterior não existe nos dados (lacuna), a variação fica nula em vez de
    comparar com um mês distante.
    """
    window = Window.partitionBy("uf", "produto", "unidade_medida").orderBy("mes")
    with_prev = monthly.select(
        "mes",
        "uf",
        "produto",
        "unidade_medida",
        "preco_medio",
        F.lag("preco_medio").over(window).alias("preco_mes_anterior"),
        F.lag("mes").over(window).alias("_mes_anterior"),
    )
    is_consecutive = (F.col("_mes_anterior") == F.add_months("mes", -1)) & (
        F.col("preco_mes_anterior") > 0
    )
    return with_prev.withColumn(
        "variacao_pct",
        F.when(
            is_consecutive,
            F.round(
                (F.col("preco_medio") - F.col("preco_mes_anterior"))
                / F.col("preco_mes_anterior")
                * 100,
                2,
            ),
        ),
    ).drop("_mes_anterior")


def city_ranking(silver: DataFrame, min_stations: int = MIN_STATIONS_PER_CITY) -> DataFrame:
    """Ranking nacional de municípios por preço médio, em cada mês e produto.

    Só entram municípios com pelo menos `min_stations` postos distintos, para evitar que
    um único posto defina o "mais caro" ou o "mais barato" da cidade.
    """
    cities = (
        silver.withColumn("mes", F.trunc("data_coleta", "month"))
        .groupBy("mes", "produto", "unidade_medida", "uf", "municipio")
        .agg(
            F.round(F.avg("valor_venda"), 4).alias("preco_medio"),
            F.countDistinct("cnpj").alias("qtd_postos"),
        )
        .filter(F.col("qtd_postos") >= min_stations)
    )
    scope = Window.partitionBy("mes", "produto", "unidade_medida")
    return cities.select(
        "*",
        F.rank().over(scope.orderBy(F.col("preco_medio").desc())).alias("rank_mais_caro"),
        F.rank().over(scope.orderBy(F.col("preco_medio").asc())).alias("rank_mais_barato"),
    )


def ethanol_gasoline_ratio(monthly: DataFrame, parity: float = ETHANOL_PARITY) -> DataFrame:
    """Razão preço do etanol / preço da gasolina comum por UF e mês (R$ / litro)."""
    liters = monthly.filter(
        (F.col("unidade_medida") == LITER_UNIT) & F.col("produto").isin("ETANOL", "GASOLINA")
    )
    pivoted = liters.groupBy("mes", "uf").agg(
        F.max(F.when(F.col("produto") == "ETANOL", F.col("preco_medio"))).alias("preco_etanol"),
        F.max(F.when(F.col("produto") == "GASOLINA", F.col("preco_medio"))).alias("preco_gasolina"),
    )
    return (
        pivoted.filter(F.col("preco_etanol").isNotNull() & (F.col("preco_gasolina") > 0))
        .withColumn("razao", F.round(F.col("preco_etanol") / F.col("preco_gasolina"), 4))
        .withColumn("etanol_compensa", F.col("razao") <= parity)
    )


def write_gold(df: DataFrame, table: str) -> None:
    """Overwrite completo e idempotente da tabela gold."""
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table)
    )


def run(spark: SparkSession, tables: Tables) -> dict[str, int]:
    """Ponto de entrada do notebook. Devolve o número de linhas de cada tabela gold."""
    silver = spark.read.table(tables.silver)
    monthly = monthly_average_price(silver)
    write_gold(monthly, tables.gold_monthly)
    # Lê de volta a tabela gravada para as demais derivarem de um resultado já materializado.
    monthly = spark.read.table(tables.gold_monthly)
    outputs = {
        tables.gold_variation: monthly_variation(monthly),
        tables.gold_ranking: city_ranking(silver),
        tables.gold_ratio: ethanol_gasoline_ratio(monthly),
    }
    for table, df in outputs.items():
        write_gold(df, table)
    return {t: spark.read.table(t).count() for t in [tables.gold_monthly, *outputs]}
