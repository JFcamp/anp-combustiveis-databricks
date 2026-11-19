from datetime import date
from decimal import Decimal

from anp_pipeline.config import Tables
from anp_pipeline.gold import (
    city_ranking,
    ethanol_gasoline_ratio,
    monthly_average_price,
    monthly_variation,
)

SILVER_SCHEMA = (
    "uf string, municipio string, cnpj string, produto string, unidade_medida string, "
    "data_coleta date, valor_venda decimal(10,4)"
)


def silver(spark, rows):
    typed = [(u, m, c, p, "R$ / LITRO", d, Decimal(v)) for (u, m, c, p, d, v) in rows]
    return spark.createDataFrame(typed, SILVER_SCHEMA)


def by_key(df, *keys):
    return {tuple(r[k] for k in keys): r.asDict() for r in df.collect()}


def test_monthly_average_price(spark):
    df = silver(
        spark,
        [
            ("SP", "SAO PAULO", "1", "GASOLINA", date(2025, 1, 3), "5.00"),
            ("SP", "SAO PAULO", "2", "GASOLINA", date(2025, 1, 20), "6.00"),
            ("SP", "SAO PAULO", "2", "GASOLINA", date(2025, 1, 27), "7.00"),
            ("SP", "SAO PAULO", "1", "GASOLINA", date(2025, 2, 3), "8.00"),
        ],
    )
    out = by_key(monthly_average_price(df), "mes", "uf", "produto")
    jan = out[(date(2025, 1, 1), "SP", "GASOLINA")]
    assert jan["preco_medio"] == Decimal("6.0000")
    assert jan["preco_min"] == Decimal("5.0000")
    assert jan["preco_max"] == Decimal("7.0000")
    assert jan["qtd_coletas"] == 3
    assert jan["qtd_postos"] == 2
    assert out[(date(2025, 2, 1), "SP", "GASOLINA")]["preco_medio"] == Decimal("8.0000")


def test_monthly_variation_and_first_month_is_null(spark):
    df = silver(
        spark,
        [
            ("SP", "A", "1", "ETANOL", date(2025, 1, 5), "4.00"),
            ("SP", "A", "1", "ETANOL", date(2025, 2, 5), "4.40"),
            ("SP", "A", "1", "ETANOL", date(2025, 3, 5), "3.96"),
        ],
    )
    out = by_key(monthly_variation(monthly_average_price(df)), "mes")
    assert out[(date(2025, 1, 1),)]["variacao_pct"] is None
    assert out[(date(2025, 2, 1),)]["variacao_pct"] == Decimal("10.00")
    assert out[(date(2025, 3, 1),)]["variacao_pct"] == Decimal("-10.00")
    assert out[(date(2025, 3, 1),)]["preco_mes_anterior"] == Decimal("4.4000")


def test_monthly_variation_ignores_gap_months(spark):
    df = silver(
        spark,
        [
            ("SP", "A", "1", "ETANOL", date(2025, 1, 5), "4.00"),
            ("SP", "A", "1", "ETANOL", date(2025, 3, 5), "5.00"),  # fevereiro ausente
        ],
    )
    out = by_key(monthly_variation(monthly_average_price(df)), "mes")
    assert out[(date(2025, 3, 1),)]["variacao_pct"] is None


def test_monthly_variation_is_per_uf_and_product(spark):
    df = silver(
        spark,
        [
            ("SP", "A", "1", "ETANOL", date(2025, 1, 5), "4.00"),
            ("RJ", "B", "2", "ETANOL", date(2025, 2, 5), "9.00"),
            ("SP", "A", "1", "GASOLINA", date(2025, 2, 5), "9.00"),
        ],
    )
    out = monthly_variation(monthly_average_price(df))
    assert out.filter("variacao_pct is not null").count() == 0


def test_city_ranking_requires_min_stations_and_ranks(spark):
    rows = []
    for i, price in enumerate(["5.00", "5.10", "5.20"]):  # cidade barata, 3 postos
        rows.append(("SP", "BARATA", f"b{i}", "GASOLINA", date(2025, 1, 5), price))
    for i, price in enumerate(["7.00", "7.20", "7.40"]):  # cidade cara, 3 postos
        rows.append(("AC", "CARA", f"c{i}", "GASOLINA", date(2025, 1, 5), price))
    rows.append(("RJ", "UM_POSTO", "x", "GASOLINA", date(2025, 1, 5), "1.00"))  # excluída
    out = by_key(city_ranking(silver(spark, rows)), "municipio")
    assert set(out) == {("BARATA",), ("CARA",)}
    assert out[("CARA",)]["rank_mais_caro"] == 1
    assert out[("CARA",)]["rank_mais_barato"] == 2
    assert out[("BARATA",)]["rank_mais_barato"] == 1
    assert out[("BARATA",)]["qtd_postos"] == 3


def test_city_ranking_counts_distinct_stations(spark):
    rows = [("SP", "X", "1", "GASOLINA", date(2025, 1, d), "5.00") for d in (1, 2, 3, 4)]
    assert city_ranking(silver(spark, rows)).count() == 0  # 4 coletas, mas 1 posto


def test_ethanol_gasoline_ratio(spark):
    df = silver(
        spark,
        [
            ("SP", "A", "1", "ETANOL", date(2025, 1, 5), "4.00"),
            ("SP", "A", "1", "GASOLINA", date(2025, 1, 5), "6.00"),  # 0,6667 -> compensa
            ("RJ", "B", "2", "ETANOL", date(2025, 1, 5), "5.00"),
            ("RJ", "B", "2", "GASOLINA", date(2025, 1, 5), "6.00"),  # 0,8333 -> não compensa
            ("MG", "C", "3", "ETANOL", date(2025, 1, 5), "4.00"),  # sem gasolina -> sem linha
        ],
    )
    out = by_key(ethanol_gasoline_ratio(monthly_average_price(df)), "uf")
    assert set(out) == {("SP",), ("RJ",)}
    assert out[("SP",)]["razao"] == Decimal("0.6667")
    assert out[("SP",)]["etanol_compensa"] is True
    assert out[("RJ",)]["etanol_compensa"] is False


def test_ratio_ignores_non_liter_units_and_other_products(spark):
    df = silver(
        spark,
        [
            ("SP", "A", "1", "ETANOL", date(2025, 1, 5), "4.00"),
            ("SP", "A", "1", "GASOLINA ADITIVADA", date(2025, 1, 5), "6.00"),
        ],
    )
    assert ethanol_gasoline_ratio(monthly_average_price(df)).count() == 0


def test_tables_names():
    t = Tables("workspace", "anp_combustiveis")
    assert t.silver == "workspace.anp_combustiveis.silver_anp_precos"
    assert t.raw_glob == "/Volumes/workspace/anp_combustiveis/raw/anp/*/*.csv"
