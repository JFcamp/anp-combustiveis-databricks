from datetime import date
from decimal import Decimal

from anp_pipeline.quality import VALID_UFS, apply_quality_rules

COLUMNS = "cnpj string, uf string, produto string, data_coleta date, valor_venda decimal(10,4)"


def make_df(spark, rows):
    return spark.createDataFrame(rows, COLUMNS)


GOOD = ("00.306.000/0001-00", "DF", "GASOLINA", date(2025, 1, 2), Decimal("5.99"))


def failures_by_cnpj(invalid):
    return {r["cnpj"]: set(r["_dq_failures"]) for r in invalid.collect()}


def test_valid_row_passes(spark):
    valid, invalid = apply_quality_rules(make_df(spark, [GOOD]))
    assert valid.count() == 1
    assert invalid.count() == 0
    assert "_dq_failures" not in valid.columns


def test_zero_negative_and_null_price_fail(spark):
    rows = [
        ("A", "SP", "ETANOL", date(2025, 1, 2), Decimal("0")),
        ("B", "SP", "ETANOL", date(2025, 1, 2), Decimal("-1.5")),
        ("C", "SP", "ETANOL", date(2025, 1, 2), None),
    ]
    valid, invalid = apply_quality_rules(make_df(spark, rows))
    assert valid.count() == 0
    failures = failures_by_cnpj(invalid)
    assert "valor_venda_positivo" in failures["A"]
    assert "valor_venda_positivo" in failures["B"]
    assert "valor_venda_positivo" in failures["C"]


def test_price_above_plausible_limit_fails(spark):
    rows = [("A", "SP", "GLP", date(2025, 1, 2), Decimal("5000"))]
    _, invalid = apply_quality_rules(make_df(spark, rows))
    assert failures_by_cnpj(invalid)["A"] == {"valor_venda_plausivel"}


def test_glp_price_is_accepted(spark):
    rows = [("A", "SP", "GLP", date(2025, 1, 2), Decimal("135.00"))]
    valid, _ = apply_quality_rules(make_df(spark, rows))
    assert valid.count() == 1


def test_invalid_uf_fails(spark):
    rows = [
        ("A", "XX", "ETANOL", date(2025, 1, 2), Decimal("4.0")),
        ("B", None, "ETANOL", date(2025, 1, 2), Decimal("4.0")),
    ]
    _, invalid = apply_quality_rules(make_df(spark, rows))
    failures = failures_by_cnpj(invalid)
    assert failures["A"] == {"uf_valida"}
    assert failures["B"] == {"uf_valida"}


def test_all_27_ufs_are_valid():
    assert len(VALID_UFS) == 27


def test_null_and_future_dates_fail(spark):
    rows = [
        ("A", "SP", "ETANOL", None, Decimal("4.0")),
        ("B", "SP", "ETANOL", date(2999, 1, 1), Decimal("4.0")),
    ]
    _, invalid = apply_quality_rules(make_df(spark, rows))
    failures = failures_by_cnpj(invalid)
    assert failures["A"] == {"data_coleta_valida"}
    assert failures["B"] == {"data_coleta_valida"}


def test_missing_product_and_cnpj_fail(spark):
    rows = [(None, "SP", None, date(2025, 1, 2), Decimal("4.0"))]
    _, invalid = apply_quality_rules(make_df(spark, rows))
    row = invalid.collect()[0]
    assert set(row["_dq_failures"]) == {"produto_preenchido", "cnpj_preenchido"}


def test_multiple_failures_are_all_reported(spark):
    rows = [("A", "XX", "ETANOL", date(2025, 1, 2), Decimal("0"))]
    _, invalid = apply_quality_rules(make_df(spark, rows))
    assert failures_by_cnpj(invalid)["A"] == {"valor_venda_positivo", "uf_valida"}


def test_valid_and_invalid_partition_the_input(spark):
    rows = [
        GOOD,
        ("A", "XX", "ETANOL", date(2025, 1, 2), Decimal("4.0")),
        ("B", "SP", "ETANOL", date(2025, 1, 2), Decimal("4.0")),
    ]
    df = make_df(spark, rows)
    valid, invalid = apply_quality_rules(df)
    assert valid.count() + invalid.count() == df.count()
