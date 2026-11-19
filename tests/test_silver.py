from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from anp_pipeline.bronze import EXPECTED_COLUMNS
from anp_pipeline.silver import (
    SILVER_COLUMNS,
    build_merge_sql,
    build_silver,
    clean_and_type,
    deduplicate,
)

T1 = datetime(2025, 1, 10, tzinfo=UTC)
T2 = datetime(2025, 2, 10, tzinfo=UTC)


def bronze_row(**overrides):
    base = {
        "regiao_sigla": "NE",
        "estado_sigla": "AL",
        "municipio": "  arapiraca ",
        "revenda": "POSTO  VITAL LTDA",
        "cnpj_da_revenda": " 02.817.655/0001-82",
        "nome_da_rua": "RODOVIA AL 220",
        "numero_rua": "S/N",
        "complemento": "",
        "bairro": "PLANALTO",
        "cep": "57308-000",
        "produto": "GASOLINA",
        "data_da_coleta": "01/01/2025",
        "valor_de_venda": "5,68",
        "valor_de_compra": "",
        "unidade_de_medida": "R$ / litro",
        "bandeira": "VIBRA",
        "_source_file": "gasolina-etanol-2025-01.csv",
        "_ingested_at": T1,
    }
    base.update(overrides)
    return base


def make_bronze(spark, rows):
    schema = ", ".join(f"{c} string" for c in EXPECTED_COLUMNS)
    schema += ", _source_file string, _ingested_at timestamp"
    return spark.createDataFrame(
        [tuple(r[f.split()[0]] for f in schema.split(", ")) for r in rows], schema
    )


def test_clean_and_type_converts_types_and_standardizes(spark):
    row = clean_and_type(make_bronze(spark, [bronze_row()])).collect()[0]
    assert row["valor_venda"] == Decimal("5.6800")  # vírgula decimal -> ponto
    assert row["data_coleta"] == date(2025, 1, 1)
    assert row["uf"] == "AL"
    assert row["municipio"] == "ARAPIRACA"  # trim + maiúsculas
    assert row["revenda"] == "POSTO VITAL LTDA"  # espaços repetidos colapsados
    assert row["cnpj"] == "02817655000182"  # só dígitos
    assert row["complemento"] is None  # vazio vira nulo
    assert row["valor_compra"] is None
    assert row["unidade_medida"] == "R$ / LITRO"
    assert row["raw_valor_venda"] == "5,68"


def test_invalid_date_and_price_become_null_without_error(spark):
    rows = [
        bronze_row(data_da_coleta="32/13/2025", valor_de_venda="abc"),
        bronze_row(data_da_coleta="2025-01-01", valor_de_venda=""),
    ]
    out = clean_and_type(make_bronze(spark, rows)).collect()
    assert all(r["data_coleta"] is None and r["valor_venda"] is None for r in out)
    assert out[0]["raw_data_coleta"] == "32/13/2025"


def test_short_cnpj_becomes_null(spark):
    row = clean_and_type(make_bronze(spark, [bronze_row(cnpj_da_revenda="123")])).collect()[0]
    assert row["cnpj"] is None


def test_deduplicate_keeps_latest_ingestion(spark):
    rows = [
        bronze_row(valor_de_venda="5,00", _ingested_at=T1),
        bronze_row(valor_de_venda="6,00", _ingested_at=T2, _source_file="outro.csv"),
        bronze_row(produto="ETANOL", valor_de_venda="4,00"),
    ]
    out = deduplicate(clean_and_type(make_bronze(spark, rows)))
    result = {r["produto"]: r["valor_venda"] for r in out.collect()}
    assert result == {"GASOLINA": Decimal("6.0000"), "ETANOL": Decimal("4.0000")}


def test_deduplicate_is_deterministic_on_ties(spark):
    rows = [
        bronze_row(valor_de_venda="5,00"),
        bronze_row(valor_de_venda="5,50"),
    ]
    out = deduplicate(clean_and_type(make_bronze(spark, rows))).collect()
    assert len(out) == 1
    assert out[0]["valor_venda"] == Decimal("5.5000")


def test_build_silver_splits_valid_and_quarantine(spark):
    rows = [
        bronze_row(),
        bronze_row(cnpj_da_revenda="11.222.333/0001-81", valor_de_venda="0,00"),
        bronze_row(cnpj_da_revenda="22.333.444/0001-81", estado_sigla="XX"),
        bronze_row(cnpj_da_revenda="33.444.555/0001-81", data_da_coleta="31/02/2025"),
    ]
    silver, quarantine = build_silver(make_bronze(spark, rows))
    assert silver.columns == SILVER_COLUMNS
    assert silver.count() == 1
    failures = {r["cnpj"]: set(r["_dq_failures"]) for r in quarantine.collect()}
    assert failures["11222333000181"] == {"valor_venda_positivo"}
    assert failures["22333444000181"] == {"uf_valida"}
    assert failures["33444555000181"] == {"data_coleta_valida"}
    raw = {r["cnpj"]: r["raw_data_coleta"] for r in quarantine.collect()}
    assert raw["33444555000181"] == "31/02/2025"


def test_build_silver_is_idempotent_for_same_input(spark):
    bronze = make_bronze(spark, [bronze_row(), bronze_row()])
    first, _ = build_silver(bronze)
    second, _ = build_silver(bronze)
    assert sorted(map(tuple, first.collect())) == sorted(map(tuple, second.collect()))


def test_build_merge_sql_uses_business_key_and_guard():
    sql = build_merge_sql("workspace.anp.silver", "updates")
    assert "MERGE INTO workspace.anp.silver AS t USING updates AS s" in sql
    assert (
        "t.cnpj <=> s.cnpj AND t.produto <=> s.produto AND t.data_coleta <=> s.data_coleta" in sql
    )
    assert "WHEN MATCHED AND s._ingested_at >= t._ingested_at THEN UPDATE SET" in sql
    assert "WHEN NOT MATCHED THEN INSERT" in sql


def test_build_merge_sql_rejects_unsafe_identifiers():
    with pytest.raises(ValueError):
        build_merge_sql("t; DROP TABLE x", "updates")
