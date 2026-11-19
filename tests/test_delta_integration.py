"""Testes de integração com Delta Lake local: provam a idempotência das escritas."""

from datetime import UTC, datetime

from anp_pipeline.bronze import add_ingestion_metadata, write_bronze
from anp_pipeline.silver import build_silver, merge_silver, write_quarantine
from test_silver import bronze_row, make_bronze

T1 = datetime(2025, 1, 10, tzinfo=UTC)
T2 = datetime(2025, 2, 10, tzinfo=UTC)


def test_bronze_rewrite_of_same_file_does_not_duplicate(spark, require_delta):
    spark.sql("DROP TABLE IF EXISTS bronze_t")
    batch = make_bronze(spark, [bronze_row(), bronze_row(produto="ETANOL")])
    write_bronze(spark, add_ingestion_metadata(batch, T1), "bronze_t")
    write_bronze(spark, add_ingestion_metadata(batch, T2), "bronze_t")
    assert spark.table("bronze_t").count() == 2


def test_bronze_second_file_is_appended_not_replaced(spark, require_delta):
    spark.sql("DROP TABLE IF EXISTS bronze_t2")
    a = make_bronze(spark, [bronze_row(_source_file="a.csv")])
    b = make_bronze(spark, [bronze_row(_source_file="b.csv", produto="ETANOL")])
    write_bronze(spark, add_ingestion_metadata(a, T1), "bronze_t2")
    write_bronze(spark, add_ingestion_metadata(b, T1), "bronze_t2")
    assert spark.table("bronze_t2").count() == 2


def test_silver_merge_twice_does_not_duplicate_and_updates(spark, require_delta):
    spark.sql("DROP TABLE IF EXISTS silver_t")
    first, _ = build_silver(make_bronze(spark, [bronze_row(valor_de_venda="5,00")]))
    merge_silver(spark, first, "silver_t")
    merge_silver(spark, first, "silver_t")  # rodar de novo não duplica
    assert spark.table("silver_t").count() == 1

    corrected, _ = build_silver(
        make_bronze(spark, [bronze_row(valor_de_venda="6,00", _ingested_at=T2)])
    )
    merge_silver(spark, corrected, "silver_t")  # mesma chave, ingestão mais nova: atualiza
    rows = spark.table("silver_t").collect()
    assert len(rows) == 1
    assert str(rows[0]["valor_venda"]) == "6.0000"


def test_silver_merge_ignores_older_ingestion(spark, require_delta):
    spark.sql("DROP TABLE IF EXISTS silver_t2")
    new, _ = build_silver(make_bronze(spark, [bronze_row(valor_de_venda="6,00", _ingested_at=T2)]))
    old, _ = build_silver(make_bronze(spark, [bronze_row(valor_de_venda="5,00", _ingested_at=T1)]))
    merge_silver(spark, new, "silver_t2")
    merge_silver(spark, old, "silver_t2")
    assert str(spark.table("silver_t2").collect()[0]["valor_venda"]) == "6.0000"


def test_quarantine_rewrite_is_idempotent(spark, require_delta):
    spark.sql("DROP TABLE IF EXISTS quarantine_t")
    bronze = make_bronze(spark, [bronze_row(valor_de_venda="0,00")])
    _, quarantine = build_silver(bronze)
    write_quarantine(spark, quarantine, "quarantine_t", ["gasolina-etanol-2025-01.csv"])
    write_quarantine(spark, quarantine, "quarantine_t", ["gasolina-etanol-2025-01.csv"])
    assert spark.table("quarantine_t").count() == 1
