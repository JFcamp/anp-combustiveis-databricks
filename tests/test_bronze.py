import os
from datetime import UTC, datetime

import pytest

from anp_pipeline.bronze import (
    EXPECTED_COLUMNS,
    add_ingestion_metadata,
    build_replace_where,
    list_source_files,
    normalize_column_name,
    read_raw_csv,
    read_raw_files,
)

HEADER = (
    "Regiao - Sigla;Estado - Sigla;Municipio;Revenda;CNPJ da Revenda;Nome da Rua;Numero Rua;"
    "Complemento;Bairro;Cep;Produto;Data da Coleta;Valor de Venda;Valor de Compra;"
    "Unidade de Medida;Bandeira"
)
ROW = (
    "NE;AL;ARAPIRACA;POSTO VITAL LTDA; 02.817.655/0001-82;RODOVIA AL 220;S/N;;PLANALTO;"
    "57308-000;GASOLINA;01/01/2024;5,68;;R$ / litro;VIBRA"
)


def write_csv(path, header=HEADER, rows=(ROW,)):
    # utf-8-sig grava o BOM, como nos arquivos reais da ANP
    path.write_text("\n".join([header, *rows]) + "\n", encoding="utf-8-sig")
    return str(path)


def test_normalize_column_name():
    assert normalize_column_name("Regiao - Sigla") == "regiao_sigla"
    assert normalize_column_name("\ufeffRegiao - Sigla") == "regiao_sigla"
    assert normalize_column_name("CNPJ da Revenda") == "cnpj_da_revenda"
    assert normalize_column_name("Município") == "municipio"


def test_read_raw_csv_keeps_raw_strings(spark, tmp_path):
    path = write_csv(tmp_path / "gasolina-etanol-2024-01.csv")
    df = read_raw_csv(spark, path)
    row = df.collect()[0]
    assert df.columns == [*EXPECTED_COLUMNS, "_source_file"]
    assert all(t == "string" for _, t in df.dtypes)
    assert row["valor_de_venda"] == "5,68"  # vírgula decimal preservada
    assert row["data_da_coleta"] == "01/01/2024"
    assert row["cnpj_da_revenda"] == " 02.817.655/0001-82"  # espaço preservado
    assert row["_source_file"] == "gasolina-etanol-2024-01.csv"


def test_read_raw_csv_rejects_unexpected_header(spark, tmp_path):
    path = write_csv(tmp_path / "ruim.csv", header="a;b;c", rows=("1;2;3",))
    with pytest.raises(ValueError, match="sem as colunas esperadas"):
        read_raw_csv(spark, path)


def test_read_raw_files_unions_files_with_source_file(spark, tmp_path):
    p1 = write_csv(tmp_path / "a.csv")
    p2 = write_csv(tmp_path / "b.csv", rows=(ROW, ROW))
    df = read_raw_files(spark, [p1, p2])
    counts = {
        r["_source_file"]: r["n"]
        for r in df.groupBy("_source_file").count().withColumnRenamed("count", "n").collect()
    }
    assert counts == {"a.csv": 1, "b.csv": 2}


def test_list_source_files_uses_glob(tmp_path):
    write_csv(tmp_path / "a.csv")
    write_csv(tmp_path / "b.csv")
    (tmp_path / "ignorar.txt").write_text("x")
    files = list_source_files(str(tmp_path / "*.csv"))
    assert [os.path.basename(f) for f in files] == ["a.csv", "b.csv"]


def test_add_ingestion_metadata(spark, tmp_path):
    df = read_raw_csv(spark, write_csv(tmp_path / "a.csv"))
    moment = datetime(2025, 1, 1, 12, 0, tzinfo=UTC)
    out = add_ingestion_metadata(df, moment)
    assert dict(out.dtypes)["_ingested_at"] == "timestamp"
    assert out.select("_ingested_at").distinct().count() == 1


def test_build_replace_where_escapes_and_dedupes():
    cond = build_replace_where(["b.csv", "a.csv", "a.csv", "o'brien.csv"])
    assert cond == "_source_file IN ('a.csv', 'b.csv', 'o''brien.csv')"
