"""Regras de qualidade de dados da camada silver.

Cada regra é uma expressão booleana que é True quando a linha PASSA. Valores nulos nunca
passam (a regra é envolvida em `coalesce(..., False)`), então um preço que não pôde ser
convertido para número também é reprovado.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

DQ_FAILURES_COL = "_dq_failures"

VALID_UFS: frozenset[str] = frozenset(
    {
        "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS", "MG", "PA",
        "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO",
    }
)  # fmt: skip

# Limite de sanidade. O GLP P13 (R$ / 13 kg) é o produto mais caro, na casa de R$ 100 a 200.
MAX_PLAUSIBLE_PRICE = 1000


def quality_rules(max_price: float = MAX_PLAUSIBLE_PRICE) -> dict[str, Column]:
    """Devolve {nome_da_regra: expressão que é True quando a linha passa}."""
    rules = {
        "valor_venda_positivo": F.col("valor_venda") > 0,
        "valor_venda_plausivel": F.col("valor_venda") <= max_price,
        "uf_valida": F.col("uf").isin(*sorted(VALID_UFS)),
        "data_coleta_valida": F.col("data_coleta") <= F.current_date(),
        "produto_preenchido": F.col("produto").isNotNull(),
        "cnpj_preenchido": F.col("cnpj").isNotNull(),
    }
    # data_coleta nula (parse falhou) também precisa reprovar: o `<=` acima devolve nulo.
    return {name: F.coalesce(expr, F.lit(False)) for name, expr in rules.items()}


def apply_quality_rules(
    df: DataFrame, rules: dict[str, Column] | None = None
) -> tuple[DataFrame, DataFrame]:
    """Separa `df` em (válidos, inválidos).

    Os inválidos ganham a coluna `_dq_failures` (array com o nome de todas as regras
    que a linha falhou). Os válidos saem sem essa coluna.
    """
    rules = rules or quality_rules()
    failures = F.filter(
        F.array(*[F.when(~expr, F.lit(name)) for name, expr in rules.items()]),
        lambda item: item.isNotNull(),
    )
    flagged = df.withColumn(DQ_FAILURES_COL, failures)
    is_valid = F.size(F.col(DQ_FAILURES_COL)) == 0
    return flagged.filter(is_valid).drop(DQ_FAILURES_COL), flagged.filter(~is_valid)
