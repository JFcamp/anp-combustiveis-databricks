"""Nomes de tabelas e caminhos, derivados de catalog e schema (parâmetros do job)."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Tables:
    catalog: str
    schema: str

    def _fq(self, name: str) -> str:
        return f"{self.catalog}.{self.schema}.{name}"

    @property
    def bronze(self) -> str:
        return self._fq("bronze_anp_precos")

    @property
    def silver(self) -> str:
        return self._fq("silver_anp_precos")

    @property
    def quarantine(self) -> str:
        return self._fq("silver_anp_precos_quarentena")

    @property
    def gold_monthly(self) -> str:
        return self._fq("gold_preco_medio_mensal_uf_produto")

    @property
    def gold_variation(self) -> str:
        return self._fq("gold_variacao_mensal_uf_produto")

    @property
    def gold_ranking(self) -> str:
        return self._fq("gold_ranking_municipios_mensal")

    @property
    def gold_ratio(self) -> str:
        return self._fq("gold_razao_etanol_gasolina_uf")

    @property
    def raw_glob(self) -> str:
        return f"/Volumes/{self.catalog}/{self.schema}/raw/anp/*/*.csv"
