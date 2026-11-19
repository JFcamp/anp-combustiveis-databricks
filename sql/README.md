# Queries do dashboard

Cada arquivo é um dataset do AI/BI Dashboard (ou uma query do Databricks SQL).

| Arquivo | Visual sugerido |
|---|---|
| `01_preco_medio_brasil_por_produto.sql` | Linhas: preço médio nacional por produto ao longo do tempo |
| `02_preco_medio_por_uf.sql` | Barras ou mapa: preço por UF no mês mais recente |
| `03_variacao_mensal_por_uf.sql` | Heatmap: variação % mês a mês |
| `04_ranking_municipios_mais_caros.sql` | Tabela: top 10 mais caros |
| `05_ranking_municipios_mais_baratos.sql` | Tabela: top 10 mais baratos |
| `06_etanol_vs_gasolina.sql` | Barras: razão etanol/gasolina com linha em 0,70 |
| `07_qualidade_de_dados.sql` | Barras: quarentena por regra |
| `08_cobertura_do_pipeline.sql` | Contadores por camada |

Como montar: em **Dashboards > Create dashboard > Data**, crie um dataset por arquivo e, nas
queries com `:produto`, adicione um filtro de parâmetro com valores como `GASOLINA`,
`ETANOL` e `DIESEL S10`. As queries assumem `workspace.anp_combustiveis`; troque se usar
outro catalog ou schema.
