-- Dataset 3: variação % mês a mês por UF para um produto (heatmap ou barras).
-- Parâmetro do dashboard: :produto.
SELECT
  mes,
  uf,
  preco_mes_anterior,
  preco_medio,
  variacao_pct
FROM workspace.anp_combustiveis.gold_variacao_mensal_uf_produto
WHERE produto = :produto
  AND unidade_medida = 'R$ / LITRO'
  AND variacao_pct IS NOT NULL
ORDER BY mes, uf;
