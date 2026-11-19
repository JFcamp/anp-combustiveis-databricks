-- Dataset 2: preço médio por UF no mês mais recente, para um produto (gráfico de barras/mapa).
-- Parâmetro do dashboard: :produto (ex.: GASOLINA, ETANOL, DIESEL S10).
SELECT
  uf,
  preco_medio,
  qtd_postos
FROM workspace.anp_combustiveis.gold_preco_medio_mensal_uf_produto
WHERE produto = :produto
  AND unidade_medida = 'R$ / LITRO'
  AND mes = (SELECT MAX(mes) FROM workspace.anp_combustiveis.gold_preco_medio_mensal_uf_produto)
ORDER BY preco_medio DESC;
