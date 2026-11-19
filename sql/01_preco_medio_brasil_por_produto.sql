-- Dataset 1: evolução do preço médio nacional por produto (gráfico de linhas).
-- Média ponderada pelo nº de coletas de cada UF, só produtos vendidos por litro.
-- Se seu catalog/schema forem outros, troque `workspace.anp_combustiveis`.
SELECT
  mes,
  produto,
  ROUND(SUM(preco_medio * qtd_coletas) / SUM(qtd_coletas), 2) AS preco_medio_brasil
FROM workspace.anp_combustiveis.gold_preco_medio_mensal_uf_produto
WHERE unidade_medida = 'R$ / LITRO'
GROUP BY mes, produto
ORDER BY mes, produto;
