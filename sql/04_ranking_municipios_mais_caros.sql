-- Dataset 4: 10 municípios mais caros no mês mais recente (tabela/barras).
-- Parâmetro do dashboard: :produto. Só municípios com pelo menos 3 postos (regra da gold).
SELECT
  rank_mais_caro AS posicao,
  municipio,
  uf,
  preco_medio,
  qtd_postos
FROM workspace.anp_combustiveis.gold_ranking_municipios_mensal
WHERE produto = :produto
  AND unidade_medida = 'R$ / LITRO'
  AND mes = (SELECT MAX(mes) FROM workspace.anp_combustiveis.gold_ranking_municipios_mensal)
  AND rank_mais_caro <= 10
ORDER BY rank_mais_caro;
