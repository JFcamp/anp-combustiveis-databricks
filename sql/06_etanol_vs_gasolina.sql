-- Dataset 6: razão etanol/gasolina por UF no mês mais recente.
-- razao <= 0,70 indica que o etanol compensa (regra prática de 70%).
SELECT
  uf,
  preco_etanol,
  preco_gasolina,
  razao,
  etanol_compensa
FROM workspace.anp_combustiveis.gold_razao_etanol_gasolina_uf
WHERE mes = (SELECT MAX(mes) FROM workspace.anp_combustiveis.gold_razao_etanol_gasolina_uf)
ORDER BY razao;
