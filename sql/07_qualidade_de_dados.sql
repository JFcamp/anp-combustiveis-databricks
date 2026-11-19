-- Dataset 7: registros em quarentena por regra de qualidade que falhou.
-- Uma linha pode falhar em várias regras e conta uma vez em cada.
SELECT
  failure AS regra,
  COUNT(*) AS registros
FROM workspace.anp_combustiveis.silver_anp_precos_quarentena
LATERAL VIEW EXPLODE(_dq_failures) f AS failure
GROUP BY failure
ORDER BY registros DESC;
