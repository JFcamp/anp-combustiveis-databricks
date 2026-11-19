-- Verificação: linhas por camada e proporção de registros em quarentena.
SELECT 'bronze' AS camada, COUNT(*) AS linhas FROM workspace.anp_combustiveis.bronze_anp_precos
UNION ALL
SELECT 'silver', COUNT(*) FROM workspace.anp_combustiveis.silver_anp_precos
UNION ALL
SELECT 'quarentena', COUNT(*) FROM workspace.anp_combustiveis.silver_anp_precos_quarentena;
