# Rascunho de post para o LinkedIn

> Antes de publicar: rode o pipeline no seu workspace, monte o dashboard e troque os trechos
> entre `[colchetes]`. Não publique números que você não viu nos resultados das queries.

---

Montei um pipeline de dados completo no Databricks com os preços de combustíveis da ANP.

O problema: a ANP publica os preços coletados nos postos todo mês, mas em CSVs grandes, com
vírgula decimal, datas em dd/MM/yyyy e colunas com espaço no nome. Dá para analisar, mas não
sem antes limpar.

O que construí:

• Arquitetura Medallion (bronze, silver, gold) com Delta Lake e Unity Catalog
• PySpark e Spark SQL, com a lógica em módulos Python testados (pytest)
• Regras de qualidade de dados: registros inválidos vão para uma tabela de quarentena com o motivo da falha
• Escrita idempotente: MERGE INTO na silver e replaceWhere na bronze. Rodar duas vezes não duplica nada
• Job orquestrado como código com Databricks Asset Bundles
• CI no GitHub Actions com ruff e pytest
• 4 tabelas gold (preço médio mensal, variação mês a mês, ranking de municípios, etanol vs. gasolina) alimentando um dashboard AI/BI

Um detalhe técnico que valeu a pena: o serverless roda com ANSI mode, onde um CAST inválido
derruba o job. Usei try_cast e try_to_timestamp, e o dado ruim vira nulo e cai na quarentena
com o valor original preservado.

[Opcional, só com dado real das suas queries: um insight que você viu no dashboard.]

Tudo rodando no Databricks Free Edition. O código, o diagrama e o passo a passo estão no README:
https://github.com/JFcamp/anp-combustiveis-databricks

Feedbacks são bem-vindos.

#DataEngineering #Databricks #PySpark #DeltaLake #EngenhariaDeDados
