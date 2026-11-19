# Requirements: Pipeline Medallion de preços de combustíveis (ANP) no Databricks

## Introdução

Pipeline de dados ponta a ponta no Databricks Free Edition, com arquitetura Medallion
(bronze → silver → gold), Delta Lake, Unity Catalog, PySpark e Spark SQL. A fonte é a
Série Histórica de Preços de Combustíveis e de GLP da ANP (dados abertos, CSV mensais).
O resultado é um conjunto de tabelas gold analíticas e um dashboard AI/BI.

## Requisitos

### R1. Ingestão (bronze)
1. O sistema DEVE baixar os CSVs mensais da ANP por um script local (`scripts/download_data.py`),
   porque o compute serverless do Free Edition tem acesso restrito à internet.
2. O script DEVE permitir fazer upload dos arquivos para um Volume do Unity Catalog usando
   o Databricks CLI, com credenciais vindas de variáveis de ambiente ou perfil do CLI.
3. A tabela bronze DEVE preservar os valores brutos (todas as colunas como string) e
   adicionar `_ingested_at` e `_source_file`.
4. Reexecutar a ingestão do mesmo arquivo NÃO PODE duplicar linhas (substitui só as linhas do arquivo).

### R2. Limpeza (silver)
1. Nomes de colunas em snake_case; strings aparadas e padronizadas; vazio vira nulo.
2. Tipos corretos: `data_coleta` como `date`, preços como `decimal` (vírgula decimal → ponto).
3. Duplicados removidos pela chave de negócio (`cnpj`, `produto`, `data_coleta`).
4. Checagens de qualidade (ver R3); registros inválidos vão para a tabela de quarentena
   com o motivo da falha.
5. A escrita DEVE ser incremental e idempotente (`MERGE INTO`).

### R3. Qualidade de dados
1. `valor_venda` não nulo e > 0 (e abaixo de um limite de sanidade configurável).
2. `uf` pertence às 27 UFs válidas.
3. `data_coleta` não nula (parse válido) e não futura.
4. `produto` e `cnpj` não nulos.
5. Cada linha da quarentena DEVE listar todas as regras que falhou.

### R4. Camada gold
1. Mínimo de 3 tabelas analíticas; este projeto entrega 4:
   preço médio mensal por UF/produto, variação % mês a mês, ranking de municípios
   (mais caros/baratos) e razão etanol/gasolina por UF.
2. A escrita DEVE ser idempotente (rodar duas vezes não duplica dados).

### R5. Orquestração
1. Um Databricks Job encadeia bronze → silver → gold, definido como código com
   Databricks Asset Bundles (`databricks.yml`).

### R6. Qualidade de código
1. A lógica de transformação fica em funções Python em `src/`; notebooks só chamam essas funções.
2. Testes `pytest` com PySpark local cobrem as funções de transformação e qualidade.
3. GitHub Actions executa `ruff` e `pytest` a cada push.
4. Nenhuma credencial no código ou no repositório.

### R7. Consumo
1. Queries SQL para um AI/BI Dashboard ficam em `sql/`, sobre as tabelas gold.

### R8. Documentação
1. README em português com resumo em inglês no topo, diagrama Mermaid, stack, passo a passo
   (incluindo criação da conta Free Edition), decisões técnicas, regras de qualidade,
   exemplos de insights e espaço para prints.
2. Insights no README só podem vir de queries realmente executadas; onde não houver
   execução, usar placeholders.
3. Rascunho de post para o LinkedIn (PT-BR).
