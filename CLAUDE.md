# Projeto Vendas - E-commerce Brasileiro

## Convenções

- Catálogo: `projetovendas`. Nunca usar outro catálogo.
- Schemas: `bronze`, `silver`, `gold`.
- Nomes de tabelas e colunas em português, snake_case, sem acento.
- Silver em Python (`from pyspark import pipelines as dp`), gold em SQL.
- Um arquivo por tabela: `transformations/silver/<tabela>.py` e `transformations/gold/<tabela>.sql`.
- Todas as tabelas são materialized views com leitura batch (`spark.read.table`), nunca streaming table, porque a bronze é sobrescrita a cada execução.
- Pipeline serverless, catálogo `projetovendas`, schema padrão `silver`. Golds publicadas como `gold.<tabela>`. Nomes no código sempre `schema.tabela`, sem catálogo.
- Cada arquivo começa com comentários explicando o PORQUÊ das regras, em português.
- Dinheiro sempre `DECIMAL(10,2)`.
- Problema de qualidade conhecido é MARCADO em uma coluna e medido com `@dp.expect` (warn). Nunca descartar linhas: apagar vendas mudaria a receita.
- `@dp.expect_all_or_fail` só para o que nunca pode acontecer.
- Sempre rodar `databricks bundle validate --strict` antes do deploy.
## Regras para a camada gold

- SQL, um arquivo por tabela em `transformations/gold/`, `CREATE OR REFRESH MATERIALIZED VIEW gold.<tabela>`.
- Declare TODAS as colunas entre parenteses com tipo e `COMMENT` (sem o tipo, o comentario e ignorado), e `COMMENT` na tabela dizendo quando usar a tabela. Comentarios em portugues, com unidade (R$), regra de calculo e avisos que evitem erro do Genie.
- Inclua TODAS as vendas, inclusive de produto nao cadastrado: dinheiro que entrou e receita.
- Periodo dos dados: 13/12/2025 a 11/01/2026.
- Toda gold nova ganha testes no notebook `testes/testes_qualidade_nb`.
## Numeros de referencia (camada gold)

- Clientes: 50 (45 VIP, 4 TOP_TIER, 1 REGULAR).
- Receita total: R$ 1.482.116,03.
- Vendas: 3.020 (2.155 ecommerce, 865 loja_fisica).
- Maior cliente: Henrique Da Conceição (DF, R$ 45.193,19).
- Produtos com preco de concorrente: 215 (35 MAIS_CARO_QUE_TODOS, 6 MAIS_BARATO_QUE_TODOS, 92 ACIMA_DA_MEDIA, 76 ABAIXO_DA_MEDIA, 6 NA_MEDIA).
- Produtos com preco suspeito: 15.
- Periodo: 13/12/2025 a 11/01/2026.
