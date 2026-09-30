# Databricks notebook source
# ==============================================================================
# TESTES DE QUALIDADE DA CAMADA SILVER
# ==============================================================================
# Este notebook valida a qualidade dos dados na camada silver.
# Cada teste conta linhas com problema; se algum encontrar problemas,
# o notebook falha com AssertionError e mostra uma tabela com o resultado.
# ==============================================================================

# COMMAND ----------

dbutils.widgets.text("catalogo", "projetovendas")
catalogo = dbutils.widgets.get("catalogo")

# COMMAND ----------

resultados = []

# COMMAND ----------

# TESTE 1: Chave única - silver.produtos (id_produto)
duplicatas_produtos = spark.sql(f"""
    SELECT COUNT(*) as duplicatas
    FROM (
        SELECT id_produto, COUNT(*) as cnt
        FROM {catalogo}.silver.produtos
        GROUP BY id_produto
        HAVING COUNT(*) > 1
    )
""")
total_duplicatas_produtos = duplicatas_produtos.collect()[0]["duplicatas"]
resultados.append(("produtos - chave única (id_produto)", total_duplicatas_produtos, total_duplicatas_produtos == 0))

# COMMAND ----------

# TESTE 2: Chave única - silver.clientes (id_cliente)
duplicatas_clientes = spark.sql(f"""
    SELECT COUNT(*) as duplicatas
    FROM (
        SELECT id_cliente, COUNT(*) as cnt
        FROM {catalogo}.silver.clientes
        GROUP BY id_cliente
        HAVING COUNT(*) > 1
    )
""")
total_duplicatas_clientes = duplicatas_clientes.collect()[0]["duplicatas"]
resultados.append(("clientes - chave única (id_cliente)", total_duplicatas_clientes, total_duplicatas_clientes == 0))

# COMMAND ----------

# TESTE 3: Chave única - silver.preco_competidores (id_produto + nome_concorrente)
duplicatas_preco = spark.sql(f"""
    SELECT COUNT(*) as duplicatas
    FROM (
        SELECT id_produto, nome_concorrente, COUNT(*) as cnt
        FROM {catalogo}.silver.preco_competidores
        GROUP BY id_produto, nome_concorrente
        HAVING COUNT(*) > 1
    )
""")
total_duplicatas_preco = duplicatas_preco.collect()[0]["duplicatas"]
resultados.append(("preco_competidores - chave única (id_produto + nome_concorrente)", total_duplicatas_preco, total_duplicatas_preco == 0))

# COMMAND ----------

# TESTE 4: Chave única - silver.vendas (id_venda)
duplicatas_vendas = spark.sql(f"""
    SELECT COUNT(*) as duplicatas
    FROM (
        SELECT id_venda, COUNT(*) as cnt
        FROM {catalogo}.silver.vendas
        GROUP BY id_venda
        HAVING COUNT(*) > 1
    )
""")
total_duplicatas_vendas = duplicatas_vendas.collect()[0]["duplicatas"]
resultados.append(("vendas - chave única (id_venda)", total_duplicatas_vendas, total_duplicatas_vendas == 0))

# COMMAND ----------

# TESTE 5: receita = quantidade × preco_unitario
receita_errada = spark.sql(f"""
    SELECT COUNT(*) as erro
    FROM {catalogo}.silver.vendas
    WHERE receita != CAST(quantidade * preco_unitario AS DECIMAL(10,2))
""")
total_receita_errada = receita_errada.collect()[0]["erro"]
resultados.append(("vendas - receita = quantidade × preco_unitario", total_receita_errada, total_receita_errada == 0))

# COMMAND ----------

# TESTE 6: Vendas de produto não cadastrado abaixo de 1% do total
total_vendas = spark.sql(f"SELECT COUNT(*) as total FROM {catalogo}.silver.vendas").collect()[0]["total"]
vendas_nao_cadastrado = spark.sql(f"""
    SELECT COUNT(*) as qtd
    FROM {catalogo}.silver.vendas
    WHERE produto_cadastrado = false
""").collect()[0]["qtd"]
pct_nao_cadastrado = (vendas_nao_cadastrado / total_vendas * 100) if total_vendas > 0 else 100
resultados.append((f"vendas - produto não cadastrado < 1% ({pct_nao_cadastrado:.2f}%)", vendas_nao_cadastrado, pct_nao_cadastrado < 1))

# COMMAND ----------

# Mostrar resultados
print("=" * 80)
print("RESULTADOS DOS TESTES DE QUALIDADE")
print("=" * 80)

resultado_df = spark.createDataFrame(
    [(nome, valor, "PASS" if ok else "FAIL") for nome, valor, ok in resultados],
    ["teste", "valor", "status"]
)
display(resultado_df)

# COMMAND ----------

# Verificar se algum teste falhou
testes_falharam = [r for r in resultados if not r[2]]
if testes_falharam:
    nomes_falha = ", ".join(r[0] for r in testes_falharam)
    raise AssertionError(f"Testes falharam: {nomes_falha}")
else:
    print("Todos os testes passaram!")
