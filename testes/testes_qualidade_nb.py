# Databricks notebook source
# DBTITLE 1,Header
# ==============================================================================
# TESTES DE QUALIDADE DA CAMADA SILVER
# ==============================================================================
# Este notebook valida a qualidade dos dados na camada silver.
# Cada teste conta linhas com problema; se algum encontrar problemas,
# o notebook falha com AssertionError e mostra uma tabela com o resultado.
# ==============================================================================

# COMMAND ----------

# DBTITLE 1,Widget catalogo
dbutils.widgets.text("catalogo", "projetovendas")
catalogo = dbutils.widgets.get("catalogo")

# COMMAND ----------

# DBTITLE 1,Lista de resultados
resultados = []

# COMMAND ----------

# DBTITLE 1,TESTE 1: produtos chave única
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

# DBTITLE 1,TESTE 2: clientes chave única
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

# DBTITLE 1,TESTE 3: preco_competidores chave única
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

# DBTITLE 1,TESTE 4: vendas chave única
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

# DBTITLE 1,TESTE 5: receita = qtd × preco
# TESTE 5: receita = quantidade × preco_unitario
receita_errada = spark.sql(f"""
    SELECT COUNT(*) as erro
    FROM {catalogo}.silver.vendas
    WHERE receita != CAST(quantidade * preco_unitario AS DECIMAL(10,2))
""")
total_receita_errada = receita_errada.collect()[0]["erro"]
resultados.append(("vendas - receita = quantidade × preco_unitario", total_receita_errada, total_receita_errada == 0))

# COMMAND ----------

# DBTITLE 1,TESTE 6: produto não cadastrado < 1%
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

# DBTITLE 1,Header Gold


# COMMAND ----------

# DBTITLE 1,Header Gold
# ==============================================================================
# TESTES DE QUALIDADE DA CAMADA GOLD
# ==============================================================================

# COMMAND ----------

# DBTITLE 1,TESTE 7: CS receita = silver
# TESTE 7: gold.clientes_segmentacao - receita total = silver.vendas
receita_silver = spark.sql(f"SELECT SUM(receita) as total FROM {catalogo}.silver.vendas").collect()[0]["total"]
receita_gold_cs = spark.sql(f"SELECT SUM(receita) as total FROM {catalogo}.gold.clientes_segmentacao").collect()[0]["total"]
diff_cs = abs(receita_silver - receita_gold_cs) if receita_silver and receita_gold_cs else 0
resultados.append((f"gold.clientes_segmentacao - receita = silver.vendas (diff R$ {diff_cs:.2f})", diff_cs, diff_cs < 0.01))

# COMMAND ----------

# DBTITLE 1,TESTE 8: CS integridade
# TESTE 8: gold.clientes_segmentacao - id_cliente único, segmento válido, VIP >= 22000
dup_cs = spark.sql(f"""
    SELECT COUNT(*) as dup
    FROM (SELECT id_cliente, COUNT(*) as cnt FROM {catalogo}.gold.clientes_segmentacao GROUP BY id_cliente HAVING COUNT(*) > 1)
""").collect()[0]["dup"]
resultados.append(("gold.clientes_segmentacao - id_cliente único", dup_cs, dup_cs == 0))

seg_invalido = spark.sql(f"""
    SELECT COUNT(*) as cnt FROM {catalogo}.gold.clientes_segmentacao
    WHERE segmento_cliente NOT IN ('VIP', 'TOP_TIER', 'REGULAR')
""").collect()[0]["cnt"]
resultados.append(("gold.clientes_segmentacao - segmento válido", seg_invalido, seg_invalido == 0))

vip_baixo = spark.sql(f"""
    SELECT COUNT(*) as cnt FROM {catalogo}.gold.clientes_segmentacao
    WHERE segmento_cliente = 'VIP' AND receita < 22000
""").collect()[0]["cnt"]
resultados.append(("gold.clientes_segmentacao - nenhum VIP < R$ 22.000", vip_baixo, vip_baixo == 0))

# COMMAND ----------

# DBTITLE 1,TESTE 9: colunas com comentário
# TESTE 9: Toda coluna do schema gold com comentário
colunas_sem_comment = spark.sql(f"""
    SELECT table_name, column_name
    FROM {catalogo}.information_schema.columns
    WHERE table_schema = 'gold'
      AND NOT STARTSWITH(table_name, '__materialization')
      AND (comment IS NULL OR comment = '')
""")
total_sem_comment = colunas_sem_comment.count()
resultados.append(("gold - toda coluna com comentário", total_sem_comment, total_sem_comment == 0))
if total_sem_comment > 0:
    display(colunas_sem_comment)

# COMMAND ----------

# DBTITLE 1,TESTE 10: receita golds = silver
# TESTE 10: receita total de vendas_temporais, vendas_produtos e vendas_detalhadas = silver.vendas
for tabela in ['vendas_temporais', 'vendas_produtos', 'vendas_detalhadas']:
    receita_t = spark.sql(f"SELECT SUM(receita) as total FROM {catalogo}.gold.{tabela}").collect()[0]["total"]
    diff_t = abs(receita_silver - receita_t) if receita_silver and receita_t else 0
    resultados.append((f"gold.{tabela} - receita = silver.vendas (diff R$ {diff_t:.2f})", diff_t, diff_t < 0.01))

# COMMAND ----------

# DBTITLE 1,TESTE 11: vendas_detalhadas linhas
# TESTE 11: vendas_detalhadas - mesmo nº de linhas e id_venda único
linhas_silver = spark.sql(f"SELECT COUNT(*) as cnt FROM {catalogo}.silver.vendas").collect()[0]["cnt"]
linhas_gold = spark.sql(f"SELECT COUNT(*) as cnt FROM {catalogo}.gold.vendas_detalhadas").collect()[0]["cnt"]
diff_linhas = abs(linhas_silver - linhas_gold)
resultados.append((f"gold.vendas_detalhadas - mesmo nº de linhas ({linhas_silver} vs {linhas_gold})", diff_linhas, diff_linhas == 0))

dup_vd = spark.sql(f"""
    SELECT COUNT(*) as dup
    FROM (SELECT id_venda, COUNT(*) as cnt FROM {catalogo}.gold.vendas_detalhadas GROUP BY id_venda HAVING COUNT(*) > 1)
""").collect()[0]["dup"]
resultados.append(("gold.vendas_detalhadas - id_venda único", dup_vd, dup_vd == 0))

# COMMAND ----------

# DBTITLE 1,TESTE 12: vendas_detalhadas segmento
# TESTE 12: vendas_detalhadas - toda venda com segmento e região
sem_seg = spark.sql(f"""
    SELECT COUNT(*) as cnt FROM {catalogo}.gold.vendas_detalhadas
    WHERE segmento_cliente IS NULL OR regiao IS NULL
""").collect()[0]["cnt"]
resultados.append(("gold.vendas_detalhadas - toda venda com segmento e região", sem_seg, sem_seg == 0))

# COMMAND ----------

# DBTITLE 1,TESTE 13: precos id único
# TESTE 13: gold.precos_competitividade - id_produto único
dup_pc = spark.sql(f"""
    SELECT COUNT(*) as dup
    FROM (SELECT id_produto, COUNT(*) as cnt FROM {catalogo}.gold.precos_competitividade GROUP BY id_produto HAVING COUNT(*) > 1)
""").collect()[0]["dup"]
resultados.append(("gold.precos_competitividade - id_produto único", dup_pc, dup_pc == 0))

# COMMAND ----------

# DBTITLE 1,Mostrar resultados
# Mostrar resultados
print("=" * 80)
print("RESULTADOS DOS TESTES DE QUALIDADE")
print("=" * 80)

resultado_df = spark.createDataFrame(
    [(nome, float(valor), "PASS" if ok else "FAIL") for nome, valor, ok in resultados],
    ["teste", "valor", "status"]
)
display(resultado_df)

# COMMAND ----------

# DBTITLE 1,Verificar falhas
# Verificar se algum teste falhou
testes_falharam = [r for r in resultados if not r[2]]
if testes_falharam:
    nomes_falha = ", ".join(r[0] for r in testes_falharam)
    raise AssertionError(f"Testes falharam: {nomes_falha}")
else:
    print("Todos os testes passaram!")

# COMMAND ----------

