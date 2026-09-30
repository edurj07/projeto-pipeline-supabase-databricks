# ==============================================================================
# SILVER.VENDAS
# ==============================================================================
# Esta tabela limpa e enriquece os dados de vendas da bronze.
#
# REGRAS DE NEGÓCIO:
# 1. Remove duplicatas por id_venda.
# 2. Converte preco_unitario para DECIMAL(10,2).
# 3. Calcula receita = quantidade × preco_unitario em DECIMAL(10,2).
# 4. Extrai data (date), hora (0-23), dia_semana_num (1=domingo...7=sábado)
#    e dia_semana em português.
# 5. produto_cadastrado = false quando o id_produto não existe em silver.produtos.
# 6. venda_antes_do_cadastro = true quando data_venda < data_criacao do produto.
#
# QUALIDADE:
# - Fail: id_venda, data_venda, id_cliente, id_produto, quantidade e preco_unitario
#   preenchidos; quantidade > 0; preco_unitario > 0; canal_venda válido.
# - Warn: produto_cadastrado (marca vendas de produto não cadastrado).
# - Warn: venda_depois_do_cadastro = NOT venda_antes_do_cadastro.
# ==============================================================================

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


@dp.materialized_view(name="silver.vendas")
@dp.expect_all_or_fail({
    "id_venda_preenchido": "id_venda IS NOT NULL",
    "data_venda_preenchida": "data_venda IS NOT NULL",
    "id_cliente_preenchido": "id_cliente IS NOT NULL",
    "id_produto_preenchido": "id_produto IS NOT NULL",
    "quantidade_preenchida": "quantidade IS NOT NULL",
    "preco_unitario_preenchido": "preco_unitario IS NOT NULL",
    "quantidade_positiva": "quantidade > 0",
    "preco_unitario_positivo": "preco_unitario > 0",
    "canal_venda_valido": "canal_venda IN ('ecommerce', 'loja_fisica')",
})
@dp.expect("produto_cadastrado", "produto_cadastrado = true")
@dp.expect("venda_depois_do_cadastro", "NOT venda_antes_do_cadastro")
def vendas():
    # Lê os produtos da silver para verificar cadastro e data de criação
    produtos = spark.read.table("silver.produtos").select(
        "id_produto",
        "data_criacao",
    )

    return (
        spark.read.table("bronze.vendas")
        .dropDuplicates(["id_venda"])
        .withColumn(
            "preco_unitario",
            F.col("preco_unitario").cast(DecimalType(10, 2)),
        )
        .withColumn(
            "receita",
            (F.col("quantidade") * F.col("preco_unitario")).cast(DecimalType(10, 2)),
        )
        .withColumn("data", F.to_date(F.col("data_venda")))
        .withColumn("hora", F.hour(F.col("data_venda")))
        .withColumn("dia_semana_num", F.dayofweek(F.col("data_venda")))
        .withColumn(
            "dia_semana",
            F.when(F.col("dia_semana_num") == 1, F.lit("Domingo"))
            .when(F.col("dia_semana_num") == 2, F.lit("Segunda"))
            .when(F.col("dia_semana_num") == 3, F.lit("Terça"))
            .when(F.col("dia_semana_num") == 4, F.lit("Quarta"))
            .when(F.col("dia_semana_num") == 5, F.lit("Quinta"))
            .when(F.col("dia_semana_num") == 6, F.lit("Sexta"))
            .when(F.col("dia_semana_num") == 7, F.lit("Sábado")),
        )
        .join(produtos, on="id_produto", how="left")
        .withColumn("produto_cadastrado", F.col("data_criacao").isNotNull())
        .withColumn(
            "venda_antes_do_cadastro",
            F.col("data_criacao").isNotNull()
            & (F.col("data_venda") < F.col("data_criacao")),
        )
        .select(
            "id_venda",
            "data_venda",
            "data",
            "hora",
            "dia_semana_num",
            "dia_semana",
            "id_cliente",
            "id_produto",
            "canal_venda",
            "quantidade",
            "preco_unitario",
            "receita",
            "produto_cadastrado",
            "venda_antes_do_cadastro",
        )
    )
