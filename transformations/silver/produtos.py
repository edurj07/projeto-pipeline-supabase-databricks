# ==============================================================================
# SILVER.PRODUTOS
# ==============================================================================
# Esta tabela limpa e enriquece os dados de produtos da bronze.
#
# REGRAS DE NEGÓCIO:
# 1. Remove duplicatas por id_produto (a bronze pode ter registros duplicados
#    se for sobrescrita parcialmente).
# 2. Aplica trim() no nome_produto para remover espaços extras.
# 3. Converte preco_atual para DECIMAL(10,2) para garantir precisão monetária.
# 4. Cria faixa_preco: PREMIUM (> 1000), MEDIO (> 500) ou BASICO (<= 500).
#
# QUALIDADE:
# - Fail: id_produto preenchido e preco_atual > 0 (não podem existir produtos
#   sem ID ou com preço inválido).
# ==============================================================================

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


@dp.materialized_view(name="silver.produtos")
@dp.expect_all_or_fail({
    "id_produto_preenchido": "id_produto IS NOT NULL",
    "preco_atual_positivo": "preco_atual > 0",
})
def produtos():
    return (
        spark.read.table("bronze.produtos")
        .dropDuplicates(["id_produto"])
        .withColumn("nome_produto", F.trim(F.col("nome_produto")))
        .withColumn("preco_atual", F.col("preco_atual").cast(DecimalType(10, 2)))
        .withColumn(
            "faixa_preco",
            F.when(F.col("preco_atual") > 1000, "PREMIUM")
            .when(F.col("preco_atual") > 500, "MEDIO")
            .otherwise("BASICO"),
        )
        .select(
            "id_produto",
            "nome_produto",
            "categoria",
            "marca",
            "preco_atual",
            "data_criacao",
            "faixa_preco",
        )
    )
