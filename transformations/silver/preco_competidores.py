# ==============================================================================
# SILVER.PRECO_COMPETIDORES
# ==============================================================================
# Esta tabela limpa e enriquece os dados de preços de concorrentes.
#
# REGRAS DE NEGÓCIO:
# 1. Remove duplicatas pela chave composta (id_produto + nome_concorrente).
# 2. Converte preco_concorrente para DECIMAL(10,2).
# 3. Converte data_coleta de texto (string) para timestamp.
# 4. Marca preco_suspeito = true quando o preço do concorrente é menor que 60%
#    do nosso preco_atual (indica erro de digitação ou dado incorreto).
#
# QUALIDADE:
# - Fail: id_produto preenchido e preço > 0.
# - Warn: preco_plausivel = NOT preco_suspeito (preços suspeitos são mantidos
#   mas marcados para investigação).
# ==============================================================================

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


@dp.materialized_view(name="silver.preco_competidores")
@dp.expect_all_or_fail({
    "id_produto_preenchido": "id_produto IS NOT NULL",
    "preco_positivo": "preco_concorrente > 0",
})
@dp.expect("preco_plausivel", "NOT preco_suspeito")
def preco_competidores():
    # Lê o preço atual dos produtos da silver (já limpa e sem duplicatas)
    produtos = spark.read.table("silver.produtos").select(
        "id_produto",
        F.col("preco_atual").alias("preco_atual_produto"),
    )

    return (
        spark.read.table("bronze.preco_competidores")
        .dropDuplicates(["id_produto", "nome_concorrente"])
        .withColumn(
            "preco_concorrente",
            F.col("preco_concorrente").cast(DecimalType(10, 2)),
        )
        .withColumn("data_coleta", F.to_timestamp("data_coleta"))
        .join(produtos, on="id_produto", how="left")
        .withColumn(
            "preco_suspeito",
            F.col("preco_concorrente") < (F.col("preco_atual_produto") * 0.60),
        )
        .select(
            "id_produto",
            "nome_concorrente",
            "preco_concorrente",
            "data_coleta",
            "preco_suspeito",
        )
    )
