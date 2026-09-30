# ==============================================================================
# SILVER.CLIENTES
# ==============================================================================
# Esta tabela limpa e enriquece os dados de clientes da bronze.
#
# REGRAS DE NEGÓCIO:
# 1. Remove duplicatas por id_cliente.
# 2. Guarda o nome original em nome_original (para auditoria).
# 3. Remove pronomes de tratamento do início do nome (Sr., Sra., Srta., Dr., Dra.)
#    e aplica formato título (initcap).
# 4. Estado (UF) em maiúsculas.
# 5. Adiciona nome_estado e regiao a partir de mapeamento fixo das 27 UFs do IBGE.
#    Não existe tabela de estados na bronze, então o mapeamento é declarado aqui.
#
# QUALIDADE:
# - Fail: id_cliente preenchido e regiao preenchida (todo cliente deve ter
#   uma região válida atribuída).
# ==============================================================================

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Mapeamento das 27 UFs do IBGE para nome do estado e região
ESTADOS_IBGE = {
    "AC": ("Acre", "Norte"),
    "AL": ("Alagoas", "Nordeste"),
    "AP": ("Amapá", "Norte"),
    "AM": ("Amazonas", "Norte"),
    "BA": ("Bahia", "Nordeste"),
    "CE": ("Ceará", "Nordeste"),
    "DF": ("Distrito Federal", "Centro-Oeste"),
    "ES": ("Espírito Santo", "Sudeste"),
    "GO": ("Goiás", "Centro-Oeste"),
    "MA": ("Maranhão", "Nordeste"),
    "MT": ("Mato Grosso", "Centro-Oeste"),
    "MS": ("Mato Grosso do Sul", "Centro-Oeste"),
    "MG": ("Minas Gerais", "Sudeste"),
    "PA": ("Pará", "Norte"),
    "PB": ("Paraíba", "Nordeste"),
    "PR": ("Paraná", "Sul"),
    "PE": ("Pernambuco", "Nordeste"),
    "PI": ("Piauí", "Nordeste"),
    "RJ": ("Rio de Janeiro", "Sudeste"),
    "RN": ("Rio Grande do Norte", "Nordeste"),
    "RS": ("Rio Grande do Sul", "Sul"),
    "RO": ("Rondônia", "Norte"),
    "RR": ("Roraima", "Norte"),
    "SC": ("Santa Catarina", "Sul"),
    "SP": ("São Paulo", "Sudeste"),
    "SE": ("Sergipe", "Nordeste"),
    "TO": ("Tocantins", "Norte"),
}


@dp.materialized_view(name="silver.clientes")
@dp.expect_all_or_fail({
    "id_cliente_preenchido": "id_cliente IS NOT NULL",
    "regiao_preenchida": "regiao IS NOT NULL",
})
def clientes():
    bronze = spark.read.table("bronze.clientes").dropDuplicates(["id_cliente"])

    # Cria um DataFrame de mapeamento UFs -> (nome_estado, regiao)
    estados_rows = [
        (uf, nome, regiao) for uf, (nome, regiao) in ESTADOS_IBGE.items()
    ]
    estados_df = spark.createDataFrame(
        estados_rows, ["estado", "nome_estado", "regiao"]
    )

    return (
        bronze
        .withColumn("nome_original", F.col("nome_cliente"))
        .withColumn(
            "nome_cliente",
            F.initcap(
                F.trim(
                    F.regexp_replace(
                        F.col("nome_cliente"),
                        r"^(Sr\.|Sra\.|Srta\.|Dr\.|Dra\.)\s+",
                        "",
                    )
                )
            ),
        )
        .withColumn("estado", F.upper(F.col("estado")))
        .join(estados_df, on="estado", how="left")
        .select(
            "id_cliente",
            "nome_original",
            "nome_cliente",
            "estado",
            "nome_estado",
            "regiao",
            "pais",
            "data_cadastro",
        )
    )
