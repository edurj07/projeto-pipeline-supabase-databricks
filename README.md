# 🛒 Projeto Vendas — Plataforma de Dados E-commerce Brasileiro

## 📋 Visão Geral

Plataforma de dados completa para e-commerce brasileiro construída no **Databricks Lakehouse**, cobrindo toda a jornada de dados desde a ingestão de arquivos **Parquet** armazenados no **Supabase** (simulando uma conexão com **AWS S3**) até dashboards executivos publicados para três diretorias. O projeto implementa uma arquitetura **medallion** (bronze → silver → gold) com pipeline declarativo Spark (SDP), testes automatizados de qualidade e republicação automática de dashboards AI/BI.

### 🔗 Sobre a Fonte de Dados (Simulação S3)

Os dados originais (vendas, clientes, produtos e preços de concorrentes) estão em formato **Parquet** armazenados no **Supabase** (PostgreSQL). Esta configuração **simula o padrão de ingestão de dados do AWS S3** — um dos cenários mais comuns em arquiteturas Lakehouse corporativas — onde arquivos Parquet são lidos de um bucket de object storage e ingeridos na camada bronze do Databricks. O notebook auxiliar `conexão com S3.ipynb` documenta a configuração dessa conexão. Esta abordagem demonstra competência em:
- **Leitura de arquivos Parquet** a partir de object storage
- **Ingestão batch** com Auto Loader / `spark.read.parquet()`
- **Padrão de arquitetura S3 → Bronze** amplamente utilizado em produção

### ✨ Destaques

- **Arquitetura Lakehouse medallion** com 3 camadas (bronze, silver, gold)
- **Pipeline declarativo Spark (SDP)** com materialized views
- **3 dashboards AI/BI publicados** para tomada de decisão estratégica
- **Job orquestrado** com 3 tasks: pipeline → testes → refresh de dashboards
- **Qualidade de dados** com regras `@dp.expect` e testes automatizados
- **Republicação automática** de dashboards via Databricks SDK
- **Catálogo Unity Catalog** com governança centralizada

---

## 🏗️ Arquitetura

```
┌─────────────────────────────────────────────────────────────────┐
│                    SUPABASE (PostgreSQL)                          │
│              Fonte: arquivos Parquet (simula AWS S3)              │
│              vendas, clientes, produtos e concorrentes            │
└────────────────────────────┬────────────────────────────────────┘
                             │
                    ┌────────▼────────┐
                    │   INGESTÃO       │
                    │   (Bronze)       │
                    └────────┬────────┘
                             │
                    ┌────────▼────────┐
                    │  TRANSFORMAÇÃO   │
                    │   (Silver)       │
                    │  Python + SDP    │
                    └────────┬────────┘
                             │
                    ┌────────▼────────┐
                    │   AGREGAÇÃO      │
                    │    (Gold)        │
                    │   SQL MVs        │
                    └────────┬────────┘
                             │
              ┌──────────────┼──────────────┐
              │              │              │
     ┌────────▼───────┐ ┌────▼────────┐ ┌───▼──────────────┐
     │  DASHBOARD     │ │  DASHBOARD  │ │   DASHBOARD      │
     │  COMERCIAL     │ │   PRICING   │ │  CUSTOMER SUCCESS │
     └────────────────┘ └─────────────┘ └──────────────────┘
```

### Catálogo e Schemas

| Camada | Catálogo | Schema | Tecnologia | Descrição |
| --- | --- | --- | --- | --- |
| Bronze | `projetovendas` | `bronze` | Tabelas Delta | Cópia bruta do Supabase (Parquet, padrão S3) |
| Silver | `projetovendas` | `silver` | Materialized Views (Python) | Limpeza, enriquecimento e padronização |
| Gold | `projetovendas` | `gold` | Materialized Views (SQL) | Agregações para análise de negócio |

---

## 📦 Bronze — Ingestão de Dados (S3/Parquet)

A camada bronze recebe os dados diretamente do Supabase (simulando AWS S3) em formato **Parquet**. O notebook `conexão com S3.ipynb` usa a biblioteca `boto3` para conectar ao endpoint S3-compatível do Supabase, baixar os arquivos Parquet, converter para DataFrames Spark e gravar como tabelas Delta no catálogo `projetovendas.bronze`.

```python
import boto3
import io
import pandas as pd

# Conexão com Supabase S3 (simula AWS S3)
s3 = boto3.client(
    "s3",
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    region_name="us-west-2",
    endpoint_url="https://<projeto>.storage.supabase.co/storage/v1/s3"
)

# Le o arquivo Parquet do bucket e grava como tabela Delta na bronze
response = s3.get_object(Bucket="LakeDataEcommerce", Key="vendas.parquet")
pdf = pd.read_parquet(io.BytesIO(response["Body"].read()))
df = spark.createDataFrame(pdf)
df.write.format("delta").mode("overwrite").saveAsTable("projetovendas.bronze.vendas")
```

O mesmo padrão é aplicado para as 4 tabelas da bronze: `vendas`, `clientes`, `produtos` e `preco_competidores`.

---

## 🥈 Silver — Transformações Python (SDP)

A camada silver é implementada em **Python (PySpark)** usando o decorador `@dp.materialized_view` do Spark Declarative Pipelines. Cada arquivo define uma tabela silver com regras de qualidade `@dp.expect_all_or_fail` (fail) e `@dp.expect` (warn).

### silver.produtos
```python
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
        .withColumn("faixa_preco",
            F.when(F.col("preco_atual") > 1000, "PREMIUM")
            .when(F.col("preco_atual") > 500, "MEDIO")
            .otherwise("BASICO"))
        .select("id_produto", "nome_produto", "categoria", "marca",
                "preco_atual", "data_criacao", "faixa_preco")
    )
```

### silver.clientes
```python
@dp.materialized_view(name="silver.clientes")
@dp.expect_all_or_fail({
    "id_cliente_preenchido": "id_cliente IS NOT NULL",
    "regiao_preenchida": "regiao IS NOT NULL",
})
def clientes():
    bronze = spark.read.table("bronze.clientes").dropDuplicates(["id_cliente"])
    # Mapeamento das 27 UFs do IBGE para nome do estado e regiao
    estados_df = spark.createDataFrame(
        [(uf, nome, regiao) for uf, (nome, regiao) in ESTADOS_IBGE.items()],
        ["estado", "nome_estado", "regiao"])
    return (
        bronze
        .withColumn("nome_original", F.col("nome_cliente"))
        .withColumn("nome_cliente",
            F.initcap(F.trim(F.regexp_replace(F.col("nome_cliente"),
                r"^(Sr\.|Sra\.|Srta\.|Dr\.|Dra\.)\s+", ""))))
        .withColumn("estado", F.upper(F.col("estado")))
        .join(estados_df, on="estado", how="left")
        .select("id_cliente", "nome_original", "nome_cliente", "estado",
                "nome_estado", "regiao", "pais", "data_cadastro")
    )
```

### silver.vendas
```python
@dp.materialized_view(name="silver.vendas")
@dp.expect_all_or_fail({
    "id_venda_preenchido": "id_venda IS NOT NULL",
    "quantidade_positiva": "quantidade > 0",
    "preco_unitario_positivo": "preco_unitario > 0",
    "canal_venda_valido": "canal_venda IN ('ecommerce', 'loja_fisica')",
})
@dp.expect("produto_cadastrado", "produto_cadastrado = true")
def vendas():
    produtos = spark.read.table("silver.produtos").select("id_produto", "data_criacao")
    return (
        spark.read.table("bronze.vendas")
        .dropDuplicates(["id_venda"])
        .withColumn("preco_unitario", F.col("preco_unitario").cast(DecimalType(10, 2)))
        .withColumn("receita", (F.col("quantidade") * F.col("preco_unitario")).cast(DecimalType(10, 2)))
        .withColumn("data", F.to_date(F.col("data_venda")))
        .withColumn("hora", F.hour(F.col("data_venda")))
        .withColumn("dia_semana_num", F.dayofweek(F.col("data_venda")))
        .join(produtos, on="id_produto", how="left")
        .withColumn("produto_cadastrado", F.col("data_criacao").isNotNull())
        .select("id_venda", "data_venda", "data", "hora", "dia_semana_num",
                "id_cliente", "id_produto", "canal_venda", "quantidade",
                "preco_unitario", "receita", "produto_cadastrado")
    )
```

### silver.preco_competidores
```python
@dp.materialized_view(name="silver.preco_competidores")
@dp.expect_all_or_fail({
    "id_produto_preenchido": "id_produto IS NOT NULL",
    "preco_positivo": "preco_concorrente > 0",
})
@dp.expect("preco_plausivel", "NOT preco_suspeito")
def preco_competidores():
    produtos = spark.read.table("silver.produtos").select(
        "id_produto", F.col("preco_atual").alias("preco_atual_produto"))
    return (
        spark.read.table("bronze.preco_competidores")
        .dropDuplicates(["id_produto", "nome_concorrente"])
        .withColumn("preco_concorrente", F.col("preco_concorrente").cast(DecimalType(10, 2)))
        .withColumn("data_coleta", F.to_timestamp("data_coleta"))
        .join(produtos, on="id_produto", how="left")
        .withColumn("preco_suspeito",
            F.col("preco_concorrente") < (F.col("preco_atual_produto") * 0.60))
        .select("id_produto", "nome_concorrente", "preco_concorrente",
                "data_coleta", "preco_suspeito")
    )
```

---

## 🥇 Gold — Queries SQL (Materialized Views)

A camada gold é implementada em **SQL** usando `CREATE OR REFRESH MATERIALIZED VIEW`. Cada view agrega os dados da silver para análises de negócio, com comentários em todas as colunas e tipos explícitos (`DECIMAL(10,2)`, `BIGINT`, `INT`).

### gold.vendas_temporais — Diretoria Comercial
```sql
CREATE OR REFRESH MATERIALIZED VIEW gold.vendas_temporais (
  data DATE, dia_semana STRING, dia_semana_num INT, hora INT,
  canal_venda STRING, total_vendas BIGINT, itens_vendidos BIGINT,
  receita DECIMAL(10,2), clientes_unicos BIGINT
)
AS SELECT data, dia_semana, dia_semana_num, hora, canal_venda,
  COUNT(*) AS total_vendas, SUM(quantidade) AS itens_vendidos,
  CAST(SUM(receita) AS DECIMAL(10,2)) AS receita,
  COUNT(DISTINCT id_cliente) AS clientes_unicos
FROM silver.vendas GROUP BY ALL
```

### gold.vendas_produtos — Diretoria Comercial
```sql
CREATE OR REFRESH MATERIALIZED VIEW gold.vendas_produtos (
  id_produto STRING, nome_produto STRING, categoria STRING, marca STRING,
  total_vendas BIGINT, itens_vendidos BIGINT, receita DECIMAL(10,2),
  ticket_medio DECIMAL(10,2), ranking_receita INT, ranking_na_categoria INT
)
AS SELECT v.id_produto,
  COALESCE(p.nome_produto, 'Produto não cadastrado') AS nome_produto,
  COUNT(*) AS total_vendas, SUM(v.quantidade) AS itens_vendidos,
  CAST(SUM(v.receita) AS DECIMAL(10,2)) AS receita,
  CAST(ROUND(AVG(v.receita), 2) AS DECIMAL(10,2)) AS ticket_medio,
  ROW_NUMBER() OVER (ORDER BY CAST(SUM(v.receita) AS DECIMAL(10,2)) DESC) AS ranking_receita,
  ROW_NUMBER() OVER (PARTITION BY COALESCE(p.categoria, 'Não cadastrado')
    ORDER BY CAST(SUM(v.receita) AS DECIMAL(10,2)) DESC) AS ranking_na_categoria
FROM silver.vendas v LEFT JOIN silver.produtos p ON v.id_produto = p.id_produto
GROUP BY ALL
```

### gold.vendas_detalhadas — Cruzamento entre Diretorias
```sql
CREATE OR REFRESH MATERIALIZED VIEW gold.vendas_detalhadas
CLUSTER BY (data)
AS SELECT v.id_venda, v.data_venda, v.data, v.dia_semana, v.hora,
  v.canal_venda, v.id_produto, COALESCE(p.nome_produto, 'Produto não cadastrado'),
  v.id_cliente, c.nome_cliente, c.estado, c.regiao, cs.segmento_cliente,
  v.quantidade, v.preco_unitario, v.receita, v.produto_cadastrado
FROM silver.vendas v
LEFT JOIN silver.produtos p ON v.id_produto = p.id_produto
LEFT JOIN silver.clientes c ON v.id_cliente = c.id_cliente
LEFT JOIN gold.clientes_segmentacao cs ON v.id_cliente = cs.id_cliente
```

### gold.clientes_segmentacao — Customer Success
```sql
CREATE OR REFRESH MATERIALIZED VIEW gold.clientes_segmentacao (
  id_cliente STRING, nome_cliente STRING, estado STRING, nome_estado STRING,
  regiao STRING, total_compras BIGINT, receita DECIMAL(10,2),
  ticket_medio DECIMAL(10,2), segmento_cliente STRING, ranking_receita INT
)
AS WITH vendas_agg AS (
  SELECT id_cliente, COUNT(*) AS total_compras, SUM(receita) AS receita,
    MIN(data_venda) AS primeira_compra, MAX(data_venda) AS ultima_compra
  FROM silver.vendas GROUP BY id_cliente
)
SELECT c.id_cliente, c.nome_cliente, c.estado, c.nome_estado, c.regiao,
  COALESCE(v.total_compras, 0), CAST(COALESCE(v.receita, 0) AS DECIMAL(10,2)),
  CASE WHEN COALESCE(v.receita, 0) >= 22000 THEN 'VIP'
       WHEN COALESCE(v.receita, 0) >= 17000 THEN 'TOP_TIER'
       ELSE 'REGULAR' END AS segmento_cliente,
  ROW_NUMBER() OVER (ORDER BY COALESCE(v.receita, 0) DESC) AS ranking_receita
FROM silver.clientes c LEFT JOIN vendas_agg v ON c.id_cliente = v.id_cliente
```

### gold.precos_competitividade — Pricing
```sql
CREATE OR REFRESH MATERIALIZED VIEW gold.precos_competitividade (
  id_produto STRING, nome_produto STRING, nosso_preco DECIMAL(10,2),
  preco_medio_concorrentes DECIMAL(10,2), preco_minimo_concorrentes DECIMAL(10,2),
  classificacao_preco STRING, possui_preco_suspeito BOOLEAN, receita DECIMAL(10,2)
)
AS WITH concorrentes_agg AS (
  SELECT id_produto, CAST(ROUND(AVG(preco_concorrente), 2) AS DECIMAL(10,2)),
    CAST(MIN(preco_concorrente) AS DECIMAL(10,2)), CAST(MAX(preco_concorrente) AS DECIMAL(10,2)),
    BOOL_OR(preco_suspeito) AS possui_preco_suspeito
  FROM silver.preco_competidores GROUP BY id_produto
)
SELECT p.id_produto, p.nome_produto, CAST(p.preco_atual AS DECIMAL(10,2)),
  CASE WHEN p.preco_atual > ca.preco_maximo_concorrentes THEN 'MAIS_CARO_QUE_TODOS'
       WHEN p.preco_atual < ca.preco_minimo_concorrentes THEN 'MAIS_BARATO_QUE_TODOS'
       WHEN p.preco_atual > ca.preco_medio_concorrentes THEN 'ACIMA_DA_MEDIA'
       WHEN p.preco_atual < ca.preco_medio_concorrentes THEN 'ABAIXO_DA_MEDIA'
       ELSE 'NA_MEDIA' END AS classificacao_preco
FROM silver.produtos p INNER JOIN concorrentes_agg ca ON p.id_produto = ca.id_produto
```

---

## 💬 Prompts Utilizados no Desenvolvimento

O projeto foi desenvolvido com o auxílio do Databricks Assistant. Abaixo estão os 4 prompts principais que guiaram a construção de cada camada:

### Prompt 1 — Camada Silver
> "Crie as transformações da camada silver em Python (PySpark) usando Spark Declarative Pipelines. Para cada tabela bronze (vendas, clientes, produtos, preco_competidores), crie uma materialized view em silver que: remova duplicatas, converta valores monetários para DECIMAL(10,2), enriqueça os dados (mapeamento de UFs para regiões, faixa de preço, cálculo de receita, dia da semana em português) e adicione regras de qualidade @dp.expect_all_or_fail para campos obrigatórios e @dp.expect (warn) para problemas conhecidos como produto não cadastrado e preço suspeito. Nunca descarte linhas — marque problemas em colunas."

### Prompt 2 — Gold da Diretoria de Customer Success
> "Crie a materialized view gold.clientes_segmentacao em SQL que segmente todos os clientes por receita: VIP (>= R$ 22.000), TOP_TIER (R$ 17.000 a R$ 21.999,99) e REGULAR (< R$ 17.000). Inclua clientes sem compras (receita zero) usando LEFT JOIN a partir de silver.clientes. Adicione ranking de receita, ticket médio, primeira e última compra. Comente todas as colunas com tipo e descrição em português."

### Prompt 3 — Gold da Diretoria Comercial
> "Crie três materialized views em SQL para a Diretoria Comercial: (1) gold.vendas_temporais agregando vendas por data, hora, dia da semana e canal com COUNT, SUM e COUNT DISTINCT; (2) gold.vendas_produtos agregando por produto com ranking geral e por categoria usando ROW_NUMBER, incluindo vendas de produto não cadastrado com COALESCE; (3) gold.vendas_detalhadas no nível do pedido com JOIN entre vendas, produtos, clientes e segmentação, com CLUSTER BY (data). Todas com comentários em colunas e tipos explícitos."

### Prompt 4 — Gold da Diretoria de Pricing
> "Crie a materialized view gold.precos_competitividade em SQL que compare os preços da loja com os concorrentes (Mercado Livre, Amazon, Magalu, Shopee). Calcule preço médio, mínimo e máximo dos concorrentes, diferença percentual vs média e vs mínimo. Classifique cada produto: MAIS_CARO_QUE_TODOS, MAIS_BARATO_QUE_TODOS, ACIMA_DA_MEDIA, ABAIXO_DA_MEDIA, NA_MEDIA. Inclua flag de preço suspeito (concorrente com preço < 60% do nosso). Adicione receita e itens vendidos com LEFT JOIN em silver.vendas."

---

## 📊 Tabelas Gold

### `gold.vendas_temporais`
Vendas agregadas por data, hora, dia da semana e canal (ecommerce/loja_física). Receita, total de vendas, itens vendidos e clientes únicos.

### `gold.vendas_produtos`
Vendas agregadas por produto, com ranking de receita, ticket médio, categoria e marca. Inclui produtos não cadastrados.

### `gold.vendas_detalhadas`
Vendas detalhadas por cliente, produto, estado, região e canal. Visão transacional completa.

### `gold.clientes_segmentacao`
Segmentação de clientes por receita: VIP (≥ R$ 22.000), TOP_TIER (R$ 17.000–21.999,99), REGULAR (< R$ 17.000). Ranking de receita por cliente.

### `gold.precos_competitividade`
Análise de competitividade de preços por produto: classificação (MAIS_CARO_QUE_TODOS, MAIS_BARATO_QUE_TODOS, ACIMA_DA_MEDIA, ABAIXO_DA_MEDIA, NA_MEDIA), diferença percentual vs média e vs mínimo de concorrentes, flag de preço suspeito.

---

## 📈 Dashboards AI/BI

### 1. Diretoria Comercial — Vendas e Produtos
**Foco:** Performance de vendas e produtos
- **KPIs:** Receita total, Total de vendas, Vendas e-commerce, Vendas loja física
- **Gráficos:** Receita por canal, Receita por dia da semana, Receita por data, Receita por hora do dia
- **Tabelas:** Top 15 produtos por receita
- **Filtros globais:** Canal e Data

### 2. Diretoria de Pricing — Competitividade de Preços
**Foco:** Posicionamento de preços vs concorrentes
- **KPIs:** Produtos analisados, Mais caros que todos, Mais baratos que todos, Preço suspeito
- **Gráficos:** Distribuição por classificação (pie), Diferença % vs média por categoria (bar), Produtos por classificação em cada categoria (stacked bar)
- **Tabelas:** Top 20 mais caros que concorrentes, Produtos com preço suspeito

### 3. Customer Success — Segmentação de Clientes
**Foco:** Comportamento e segmentação de clientes
- **KPIs:** Total de clientes, Clientes VIP, Ticket médio, Maior cliente
- **Gráficos:** Distribuição por segmento, Receita por região, Top clientes por receita
- **Tabelas:** Detalhamento por cliente

---

## ⚙️ Orquestração

### Job: Pipeline E-commerce
Job multi-task orquestrado no Databricks com 3 tarefas sequenciais:

| Ordem | Task | Tipo | Depende de | Descrição |
| --- | --- | --- | --- | --- |
| 1 | `rodar_pipeline` | SDP Pipeline | — | Atualiza bronze → silver → gold |
| 2 | `rodar_testes` | Notebook | `rodar_pipeline` | Valida qualidade dos dados gold |
| 3 | `atualizar_dashboards` | Notebook | `rodar_testes` | Invalida cache e republica 3 dashboards |

### Pipeline SDP
- **Nome:** `pipeline_silver_projetovendas`
- **Tipo:** Serverless
- **Catálogo:** `projetovendas`
- **Schema padrão:** `silver`
- **Golds publicadas como:** `gold.<tabela>`
- **Todas as tabelas são materialized views** com leitura batch (`spark.read.table`)

### Notebook de Refresh de Dashboards
O notebook `atualizar_dashboards_nb` utiliza o **Databricks SDK** para:
1. Obter o draft de cada dashboard via `w.lakeview.get()`
2. Modificar as `queryLines` dos datasets (adicionar comentário `-- refresh`) para **invalidar o cache**
3. Atualizar o draft via `w.lakeview.update()`
4. Republicar via `w.lakeview.publish(embed_credentials=True)`

Isso garante que os dashboards publicados sempre reflitam os dados mais recentes das tabelas gold.

---

## 🔒 Qualidade de Dados

### Convenções
- Problemas de qualidade conhecidos são **MARCADOS** em uma coluna e medidos com `@dp.expect` (warn)
- **Nunca descartar linhas:** apagar vendas mudaria a receita
- `@dp.expect_all_or_fail` apenas para o que nunca pode acontecer
- Dinheiro sempre `DECIMAL(10,2)`
- Nomes de tabelas e colunas em português, snake_case, sem acento

### Testes Automatizados
O notebook `testes/testes_qualidade_nb` valida:
- Total de clientes e segmentação
- Receita total
- Total de vendas (ecommerce + loja física)
- Produtos com preço de concorrente
- Produtos com preço suspeito
- Schema e documentação de colunas

---

## 📁 Estrutura do Projeto

```
projetovendas/
├── resources/
│   └── pipeline_silver_projetovendas.pipeline.yml   # Configuração do pipeline SDP
├── transformations/
│   ├── silver/                                      # Transformações silver (Python)
│   │   ├── vendas.py
│   │   ├── clientes.py
│   │   ├── produtos.py
│   │   └── precos_concorrentes.py
│   └── gold/                                         # Agregações gold (SQL)
│       ├── vendas_temporais.sql
│       ├── vendas_produtos.sql
│       ├── vendas_detalhadas.sql
│       ├── clientes_segmentacao.sql
│       └── precos_competitividade.sql
├── testes/
│   └── testes_qualidade_nb                          # Notebook de testes
├── atualizar_dashboards_nb                           # Notebook de refresh de dashboards
├── databricks.yml                                    # Configuração do Declarative Automation Bundle
└── CLAUDE.md                                         # Convenções do projeto
```

---

## 🔢 Números de Referência (Gold)

| Métrica | Valor |
| --- | --- |
| Receita total | R$ 1.482.116,03 |
| Total de vendas | 3.020 |
| Vendas e-commerce | 2.155 |
| Vendas loja física | 865 |
| Clientes | 50 (45 VIP, 4 TOP_TIER, 1 REGULAR) |
| Maior cliente | Henrique Da Conceição (DF, R$ 45.193,19) |
| Produtos analisados | 215 (35 MAIS_CARO, 6 MAIS_BARATO, 92 ACIMA, 76 ABAIXO, 6 NA_MEDIA) |
| Produtos com preço suspeito | 15 |
| Período dos dados | 13/12/2025 a 11/01/2026 |

---

## 🛠️ Tecnologias

| Tecnologia | Uso |
| --- | --- |
| **Databricks Lakehouse** | Plataforma de dados unificada |
| **Unity Catalog** | Governança e catálogo de dados |
| **Spark Declarative Pipelines (SDP)** | Pipeline de transformação bronze → silver → gold |
| **Materialized Views** | Tabelas gold com auto-refresh |
| **AI/BI Dashboards (Lakeview)** | Dashboards executivos publicados |
| **Databricks SDK (Python)** | Automação de refresh de dashboards |
| **Declarative Automation Bundles** | Deploy e versionamento do pipeline |
| **AWS S3 (simulado)** | Object storage — padrão de ingestão Parquet → Bronze |
| **Parquet** | Formato de armazenamento colunar dos dados de origem |
| **Supabase (PostgreSQL)** | Fonte de dados transacional (simula S3) |
| **Python (PySpark)** | Transformações silver |
| **SQL** | Transformações gold e queries de dashboards |
| **Databricks Assistant (IA)** | Assistente de IA para desenvolvimento, debugging e automação |

---

## 🚀 Como Executar

### Pré-requisitos
- Databricks workspace com serverless compute
- Unity Catalog habilitado
- Acesso ao Supabase como fonte de dados
- Databricks CLI configurado

### Deploy do Pipeline
```bash
# Validar o bundle
databricks bundle validate --strict

# Deploy do bundle
databricks bundle deploy
```

### Executar o Job Completo
```bash
# Disparar o job (pipeline + testes + refresh de dashboards)
databricks jobs run-now 116445174849570
```

### Ou executar via SDK
```python
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
w.jobs.run_now(job_id=116445174849570)
```

---

## 📌 Convenções do Projeto

- **Catálogo:** `projetovendas` (nunca usar outro)
- **Schemas:** `bronze`, `silver`, `gold`
- **Silver:** Python (`from pyspark import pipelines as dp`)
- **Gold:** SQL (`CREATE OR REFRESH MATERIALIZED VIEW`)
- **Um arquivo por tabela** em `transformations/`
- **Comentários** em português explicando o PORQUÊ das regras
- **Dinheiro:** sempre `DECIMAL(10,2)`
- **Sempre rodar** `databricks bundle validate --strict` antes do deploy

---

## 🤖 Uso de IA no Projeto

O **Databricks Assistant** foi utilizado como ferramenta de desenvolvimento ao longo de todo o projeto, atuando como um par de programação inteligente — assim como Git, Databricks CLI ou SDK são ferramentas que ampliam a produtividade do engenheiro.

### Como a IA foi utilizada

| Etapa | Descrição |
| --- | --- |
| **Desenvolvimento de código** | Geração e refinamento de transformações silver (Python/PySpark) e gold (SQL), incluindo materialized views com tipos explícitos, comentários e regras de qualidade `@dp.expect` |
| **Debugging e correção** | Diagnóstico e correção de erros de schema (ROW_NUMBER retornando INT ao invés de BIGINT), incompatibilidades de tipo em DataFrames, e falhas de validação do pipeline SDP |
| **Automação de dashboards** | Criação do notebook de refresh que usa o Databricks SDK para invalidar cache e republicar dashboards — solução desenvolvida após identificar que a republicação manual não atualizava os dados publicados |
| **Criação de widgets** | Posicionamento e configuração de KPIs, gráficos (bar, pie, line), tabelas e filtros globais nos 3 dashboards AI/BI via API do Lakeview |
| **Orquestração de jobs** | Configuração do job multi-task (pipeline → testes → refresh de dashboards) e adição da task de automação |
| **Testes de qualidade** | Escrita de testes automatizados validando números de referência, segmentação de clientes, métricas de pricing e cobertura de schema |
| **Documentação** | Geração do README.md profissional, atualização do CLAUDE.md com convenções e números de referência |
| **Publicação no GitHub** | Inicialização do repositório Git, commit de arquivos e push via integração Git do Databricks |

### Filosofia de uso

A IA foi tratada como uma **ferramenta de produtividade**, não como substituto do engenheiro. Todas as decisões de arquitetura, regras de negócio, convenções de nomenclatura e critérios de qualidade foram definidas pelo desenvolvedor. A IA acelerou a implementação, mas o entendimento do domínio e a validação dos resultados foram humanos.

---

## 👤 Autor

**Eduardo Ramiro**
- Linkedin: https://www.linkedin.com/in/eduardo-ramiro/
- GitHub: https://github.com/edurj07

---

## 📄 Licença

Este projeto é de uso pessoal/portfólio. Todos os dados são sintéticos para fins de demonstração.
