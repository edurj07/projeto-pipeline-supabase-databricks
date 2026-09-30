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
