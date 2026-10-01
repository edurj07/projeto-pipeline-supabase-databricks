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

## Genie Space: Diretoria E-commerce

![Genie Space - Diretoria E-commerce](docs/images/genie_space_chat.png)

> Print da tela inicial do Genie Space com as 6 perguntas de exemplo (2 por diretoria). O space atende os tres diretores: Comercial, Customer Success e Pricing.

- Space ID: `01f1bd305a35162797249bd6197b1bda`.
- Um único space atende os tres diretores: Comercial (vendas), Customer Success (clientes) e Pricing (precos da concorrencia: Mercado Livre, Amazon, Magalu e Shopee).
- Warehouse: "Serverless Starter Warehouse".
- Tabelas: as 5 golds (`vendas_temporais`, `vendas_produtos`, `vendas_detalhadas`, `clientes_segmentacao`, `precos_competitividade`). Nenhuma tabela da bronze ou da silver.
- O Genie nao sabe nada da empresa: tudo o que ele sabe vem das tabelas, dos comentarios das colunas e do que colocarmos no space. Os comentarios da gold ja existem; nao repita nas instrucoes o que o comentario ja diz.
- Mudou uma instrucao? Edite o JSON e faca deploy. Nada de ajustar o space pela interface, que se perde no proximo deploy.

### Conventions do bundle (Genie)

- O space fica em `src/genie/diretoria_ecommerce.geniespace.json` (o serialized space exportado) e o recurso em `resources/diretoria.genie_space.yml`.
- Warehouse por lookup do nome e `parent_path: ${workspace.root_path}`, para nao colidir com outro space de mesmo nome na sua pasta.
- Os identificadores das tabelas ficam escritos no JSON (`projetovendas.gold.<tabela>`), porque o arquivo nao passa por variaveis do bundle.
- Em prod o space ID muda: anote no JSON do dashboard o novo ID ao promover.

### Instrucoes gerais do space (texto)

- Responder em portugues; dinheiro em R$ com 2 casas.
- Receita e bruta: nao existe custo, margem ou lucro. Se perguntarem sobre lucro, recuse e explique.
- Periodo: 13/12/2025 a 11/01/2026. "No mes" ou "ate agora" e o periodo inteiro, nunca `current_date()`.
- "Hoje", "ontem" e "esta semana" nao se respondem: 11/01/2026 nao e hoje. Explique o periodo em texto e pergunte se quer ver a data final.
- Tabela por tipo de pergunta: tempo e canal -> `vendas_temporais`; produto -> `vendas_produtos`; cliente -> `clientes_segmentacao`; preco da concorrencia -> `precos_competitividade`; cruzamento entre diretorias -> `vendas_detalhadas`.
- REGRA ABSOLUTA DE REGIAO E ESTADO: toda pergunta sobre regiao ou estado -> SEMPRE use `clientes_segmentacao`, nunca `vendas_detalhadas`. `COUNT(*)` conta clientes e `SUM(receita)` soma a receita por cliente. Use `vendas_detalhadas` para regiao APENAS quando a pergunta cruzar regiao com produto, categoria ou canal simultaneamente.
- Ticket medio = receita / numero de vendas: `SUM(total_vendas)` em `vendas_temporais` e `vendas_produtos`, `COUNT(*)` em `vendas_detalhadas`, `SUM(total_compras)` em `clientes_segmentacao`. Nunca use media de medias.
- Contar produto por `id_produto` (ha nomes repetidos).
- Dia da semana: use receita media por dia (AVG), nao soma, porque o periodo tem 5 sab/domingos e so 4 de cada dia util; cite tambem o dia de maior receita total e por que ele lidera.
- Segmentos: VIP (receita >= R$ 22.000), TOP_TIER (R$ 17.000 a R$ 21.999,99), REGULAR (< R$ 17.000).
- "Mais caro que o mercado" = `diferenca_pct_vs_media > 0`. "Mais caro que todos" = `classificacao_preco = 'MAIS_CARO_QUE_TODOS'`.
- Toda contagem de produtos em `precos_competitividade` separa confirmados (`possui_preco_suspeito = false`) dos que tem preco suspeito (`possui_preco_suspeito = true`, a confirmar antes de reagir) e diz em que categoria estao os suspeitos, com o total.
- Distincao entre "vende mais" e "gera mais": "qual X vende mais" (canal, categoria, dia da semana) -> traga TODOS os valores para comparacao. "qual X gera mais receita" (regiao, estado, categoria) -> traga apenas o primeiro (LIMIT 1), com receita e numero de clientes.
- Em "melhores clientes" ou "ranking de clientes", SEMPRE inclua: `nome_cliente`, `estado`, `regiao`, `receita`, `total_compras`, `segmento_cliente` e `ranking_receita`. Ordene por `ranking_receita` ASC.
- Categoria como agrupamento de vendas (ex: receita por categoria) -> use `vendas_detalhadas`. Categoria em ranking de produtos -> use `vendas_produtos`.
- Numero de clientes (`COUNT DISTINCT id_cliente`) so em pergunta por regiao, estado ou segmento — nunca somando `clientes_unicos` entre linhas.
- Canais: exiba "E-commerce" (para ecommerce) e "Loja fisica" (para loja_fisica).
- Rankings com 10 linhas.

### Sinonimos de colunas

| Sinonimo | Coluna |
|---|---|
| faturamento | `receita` (todas as tabelas) |
| UF | `estado` (`vendas_detalhadas`, `clientes_segmentacao`) |
| canal | `canal_venda` (`vendas_temporais`, `vendas_detalhadas`) |
| perfil | `segmento_cliente` (`vendas_detalhadas`, `clientes_segmentacao`) |
| posicao de preco | `classificacao_preco` (`precos_competitividade`) |

### Joins

| Tabela A | Tabela B | Chave | Tipo |
|---|---|---|---|
| `vendas_produtos` | `precos_competitividade` | `id_produto` | 1:1 |
| `vendas_detalhadas` | `clientes_segmentacao` | `id_cliente` | N:1 |

### SQL de exemplo (5)

1. Ticket medio por segmento de cliente: `SUM(receita) / SUM(total_compras)` em `clientes_segmentacao`.
2. Participacao dos TOP_TIER na receita: `SUM(CASE WHEN segmento_cliente = 'TOP_TIER' THEN receita ELSE 0 END) / SUM(receita) * 100`.
3. Receita por regiao: `SELECT regiao, SUM(receita), COUNT(*) AS clientes FROM clientes_segmentacao GROUP BY regiao ORDER BY receita DESC`.
4. Produtos mais caros que a media do mercado, confirmado vs a confirmar: `CASE WHEN possui_preco_suspeito THEN 'A confirmar' ELSE 'Confirmado' END`, `COUNT(*)`, `COLLECT_LIST(DISTINCT categoria)` em `precos_competitividade WHERE diferenca_pct_vs_media > 0`.
5. Ticket medio (medida de `vendas_temporais`): `SUM(receita) / SUM(total_vendas)`.

### Perguntas de exemplo na tela inicial (6)

Comercial: "Qual foi a receita total do periodo?" e "Qual canal vende mais?"
Customer Success: "Quem sao os 5 melhores clientes?" e "Quantos clientes VIP temos e quanto representam da receita?"
Pricing: "Quantos produtos estao mais caros que todos os concorrentes?" e "Qual categoria esta mais cara que o mercado?"

### Benchmarks (12) — placar final

10 perguntas de teste + 2 de limite (recusa). Resultado final apos 5 rodadas de iteracao:

| Rodada | Passaram | Falhas | Need review | Correcao aplicada |
|---|---|---|---|---|
| 1 | 8/12 | 2 | 2 | Benchmark de canal alinhado com `vendas_temporais`; instrucao "todos os valores" + regiao->`clientes_segmentacao` |
| 2 | 8/12 | 2 | 2 | "Vende mais" vs "gera mais"; categoria->`vendas_detalhadas` |
| 3 | 9/12 | 1 | 2 | Regra absoluta de regiao; SQL exemplo 3 corrigido para `clientes_segmentacao` |
| 4 | 9/12 | 1 | 2 | "Dia da semana vende mais" -> todos os 7 dias |
| 5 | 10/12 | 0 | 2 | Colunas do ranking de clientes (sempre incluir `estado`) |

As 2 "need review" sao testes de recusa ("Qual foi o nosso lucro?" e "Quanto vendemos ontem?") que nao tem SQL de referencia. O Genie recusa corretamente em ambos os casos.

### Link "Ask Genie" nos dashboards

O botao "Ask Genie" dos 3 dashboards deve apontar para este space. O space ID vai fixo no JSON do dashboard. Em prod o ID muda: anote nas instrucoes do projeto que em prod ele muda.
