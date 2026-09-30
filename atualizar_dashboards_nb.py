# Databricks notebook source
# DBTITLE 1,Refresh e Republicação de Dashboards
# MAGIC %md
# MAGIC # Refresh e Republicação de Dashboards AI/BI
# MAGIC
# MAGIC Força a invalidação do cache de queries dos dashboards e os republica, garantindo que os números publicados reflitam os dados mais recentes das tabelas gold.

# COMMAND ----------

# DBTITLE 1,Refresh e Republicação de Dashboards
# -*- coding: utf-8 -*-
"""
Refresh e Republicação Automática de Dashboards AI/BI

Este notebook é executado após o pipeline SDP atualizar as tabelas gold.
Ele força a invalidação do cache de queries dos dashboards e os republica,
garantindo que os números publicados reflitam os dados mais recentes.

Por que isso é necessário?
- O pipeline atualiza as materialized views gold (bronze -> silver -> gold)
- Os dashboards AI/BI mantêm cache das queries SQL dos datasets
- Mesmo após republicar, o cache antigo pode ser usado se a query não mudou
- Modificar o texto da query (adicionar/remover um comentário) invalida o cache
- A republicação então captura dados frescos das tabelas gold
"""

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dashboards import Dashboard
import json
from datetime import datetime, timezone

# Inicializa o cliente do workspace (usa credenciais do usuário corrente)
w = WorkspaceClient()

# IDs dos três dashboards AI/BI publicados
# Commercial: KPIs de vendas, receita por canal/categoria, top produtos
# Pricing: competitividade de preços, produtos mais caros, preços suspeitos
# Customer Success: segmentação de clientes, ranking, ticket médio
dashboard_ids = {
    "Commercial": "01f1bc2855091056ab390eb14e0ed1c3",
    "Pricing": "01f1bc295b8f1ed582d26aa643eef213",
    "Customer Success": "01f1bc2837ca1e66b14c25f17e95cbca",
}

# Timestamp atual para marcar o refresh (usado no comentário SQL das queries)
refresh_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
print(f"Iniciando refresh dos dashboards - {refresh_ts}")
print("=" * 60)

# Processa cada dashboard: invalida cache das queries e republica
for name, dash_id in dashboard_ids.items():
    try:
        # Passo 1: Obter o draft atual do dashboard (com serialized_dashboard JSON)
        # O draft contém a definição completa: datasets, pages, widgets
        draft = w.lakeview.get(dashboard_id=dash_id)

        # Passo 2: Desserializar o JSON do dashboard para modificar os datasets
        sd = (
            json.loads(draft.serialized_dashboard)
            if isinstance(draft.serialized_dashboard, str)
            else draft.serialized_dashboard
        )

        # Passo 3: Para cada dataset, modificar as queryLines para invalidar o cache
        # Remove qualquer linha "-- refresh" anterior e insere uma nova com timestamp atual
        # Isso força o mecanismo de cache do Lakeview a re-executar a query contra o warehouse
        datasets_count = len(sd.get("datasets", []))
        for ds in sd.get("datasets", []):
            if "queryLines" in ds and len(ds["queryLines"]) > 0:
                # Remove comentários de refresh anteriores (evita acúmulo)
                ds["queryLines"] = [
                    line for line in ds["queryLines"]
                    if not line.strip().startswith("-- refresh")
                ]
                # Insere novo comentário de refresh com timestamp atual
                ds["queryLines"].insert(0, f"-- refresh {refresh_ts}\n")

        # Passo 4: Atualizar o dashboard draft com as queries modificadas
        # Cria objeto Dashboard com o JSON atualizado, preservando display_name e warehouse_id
        updated_dashboard = Dashboard(
            serialized_dashboard=json.dumps(sd),
            display_name=draft.display_name,
            dashboard_id=dash_id,
            warehouse_id=draft.warehouse_id,
        )
        w.lakeview.update(dashboard_id=dash_id, dashboard=updated_dashboard)

        # Passo 5: Republicar o dashboard com credenciais compartilhadas
        # A republicação executa as queries contra as tabelas gold e cria um snapshot fresco
        # embed_credentials=True permite que visualizadores acessem os dados sem permissões próprias
        resp = w.lakeview.publish(dashboard_id=dash_id, embed_credentials=True)

        print(f"✅ {name}: {datasets_count} datasets atualizados, republicado - revisão {resp.revision_create_time}")

    except Exception as e:
        print(f"❌ {name}: erro - {e}")
        import traceback
        traceback.print_exc()

print("=" * 60)
print("Refresh concluído. Os dashboards publicados agora refletem os dados mais recentes das tabelas gold.")