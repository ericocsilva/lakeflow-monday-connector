# Ingestão Monday.com → Tabelas Gerenciadas com Board Discovery Dinâmico

Pipeline de **2 tasks encadeadas** que descobre automaticamente todos os boards acessíveis no Monday.com e os ingere como tabelas Delta **gerenciadas** no Unity Catalog.

Diferente da versão estática (`ingest/managed/`), aqui os `board_ids` nunca são hardcoded — são descobertos em tempo de execução e passados entre tasks via **Databricks Job Task Values**.

## Versões disponíveis nesta pasta

| Notebook | Compute | Pipeline interno | Runtime mínimo |
|----------|---------|-----------------|----------------|
| `monday_uc_managed_dynamic_ingest.py` | **Serverless** / qualquer DBR | `ingestion_pipeline_direct` | DBR 12.x+ |
| `monday_uc_managed_dynamic_classic_ingest.py` | **Classic cluster** | `ingestion_pipeline_hms` (`readStream + foreachBatch`) | DBR 14.3+ |

**Diferença principal:** a versão `_classic` usa Spark Structured Streaming com checkpoints em DBFS (parâmetro `checkpoint_base`).

> Use a versão serverless por padrão. Use `_classic` se precisar de Streaming nativo.

---

---

## Fluxo do Job

```
Task 1: discover_boards
  └─ Consulta API Monday.com
  └─ Lista todos os boards com state=active
  └─ Publica board_ids via dbutils.jobs.taskValues.set()
        ↓
Task 2: ingest_monday_managed  (depende de Task 1)
  └─ Lê board_ids via dbutils.jobs.taskValues.get()
  └─ Injeta no pipeline_spec dinamicamente
  └─ Ingere todas as tabelas em webmotors_demo_catalog.monday_dynamic
```

> Novos boards criados no Monday.com são detectados e ingeridos automaticamente no próximo run, sem qualquer alteração de código ou configuração.

---

## Pré-requisitos

- Databricks workspace com Unity Catalog habilitado
- Secret `monday/api_token` configurado (veja [Passo 2](#passo-2--criar-o-databricks-secret))
- Permissão `CREATE TABLE` e `MODIFY` no catálogo alvo

---

## Passo 1 — Obter o token da API Monday.com

1. Login no Monday.com → foto de perfil → **Developers**
2. Aba **My Access Tokens** → **Show**
3. Copie o token

---

## Passo 2 — Criar o Databricks Secret

```bash
# Criar scope (uma vez por workspace)
databricks secrets create-scope monday

# Salvar token
databricks secrets put-secret monday api_token
```

Verificar:
```bash
databricks secrets list-secrets monday
```

---

## Passo 3 — Adicionar o repositório ao Workspace

1. **Repos** → **Add Repo**
2. URL: `https://github.com/junior-esteca_data/lakeflow-monday-connector`
3. Provider: **GitHub** → **Create Repo**

---

## Passo 4 — Configurar os notebooks (opcional)

Os notebooks já estão pré-configurados para o workspace de demo. Para adaptar ao seu ambiente, edite os campos abaixo:

**`ingest/discover_boards.py`** — nenhuma alteração necessária (usa o secret `monday/api_token`).

**`ingest/managed_dynamic/monday_uc_managed_dynamic_ingest.py`:**

```python
pipeline_spec = {
    "api_token": dbutils.secrets.get(scope="monday", key="api_token"),

    "target_database":     "monday_dynamic",   # schema criado automaticamente
    "destination_catalog": "meu_catalog",      # ← altere para seu catálogo UC

    "objects": [
        # board_ids injetado automaticamente pelo discover_boards
        {"table": {"source_table": "boards", ...}},
        {"table": {"source_table": "items",  "table_configuration": {"board_ids": board_ids}}},
        ...
    ]
}
```

---

## Passo 5 — Criar o Job

### Serverless — Via Databricks CLI

```bash
databricks jobs create --json '{
  "name": "monday-lakeflow-managed-dynamic",
  "environments": [
    {
      "environment_key": "default",
      "spec": {
        "client": "1",
        "dependencies": ["requests>=2.28.0", "pydantic>=2.0.0"]
      }
    }
  ],
  "tasks": [
    {
      "task_key": "discover_boards",
      "environment_key": "default",
      "notebook_task": {
        "notebook_path": "/Repos/<seu-email>/lakeflow-monday-connector/ingest/discover_boards",
        "source": "WORKSPACE"
      }
    },
    {
      "task_key": "ingest_monday_managed",
      "depends_on": [{"task_key": "discover_boards"}],
      "environment_key": "default",
      "notebook_task": {
        "notebook_path": "/Repos/<seu-email>/lakeflow-monday-connector/ingest/managed_dynamic/monday_uc_managed_dynamic_ingest",
        "source": "WORKSPACE"
      }
    }
  ],
  "schedule": {
    "quartz_cron_expression": "0 0 * * * ?",
    "timezone_id": "America/Sao_Paulo",
    "pause_status": "PAUSED"
  }
}'
```

### Classic — Via Databricks CLI

```bash
databricks jobs create --json '{
  "name": "monday-lakeflow-managed-dynamic-classic",
  "tasks": [
    {
      "task_key": "discover_boards",
      "existing_cluster_id": "<cluster_id_dbr_14_3_plus>",
      "notebook_task": {
        "notebook_path": "/Repos/<seu-email>/lakeflow-monday-connector/ingest/discover_boards",
        "source": "WORKSPACE"
      }
    },
    {
      "task_key": "ingest_monday_managed_classic",
      "depends_on": [{"task_key": "discover_boards"}],
      "existing_cluster_id": "<cluster_id_dbr_14_3_plus>",
      "notebook_task": {
        "notebook_path": "/Repos/<seu-email>/lakeflow-monday-connector/ingest/managed_dynamic/monday_uc_managed_dynamic_classic_ingest",
        "source": "WORKSPACE"
      }
    }
  ],
  "schedule": {
    "quartz_cron_expression": "0 0 * * * ?",
    "timezone_id": "America/Sao_Paulo",
    "pause_status": "PAUSED"
  }
}'
```

> A versão classic requer cluster com **DBR 14.3+** e biblioteca `pyspark.sql.datasource`.

### Via UI (Serverless)

1. **Workflows** → **Create Job**
2. Adicione a **Task 1**:
   - Name: `discover_boards`
   - Type: Notebook
   - Path: `.../ingest/discover_boards`
   - Environment: `requests>=2.28.0`, `pydantic>=2.0.0`
3. Adicione a **Task 2**:
   - Name: `ingest_monday_managed`
   - Type: Notebook
   - Path: `.../ingest/managed_dynamic/monday_uc_managed_dynamic_ingest`
   - **Depends on:** `discover_boards`
   - Environment: mesmo do Task 1
4. Configure o schedule

---

## Passo 6 — Executar e verificar

```bash
# Disparar run
databricks jobs run-now <JOB_ID> --no-wait

# Acompanhar — verá as duas tasks em sequência
databricks jobs get-run <RUN_ID>
```

### Verificar tabelas

```sql
SHOW TABLES IN meu_catalog.monday_dynamic;
```

| Table | Type |
|-------|------|
| `_lakeflow_offsets` | MANAGED |
| `activity_logs` | MANAGED |
| `boards` | MANAGED |
| `groups` | MANAGED |
| `items` | MANAGED |
| `tags` | MANAGED |
| `teams` | MANAGED |
| `updates` | MANAGED |
| `users` | MANAGED |
| `workspaces` | MANAGED |

### Verificar quais boards foram descobertos

No output da Task 1 (`discover_boards`) você verá:
```
Found board: 18406865721 — Poc Monday [active]
Total boards found: 1
Board IDs: 18406865721
```

---

## Re-sincronização completa (CDC)

```sql
DELETE FROM meu_catalog.monday_dynamic._lakeflow_offsets
WHERE table_name IN ('boards', 'items');
```

---

## Diferença para a versão estática (`ingest/managed/`)

| | Estático | Dinâmico (este) |
|---|---|---|
| `board_ids` | Hardcoded no notebook | Descoberto em runtime |
| Novos boards | Requer edição manual | Detectado automaticamente |
| Estrutura | 1 task | 2 tasks encadeadas |
| Task Values | Não usa | `dbutils.jobs.taskValues` |
