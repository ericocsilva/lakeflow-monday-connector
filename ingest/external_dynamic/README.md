# Ingestão Monday.com → Tabelas Externas com Board Discovery Dinâmico

Pipeline de **2 tasks encadeadas** que descobre automaticamente todos os boards acessíveis no Monday.com e os ingere como tabelas Delta **externas** no Unity Catalog.

Diferente da versão estática (`ingest/external/`), aqui os `board_ids` nunca são hardcoded — são descobertos em tempo de execução e passados entre tasks via **Databricks Job Task Values**.

## Versões disponíveis nesta pasta

| Notebook | Compute | Pipeline interno | Runtime mínimo |
|----------|---------|-----------------|----------------|
| `monday_uc_external_dynamic_ingest.py` | **Serverless** / qualquer DBR | `ingestion_pipeline_uc_external_direct` | DBR 12.x+ |
| `monday_uc_external_dynamic_classic_ingest.py` | **Classic cluster** | `ingestion_pipeline_uc_external` (`readStream + foreachBatch`) | DBR 14.3+ |

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
Task 2: ingest_monday_external  (depende de Task 1)
  └─ Lê board_ids via dbutils.jobs.taskValues.get()
  └─ Injeta no pipeline_spec dinamicamente
  └─ Ingere em classic_stable_hj897w_catalog.monday_external_dynamic
  └─ Dados armazenados em s3://bucket/monday_dynamic/<table>/
```

> Novos boards criados no Monday.com são detectados e ingeridos automaticamente no próximo run, sem qualquer alteração de código ou configuração.

---

## Diferença para tabelas gerenciadas

| | Managed Dynamic (`ingest/managed_dynamic/`) | External Dynamic (este) |
|---|---|---|
| Storage | Metastore default | Seu cloud storage (S3/ADLS/GCS) |
| `DROP TABLE` apaga dados? | Sim | **Não** |
| Requer External Location? | Não | **Sim** |

---

## Pré-requisitos

- Databricks workspace com Unity Catalog habilitado
- Uma **UC External Location** cobrindo o path de destino
- Permissão `WRITE FILES` na External Location
- Permissão `CREATE TABLE` e `MODIFY` no schema alvo
- Secret `monday/api_token` configurado

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

---

## Passo 3 — Criar a UC External Location

> Pule se já existir uma External Location cobrindo seu path.

```sql
-- AWS
CREATE EXTERNAL LOCATION monday_ext_location
  URL 's3://meu-bucket/monday_dynamic'
  WITH (STORAGE CREDENTIAL minha_credential);

-- Azure
CREATE EXTERNAL LOCATION monday_ext_location
  URL 'abfss://container@account.dfs.core.windows.net/monday_dynamic'
  WITH (STORAGE CREDENTIAL minha_credential);

-- GCP
CREATE EXTERNAL LOCATION monday_ext_location
  URL 'gs://meu-bucket/monday_dynamic'
  WITH (STORAGE CREDENTIAL minha_credential);
```

Verificar:
```sql
SHOW EXTERNAL LOCATIONS;
```

---

## Passo 4 — Adicionar o repositório ao Workspace

1. **Repos** → **Add Repo**
2. URL: `https://github.com/junior-esteca_data/lakeflow-monday-connector`
3. Provider: **GitHub** → **Create Repo**

---

## Passo 5 — Configurar os notebooks (opcional)

**`ingest/discover_boards.py`** — nenhuma alteração necessária.

**`ingest/external_dynamic/monday_uc_external_dynamic_ingest.py`:**

```python
pipeline_spec = {
    "api_token": dbutils.secrets.get(scope="monday", key="api_token"),

    "destination_catalog": "meu_catalog",           # ← altere
    "destination_schema":  "monday_external_dynamic",

    # Deve estar sob uma External Location registrada
    "external_location_base": "s3://meu-bucket/monday_dynamic",  # ← altere

    "objects": [
        # board_ids injetado automaticamente pelo discover_boards
        {"table": {"source_table": "items", "table_configuration": {"board_ids": board_ids}}},
        ...
    ]
}
```

### Layout de storage gerado automaticamente

```
s3://meu-bucket/monday_dynamic/
  boards/
  items/
  users/
  groups/
  tags/
  updates/
  activity_logs/
  workspaces/
  teams/
```

---

## Passo 6 — Criar o Job

### Serverless — Via Databricks CLI

```bash
databricks jobs create --json '{
  "name": "monday-lakeflow-external-dynamic",
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
      "task_key": "ingest_monday_external",
      "depends_on": [{"task_key": "discover_boards"}],
      "environment_key": "default",
      "notebook_task": {
        "notebook_path": "/Repos/<seu-email>/lakeflow-monday-connector/ingest/external_dynamic/monday_uc_external_dynamic_ingest",
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
  "name": "monday-lakeflow-external-dynamic-classic",
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
      "task_key": "ingest_monday_external_classic",
      "depends_on": [{"task_key": "discover_boards"}],
      "existing_cluster_id": "<cluster_id_dbr_14_3_plus>",
      "notebook_task": {
        "notebook_path": "/Repos/<seu-email>/lakeflow-monday-connector/ingest/external_dynamic/monday_uc_external_dynamic_classic_ingest",
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
   - Name: `ingest_monday_external`
   - Type: Notebook
   - Path: `.../ingest/external_dynamic/monday_uc_external_dynamic_ingest`
   - **Depends on:** `discover_boards`
4. Configure o schedule

---

## Passo 7 — Executar e verificar

```bash
databricks jobs run-now <JOB_ID> --no-wait
```

### Verificar tabelas

```sql
SHOW TABLES IN meu_catalog.monday_external_dynamic;
```

| Table | Type |
|-------|------|
| `_lakeflow_offsets` | MANAGED |
| `activity_logs` | **EXTERNAL** |
| `boards` | **EXTERNAL** |
| `groups` | **EXTERNAL** |
| `items` | **EXTERNAL** |
| `tags` | **EXTERNAL** |
| `teams` | **EXTERNAL** |
| `updates` | **EXTERNAL** |
| `users` | **EXTERNAL** |
| `workspaces` | **EXTERNAL** |

### Confirmar localização

```sql
DESCRIBE EXTENDED meu_catalog.monday_external_dynamic.boards;
-- Location: s3://meu-bucket/monday_dynamic/boards
```

---

## Re-sincronização completa (CDC)

```sql
DELETE FROM meu_catalog.monday_external_dynamic._lakeflow_offsets
WHERE table_name IN ('boards', 'items');
```

---

## Troubleshooting

### `PERMISSION_DENIED: WRITE FILES on External Location`

```sql
GRANT WRITE FILES ON EXTERNAL LOCATION monday_ext_location
TO `seu-service-principal@tenant.com`;
```

### Task 2 falha com "No board_ids received"

A Task 2 não encontrou o Task Value publicado pela Task 1. Causas comuns:
- Task 2 foi executada manualmente fora do job (use `debugValue` para testar isoladamente)
- Task 1 falhou antes de publicar o valor — verifique o output da Task 1
