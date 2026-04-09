# Ingestão Monday.com → Tabelas Externas com ForEach por Board

Pipeline de **3 tasks** que descobre todos os boards do Monday.com e processa cada board em paralelo em tasks independentes, ingerindo dados como tabelas Delta **externas** no Unity Catalog.

Os dados ficam no seu cloud storage (S3/ADLS/GCS) — `DROP TABLE` **não** apaga os dados.

## Versões disponíveis nesta pasta

| Notebook | Compute | Pipeline interno | Runtime mínimo |
|----------|---------|-----------------|----------------|
| `monday_uc_external_foreach_ingest.py` | **Serverless** / qualquer DBR | `ingestion_pipeline_uc_external_direct` | DBR 12.x+ |
| `monday_uc_external_foreach_classic_ingest.py` | **Classic cluster** | `ingestion_pipeline_uc_external` (`readStream + foreachBatch`) | DBR 14.3+ |

> Use a versão serverless por padrão. Use `_classic` se precisar de Streaming nativo.

---

## Fluxo do Job (3 Tasks)

```
Task 1: discover_boards
  └─ Consulta API Monday.com
  └─ Publica lista de board_ids via dbutils.jobs.taskValues.set()
        ↓
Task 2: foreach_ingest  (depende de Task 1 — ForEach)
  └─ Uma sub-task por board_id (paralelo)
  └─ Cada iteração ingere: boards, items, groups, tags, activity_logs
  └─ Tabelas criadas na primeira execução durante a ingestão
        ↓
Task 3: ingest_account_tables  (depende de Task 2)
  └─ Ingere tabelas account-wide: users, workspaces, teams, updates
  └─ Roda UMA vez após o ForEach — evita ConcurrentWriteException
```

> **Por que separar account-wide tables?** Tabelas como `users` e `workspaces` não são filtradas por board. Se cada iteração do ForEach tentasse escrevê-las simultaneamente, ocorreria `ConcurrentWriteException`. A Task 3 roda uma única vez após todas as iterações.

---

## Tabelas por task

| Tabela | Task | Tipo | Board-scoped? | Storage |
|--------|------|------|---------------|---------|
| `boards` | Task 2 (ForEach) | CDC | Sim | EXTERNAL |
| `items` | Task 2 (ForEach) | CDC | Sim | EXTERNAL |
| `groups` | Task 2 (ForEach) | Snapshot | Sim | EXTERNAL |
| `tags` | Task 2 (ForEach) | Snapshot | Sim | EXTERNAL |
| `activity_logs` | Task 2 (ForEach) | CDC | Sim | EXTERNAL |
| `users` | Task 3 | Snapshot (overwrite) | Não | EXTERNAL |
| `workspaces` | Task 3 | Snapshot (overwrite) | Não | EXTERNAL |
| `teams` | Task 3 | Snapshot (overwrite) | Não | EXTERNAL |
| `updates` | Task 3 | Snapshot (overwrite) | Não | EXTERNAL |
| `_lakeflow_offsets` | Task 2 (criado na ingestão) | MANAGED | — | Metastore |

---

## Diferença para tabelas gerenciadas

| | Managed ForEach (`managed_foreach/`) | External ForEach (este) |
|---|---|---|
| Storage | Metastore default | Cloud storage (S3/ADLS/GCS) |
| `DROP TABLE` apaga dados? | Sim | **Não** |
| Requer External Location? | Não | **Sim** |
| Portabilidade dos dados | Baixa | Alta |

---

## Pré-requisitos

- Databricks workspace com Unity Catalog habilitado
- Uma **UC External Location** cobrindo o `external_location_base`
- Permissão `WRITE FILES` na External Location
- Permissão `CREATE TABLE` e `MODIFY` no schema alvo
- Secret `monday/api_token` configurado:
  ```bash
  databricks secrets create-scope monday
  databricks secrets put-secret monday api_token
  ```
- Repo adicionado ao workspace

---

## Passo 1 — Criar a UC External Location

> Pule se já existir uma External Location cobrindo seu path.

```sql
-- AWS
CREATE EXTERNAL LOCATION monday_foreach_ext_location
  URL 's3://meu-bucket/monday_foreach'
  WITH (STORAGE CREDENTIAL minha_credential);

-- Azure
CREATE EXTERNAL LOCATION monday_foreach_ext_location
  URL 'abfss://container@account.dfs.core.windows.net/monday_foreach'
  WITH (STORAGE CREDENTIAL minha_credential);

-- GCP
CREATE EXTERNAL LOCATION monday_foreach_ext_location
  URL 'gs://meu-bucket/monday_foreach'
  WITH (STORAGE CREDENTIAL minha_credential);
```

Verificar:
```sql
SHOW EXTERNAL LOCATIONS;
```

---

## Passo 2 — Adicionar o repositório ao Workspace

1. **Repos** → **Add Repo**
2. URL: `https://github.com/junior-esteca_data/lakeflow-monday-connector`
3. Provider: **GitHub** → **Create Repo**

---

## Passo 3 — Configurar os notebooks (opcional)

Todos os notebooks aceitam parâmetros via widgets. Para adaptar ao seu ambiente, passe como `base_parameters` no job:

| Widget | Default | Notebooks |
|--------|---------|-----------|
| `catalog` | `webmotors_demo_catalog` | Todos exceto `discover_boards` |
| `schema` | `monday_external_foreach` | Todos exceto `discover_boards` |
| `external_location_base` | S3 do ambiente webmotors | Todos exceto `discover_boards` |
| `input` | *(vazio)* | Apenas `foreach_ingest` (injetado pelo ForEach) |

### Layout de storage gerado automaticamente

```
s3://meu-bucket/monday_foreach/
  boards/               ← dados Delta da tabela boards (EXTERNAL)
  items/
  groups/
  tags/
  activity_logs/
  users/
  workspaces/
  teams/
  updates/
```

---

## Passo 4 — Criar o Job

### Serverless — Via Databricks CLI

```bash
databricks jobs create --json '{
  "name": "monday-lakeflow-external-foreach",
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
      "task_key": "foreach_ingest",
      "depends_on": [{"task_key": "discover_boards"}],
      "for_each_task": {
        "inputs": "{{tasks.discover_boards.values.board_ids_json}}",
        "concurrency": 5,
        "task": {
          "task_key": "foreach_ingest_iteration",
          "environment_key": "default",
          "notebook_task": {
            "notebook_path": "/Repos/<seu-email>/lakeflow-monday-connector/ingest/external_foreach/monday_uc_external_foreach_ingest",
            "source": "WORKSPACE",
            "base_parameters": {
              "input": "{{input}}"
            }
          }
        }
      }
    },
    {
      "task_key": "ingest_account_tables",
      "depends_on": [{"task_key": "foreach_ingest"}],
      "environment_key": "default",
      "notebook_task": {
        "notebook_path": "/Repos/<seu-email>/lakeflow-monday-connector/ingest/external_foreach/monday_uc_external_foreach_account_tables",
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
  "name": "monday-lakeflow-external-foreach-classic",
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
      "task_key": "foreach_ingest",
      "depends_on": [{"task_key": "discover_boards"}],
      "for_each_task": {
        "inputs": "{{tasks.discover_boards.values.board_ids_json}}",
        "concurrency": 5,
        "task": {
          "task_key": "foreach_ingest_iteration",
          "existing_cluster_id": "<cluster_id_dbr_14_3_plus>",
          "notebook_task": {
            "notebook_path": "/Repos/<seu-email>/lakeflow-monday-connector/ingest/external_foreach/monday_uc_external_foreach_classic_ingest",
            "source": "WORKSPACE",
            "base_parameters": {
              "input": "{{input}}"
            }
          }
        }
      }
    },
    {
      "task_key": "ingest_account_tables",
      "depends_on": [{"task_key": "foreach_ingest"}],
      "existing_cluster_id": "<cluster_id_dbr_14_3_plus>",
      "notebook_task": {
        "notebook_path": "/Repos/<seu-email>/lakeflow-monday-connector/ingest/external_foreach/monday_uc_external_foreach_account_tables_classic",
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

> A versão classic requer cluster com **DBR 14.3+**.

### Via UI (Serverless)

1. **Workflows** → **Create Job**
2. **Task 1** — `discover_boards`:
   - Type: Notebook
   - Path: `.../ingest/discover_boards`
   - Environment: `requests>=2.28.0`, `pydantic>=2.0.0`
3. **Task 2** — `foreach_ingest`:
   - Type: **For each**
   - Inputs: `{{tasks.discover_boards.values.board_ids_json}}`
   - Concurrency: 5
   - Depends on: `discover_boards`
   - Inner task: Notebook → `.../external_foreach/monday_uc_external_foreach_ingest`
   - Inner task base_parameters: `input = {{input}}`
4. **Task 3** — `ingest_account_tables`:
   - Type: Notebook
   - Path: `.../ingest/external_foreach/monday_uc_external_foreach_account_tables`
   - Depends on: `foreach_ingest`

---

## Passo 5 — Executar e verificar

```bash
# Disparar run
databricks jobs run-now <JOB_ID> --no-wait

# Acompanhar
databricks jobs get-run <RUN_ID>
```

### Verificar tabelas criadas

```sql
SHOW TABLES IN meu_catalog.monday_external_foreach;
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

### Verificar localização das tabelas

```sql
DESCRIBE EXTENDED meu_catalog.monday_external_foreach.boards;
-- Location: s3://meu-bucket/monday_foreach/boards
```

---

## Re-sincronização completa (CDC)

```sql
DELETE FROM meu_catalog.monday_external_foreach._lakeflow_offsets
WHERE table_name IN ('boards', 'items');
```

---

## Troubleshooting

### `PERMISSION_DENIED: WRITE FILES on External Location`

```sql
GRANT WRITE FILES ON EXTERNAL LOCATION monday_foreach_ext_location
TO `seu-service-principal@tenant.com`;
```

### `This location is not allowed by any External Location`

O `external_location_base` não está coberto por nenhuma External Location registrada.

```sql
SHOW EXTERNAL LOCATIONS;
```

### `ConcurrentAppendException` na Task 2

Ocorre em MERGEs simultâneos — tratado automaticamente via retry com exponential backoff (até 5 tentativas). Se persistir, reduza o `concurrency` do ForEach.

### Task 2 falha com "Widget 'input' is empty"

A Task 3 foi executada manualmente fora do ForEach. Para testar interativamente, defina o widget `input` com um board ID específico no notebook.

### Tabelas aparecem como MANAGED ao invés de EXTERNAL

Certifique-se de que `external_location_base` está definido nos widgets/parâmetros e que o path é coberto por uma External Location registrada.
