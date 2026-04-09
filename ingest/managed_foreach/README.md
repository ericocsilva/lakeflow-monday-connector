# Ingestão Monday.com → Tabelas Gerenciadas com ForEach por Board

Pipeline de **3 tasks** que descobre todos os boards do Monday.com e processa cada board em paralelo em tasks independentes, ingerindo dados como tabelas Delta **gerenciadas** no Unity Catalog.

Diferente do padrão Dynamic (2 tasks), aqui cada board é executado em uma task separada — o Databricks cria um cluster ou serverless context independente por board, maximizando o paralelismo.

## Versões disponíveis nesta pasta

| Notebook | Compute | Pipeline interno | Runtime mínimo |
|----------|---------|-----------------|----------------|
| `monday_uc_managed_foreach_ingest.py` | **Serverless** / qualquer DBR | `ingestion_pipeline_direct` | DBR 12.x+ |
| `monday_uc_managed_foreach_classic_ingest.py` | **Classic cluster** | `ingestion_pipeline_hms` (`readStream + foreachBatch`) | DBR 14.3+ |

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

| Tabela | Task | Tipo | Board-scoped? |
|--------|------|------|---------------|
| `boards` | Task 2 (ForEach) | CDC | Sim |
| `items` | Task 2 (ForEach) | CDC | Sim |
| `groups` | Task 2 (ForEach) | Snapshot | Sim |
| `tags` | Task 2 (ForEach) | Snapshot | Sim |
| `activity_logs` | Task 2 (ForEach) | CDC | Sim |
| `users` | Task 3 | Snapshot (overwrite) | Não |
| `workspaces` | Task 3 | Snapshot (overwrite) | Não |
| `teams` | Task 3 | Snapshot (overwrite) | Não |
| `updates` | Task 3 | Snapshot (overwrite) | Não |
| `_lakeflow_offsets` | Task 2 (criado na ingestão) | MANAGED | — |

---

## Pré-requisitos

- Databricks workspace com Unity Catalog habilitado
- Secret `monday/api_token` configurado:
  ```bash
  databricks secrets create-scope monday
  databricks secrets put-secret monday api_token
  ```
- Permissão `CREATE TABLE` e `MODIFY` no catálogo alvo
- Repo adicionado ao workspace

---

## Passo 1 — Adicionar o repositório ao Workspace

1. **Repos** → **Add Repo**
2. URL: `https://github.com/junior-esteca_data/lakeflow-monday-connector`
3. Provider: **GitHub** → **Create Repo**

---

## Passo 2 — Configurar os notebooks (opcional)

Todos os notebooks aceitam os parâmetros via widgets. Os defaults estão configurados para o ambiente de demo. Para adaptar ao seu ambiente, passe os parâmetros como `base_parameters` no job:

| Widget | Default | Notebooks |
|--------|---------|-----------|
| `catalog` | `classic_stable_hj897w_catalog` | Todos exceto `discover_boards` |
| `schema` | `monday_foreach` | Todos exceto `discover_boards` |
| `input` | *(vazio)* | Apenas `foreach_ingest` (injetado pelo ForEach) |

---

## Passo 3 — Criar o Job

### Serverless — Via Databricks CLI

```bash
databricks jobs create --json '{
  "name": "monday-lakeflow-managed-foreach",
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
            "notebook_path": "/Repos/<seu-email>/lakeflow-monday-connector/ingest/managed_foreach/monday_uc_managed_foreach_ingest",
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
        "notebook_path": "/Repos/<seu-email>/lakeflow-monday-connector/ingest/managed_foreach/monday_uc_managed_foreach_account_tables",
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
  "name": "monday-lakeflow-managed-foreach-classic",
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
            "notebook_path": "/Repos/<seu-email>/lakeflow-monday-connector/ingest/managed_foreach/monday_uc_managed_foreach_classic_ingest",
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
        "notebook_path": "/Repos/<seu-email>/lakeflow-monday-connector/ingest/managed_foreach/monday_uc_managed_foreach_account_tables_classic",
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
   - Inner task: Notebook → `.../managed_foreach/monday_uc_managed_foreach_ingest`
   - Inner task base_parameters: `input = {{input}}`
4. **Task 3** — `ingest_account_tables`:
   - Type: Notebook
   - Path: `.../ingest/managed_foreach/monday_uc_managed_foreach_account_tables`
   - Depends on: `foreach_ingest`

---

## Passo 4 — Executar e verificar

```bash
# Disparar run
databricks jobs run-now <JOB_ID> --no-wait

# Acompanhar
databricks jobs get-run <RUN_ID>
```

### Verificar tabelas criadas

```sql
SHOW TABLES IN meu_catalog.monday_foreach;
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

---

## Re-sincronização completa (CDC)

```sql
-- Resetar offset de todos os boards para boards e items
DELETE FROM meu_catalog.monday_foreach._lakeflow_offsets
WHERE table_name IN ('boards', 'items');
```

---

## Comparação com outros padrões

| | Static (`managed/`) | Dynamic (`managed_dynamic/`) | ForEach (este) |
|---|---|---|---|
| Boards | Hardcoded | Descobertos automaticamente | Descobertos automaticamente |
| Paralelismo | 1 task | 1 task (todos boards juntos) | 1 sub-task **por board** |
| Tasks no job | 1 | 2 | 4 |
| Escalabilidade | Baixa | Média | **Alta** |
| Quando usar | Poucos boards fixos | Múltiplos boards, sequencial | Muitos boards, máximo paralelismo |

---

## Troubleshooting

### `ConcurrentAppendException` na Task 2

Ocorre em MERGEs simultâneos — tratado automaticamente via retry com exponential backoff (até 5 tentativas). Se persistir, reduza o `concurrency` do ForEach.

### Task 2 falha com "Widget 'input' is empty"

A Task 3 foi executada manualmente fora do ForEach. Para testar interativamente, defina o widget `input` com um board ID específico no notebook.
