# Ingestão Monday.com → Tabelas Externas (Unity Catalog)

Pipeline que ingere dados do Monday.com como tabelas Delta **externas** no Unity Catalog.
Os dados ficam no seu cloud storage — `DROP TABLE` **não** apaga os dados.

## Versões disponíveis nesta pasta

| Notebook | Compute | Pipeline interno | Runtime mínimo |
|----------|---------|-----------------|----------------|
| `monday_uc_external_ingest.py` | **Serverless** / qualquer DBR | `ingestion_pipeline_uc_external_direct` | DBR 12.x+ |
| `monday_uc_external_classic_ingest.py` | **Classic cluster** | `ingestion_pipeline_uc_external` (`readStream + foreachBatch`) | DBR 14.3+ |

**Diferença principal:** a versão `_classic` usa Spark Structured Streaming com checkpoints em DBFS (parâmetro `checkpoint_base`).
Ambas criam tabelas **EXTERNAL** no mesmo path de cloud.

> Use a versão serverless por padrão. Use `_classic` se precisar de Streaming nativo.

---

## Diferença para tabelas gerenciadas

| | Managed (`ingest/managed/`) | External (`ingest/external/`) |
|---|---|---|
| Storage dos dados | Metastore default (gerenciado pelo UC) | Seu cloud storage (S3, ADLS, GCS) |
| `DROP TABLE` apaga dados? | **Sim** | **Não** |
| Requer External Location? | Não | **Sim** |
| Custo de storage | Incluído no workspace | No seu cloud provider |
| Portabilidade dos dados | Baixa | Alta |

---

## Pré-requisitos

- Databricks workspace com Unity Catalog habilitado
- Uma **UC External Location** apontando para seu bucket/container
- Permissão `WRITE FILES` na External Location
- Permissão `CREATE TABLE` e `MODIFY` no schema alvo
- Token pessoal da API Monday.com

---

## Passo 1 — Obter o token da API Monday.com

1. Acesse seu workspace Monday.com
2. Clique na foto de perfil (canto inferior esquerdo) → **Developers**
3. Aba **My Access Tokens** → **Show**
4. Copie o token

---

## Passo 2 — Criar o Databricks Secret

### Via Databricks CLI

```bash
# Criar o scope (uma vez por workspace)
databricks secrets create-scope monday

# Salvar o token
databricks secrets put-secret monday api_token
# Cole o token quando solicitado
```

### Via API REST

```bash
# Criar scope
databricks api post /api/2.0/secrets/scopes/create \
  --json '{"scope": "monday"}'

# Salvar token
databricks api post /api/2.0/secrets/put \
  --json '{
    "scope": "monday",
    "key": "api_token",
    "string_value": "<SEU_TOKEN>"
  }'
```

### Verificar

```bash
databricks secrets list-secrets monday
# Saída esperada:
# Key        Last Updated Timestamp
# api_token  1775073763321
```

---

## Passo 3 — Criar a UC External Location

> Pule este passo se a External Location já existir no seu workspace.

Execute uma vez no **Databricks SQL** ou em um notebook com privilégio de admin:

### AWS (S3)

```sql
CREATE EXTERNAL LOCATION monday_ext_location
  URL 's3://meu-bucket/monday'
  WITH (STORAGE CREDENTIAL minha_storage_credential);
```

### Azure (ADLS Gen2)

```sql
CREATE EXTERNAL LOCATION monday_ext_location
  URL 'abfss://container@account.dfs.core.windows.net/monday'
  WITH (STORAGE CREDENTIAL minha_storage_credential);
```

### GCP (GCS)

```sql
CREATE EXTERNAL LOCATION monday_ext_location
  URL 'gs://meu-bucket/monday'
  WITH (STORAGE CREDENTIAL minha_storage_credential);
```

### Verificar

```sql
SHOW EXTERNAL LOCATIONS;
-- ou via CLI:
-- databricks external-locations list
```

---

## Passo 4 — Adicionar o repositório ao Workspace

No Databricks:
1. **Repos** → **Add Repo**
2. URL: `https://github.com/junior-esteca_data/lakeflow-monday-connector`
3. Provider: **GitHub**
4. Confirmar em **Create Repo**

O repo será criado em:
```
/Repos/<seu-email>/lakeflow-monday-connector
```

---

## Passo 5 — Configurar o notebook

Abra o notebook:
```
/Repos/<seu-email>/lakeflow-monday-connector/ingest/external/monday_uc_external_ingest.py
```

Edite o `pipeline_spec` conforme seu ambiente:

```python
pipeline_spec = {
    "api_token": dbutils.secrets.get(scope="monday", key="api_token"),

    # UC destination
    "destination_catalog": "meu_catalog",        # catálogo UC existente
    "destination_schema":  "monday_external",    # schema criado automaticamente

    # External Location base — deve estar sob uma External Location registrada
    "external_location_base": "s3://meu-bucket/monday",
    # Azure: "abfss://container@account.dfs.core.windows.net/monday"
    # GCS:   "gs://meu-bucket/monday"

    "objects": [
        {
            "table": {
                "source_table": "boards",
                "table_configuration": {
                    "state": "active",            # active | all | archived | deleted
                }
            }
        },
        {
            "table": {
                "source_table": "items",
                "table_configuration": {
                    "board_ids": "123,456",       # opcional: filtrar boards específicos
                }
            }
        },
        {"table": {"source_table": "users"}},
        {"table": {"source_table": "workspaces"}},
        {"table": {"source_table": "teams"}},
        {"table": {"source_table": "groups"}},
        {"table": {"source_table": "tags"}},
        {"table": {"source_table": "updates"}},
        {"table": {"source_table": "activity_logs"}},
    ],
}
```

### Layout de storage gerado automaticamente

```
s3://meu-bucket/monday/
  boards/               ← dados Delta da tabela boards
  items/                ← dados Delta da tabela items
  users/
  workspaces/
  teams/
  groups/
  tags/
  updates/
  activity_logs/
```

---

## Passo 6 — Criar o Job

### Via Databricks CLI

```bash
databricks jobs create --json '{
  "name": "monday-lakeflow-external-ingest",
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
      "task_key": "ingest_monday_external",
      "environment_key": "default",
      "notebook_task": {
        "notebook_path": "/Repos/<seu-email>/lakeflow-monday-connector/ingest/external/monday_uc_external_ingest",
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

> Substitua `<seu-email>` pelo seu email Databricks.

### Via UI

1. **Workflows** → **Create Job**
2. Task type: **Notebook**
3. Source: **Workspace**
4. Path: `/Repos/<seu-email>/lakeflow-monday-connector/ingest/external/monday_uc_external_ingest`
5. Environment: adicione `requests>=2.28.0` e `pydantic>=2.0.0` em **Environment & Libraries**
6. Schedule: configure conforme necessidade

---

## Passo 7 — Executar e verificar

### Execução manual

```bash
# Disparar run imediato
databricks jobs run-now <JOB_ID> --no-wait

# Acompanhar status
databricks jobs get-run <RUN_ID>
```

### Verificar tabelas criadas

```sql
SHOW TABLES IN meu_catalog.monday_external;
```

Saída esperada:

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

> `_lakeflow_offsets` é MANAGED pois armazena apenas metadados de controle — não há necessidade de expô-la como external.

### Verificar localização das tabelas

```sql
DESCRIBE EXTENDED meu_catalog.monday_external.boards;
-- Coluna "Location" mostrará: s3://meu-bucket/monday/boards
```

---

## Comportamento por tabela

| Tabela | Sync | Descrição |
|--------|------|-----------|
| `boards` | CDC incremental | Usa activity logs para detectar mudanças |
| `items` | CDC incremental | Usa activity logs para detectar mudanças |
| `users` | Full refresh | Substituído completamente a cada run |
| `workspaces` | Full refresh | Substituído completamente a cada run |
| `teams` | Full refresh | Substituído completamente a cada run |
| `groups` | Full refresh | Substituído completamente a cada run |
| `tags` | Full refresh | Substituído completamente a cada run |
| `updates` | Full refresh | Substituído completamente a cada run |
| `activity_logs` | Full refresh | Substituído completamente a cada run |

O offset das tabelas CDC é armazenado em `_lakeflow_offsets` (tabela gerenciada no mesmo schema).

---

## Re-sincronização completa (CDC)

Para forçar um full refresh das tabelas `boards` ou `items`:

```sql
-- Apagar o offset da tabela desejada
DELETE FROM meu_catalog.monday_external._lakeflow_offsets
WHERE table_name = 'boards';
```

Na próxima execução do job, o conector fará um snapshot completo antes de retomar o CDC.

---

## Opções disponíveis por tabela

| Opção | Tabelas | Descrição |
|-------|---------|-----------|
| `state` | boards, workspaces | `active`, `all`, `archived`, `deleted` |
| `kind` | users | `all`, `guests`, `non_guests`, `non_pending` |
| `board_ids` | items, groups, tags, activity_logs | IDs separados por vírgula |
| `page_size` | boards, items, users | Registros por página da API (padrão: 50/100) |
| `scd_type` | qualquer | `SCD_TYPE_1` (padrão), `SCD_TYPE_2`, `APPEND_ONLY` |
| `primary_keys` | qualquer | Override das PKs padrão |

---

## Troubleshooting

### `PERMISSION_DENIED: WRITE FILES on External Location`

O identity do job não tem permissão de escrita na External Location.

```sql
GRANT WRITE FILES ON EXTERNAL LOCATION monday_ext_location
TO `seu-service-principal@seu-tenant.com`;
```

### `This location is not allowed by any External Location`

O `external_location_base` não está coberto por nenhuma External Location registrada.
Verifique com `SHOW EXTERNAL LOCATIONS` se o path está correto.

### Tabelas aparecem como MANAGED ao invés de EXTERNAL

Certifique-se de que `external_location_base` está definido no `pipeline_spec` e que o path é acessível.
