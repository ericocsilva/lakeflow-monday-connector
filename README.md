# Monday.com → Databricks — Lakeflow Community Connector

Conector que ingere dados do Monday.com como tabelas Delta no Databricks usando o
[Lakeflow Community Connector](https://github.com/colin-gibbons/lakeflow-community-connectors).

---

## Visão geral das versões

O repositório oferece **3 padrões de pipeline** × **2 tipos de tabela** × **2 modos de compute** = **12 combinações de notebook**, organizadas em 6 pastas dentro de `ingest/`.

### Padrões de pipeline

| Padrão | Pasta | Board IDs | Estrutura do Job |
|--------|-------|-----------|-----------------|
| **Static** | `managed/`, `external/` | Hardcoded no notebook (ou via widget) | 1 task |
| **Dynamic** | `managed_dynamic/`, `external_dynamic/` | Descobertos automaticamente via API | 2 tasks (discover → ingest) |
| **ForEach** | `managed_foreach/`, `external_foreach/` | Descobertos automaticamente; cada board em task separada | 4 tasks (discover → init → foreach → account_tables) |

### Tipos de tabela

| Tipo | Comportamento | `DROP TABLE` apaga dados? |
|------|--------------|--------------------------|
| **Managed** | Dados no storage padrão do metastore | Sim |
| **External** | Dados em path de cloud (S3/ADLS/GCS) sob uma External Location | **Não** |

### Modos de compute

| Sufixo | Pipeline interno | Compute | Runtime mínimo |
|--------|-----------------|---------|----------------|
| *(sem sufixo)* | `ingestion_pipeline_direct` / `_uc_external_direct` | **Serverless** ou qualquer DBR | DBR 12.x+ |
| `_classic` | `ingestion_pipeline_hms` / `_uc_external` | **Classic cluster** | DBR 14.3+ |

> **Qual escolher?** Use a versão sem sufixo (serverless) por padrão. Use `_classic` se precisar de Spark Structured Streaming nativo (`readStream + foreachBatch`).

---

## Mapa completo de notebooks

```
ingest/
│
├── discover_boards.py                              ← Task 1 compartilhada (dynamic + foreach)
│
├── managed/
│   ├── monday_uc_managed_ingest.py                ← Static · Serverless
│   └── monday_uc_managed_classic_ingest.py        ← Static · Classic
│
├── external/
│   ├── monday_uc_external_ingest.py               ← Static · Serverless
│   └── monday_uc_external_classic_ingest.py       ← Static · Classic
│
├── managed_dynamic/
│   ├── monday_uc_managed_dynamic_ingest.py        ← Dynamic · Serverless
│   └── monday_uc_managed_dynamic_classic_ingest.py← Dynamic · Classic
│
├── external_dynamic/
│   ├── monday_uc_external_dynamic_ingest.py       ← Dynamic · Serverless
│   └── monday_uc_external_dynamic_classic_ingest.py← Dynamic · Classic
│
├── managed_foreach/
│   ├── monday_uc_managed_foreach_init_tables.py   ← ForEach Task 2: pré-cria tabelas
│   ├── monday_uc_managed_foreach_ingest.py        ← ForEach Task 3 (por board) · Serverless
│   ├── monday_uc_managed_foreach_account_tables.py← ForEach Task 4: tabelas account-wide · Serverless
│   ├── monday_uc_managed_foreach_classic_ingest.py← ForEach Task 3 (por board) · Classic
│   └── monday_uc_managed_foreach_account_tables_classic.py ← ForEach Task 4 · Classic
│
└── external_foreach/
    ├── monday_uc_external_foreach_init_tables.py  ← ForEach Task 2: pré-cria tabelas
    ├── monday_uc_external_foreach_ingest.py        ← ForEach Task 3 (por board) · Serverless
    ├── monday_uc_external_foreach_account_tables.py← ForEach Task 4: tabelas account-wide · Serverless
    ├── monday_uc_external_foreach_classic_ingest.py← ForEach Task 3 (por board) · Classic
    └── monday_uc_external_foreach_account_tables_classic.py ← ForEach Task 4 · Classic
```

---

## Pipelines internos (`pipeline/`)

Os notebooks são wrappers finos — a lógica de ingestão fica nos pipelines:

| Arquivo | Usado por | Compute | Destaques |
|---------|-----------|---------|-----------|
| `ingestion_pipeline_direct.py` | `managed` + `managed_*` serverless | Serverless / qualquer DBR | Sem DBFS; offsets em `_lakeflow_offsets` (Delta) |
| `ingestion_pipeline_uc_external_direct.py` | `external` + `external_*` serverless | Serverless / qualquer DBR | Tabelas externas sem Streaming API |
| `ingestion_pipeline_hms.py` | `managed` + `managed_*` classic | Classic (DBR 14.3+) | `readStream + foreachBatch`; checkpoint em DBFS |
| `ingestion_pipeline_uc_external.py` | `external` + `external_*` classic | Classic (DBR 14.3+) | `readStream + foreachBatch`; tabelas externas |

---

## Tabelas ingeridas

| Tabela | Primary Key | Sync | Board-scoped? |
|--------|-------------|------|---------------|
| `boards` | `id` | CDC | ✓ (filtro `board_ids`) |
| `items` | `id` | CDC | ✓ |
| `groups` | `id` | Snapshot (MERGE no ForEach) | ✓ |
| `tags` | `id` | Snapshot (MERGE no ForEach) | ✓ |
| `activity_logs` | `id` | CDC | ✓ |
| `users` | `id` | Snapshot | ✗ (account-wide) |
| `workspaces` | `id` | Snapshot | ✗ |
| `teams` | `id` | Snapshot | ✗ |
| `updates` | `id` | Snapshot | ✗ |

> **ForEach:** tabelas board-scoped ficam na Task 3 (ForEach); account-wide ficam na Task 4 para evitar `ConcurrentWriteException`.

---

## Pré-requisitos gerais

1. **Databricks workspace** com Unity Catalog habilitado
2. **Secret** `monday/api_token` configurado:
   ```bash
   databricks secrets create-scope monday
   databricks secrets put-secret monday api_token
   ```
3. **Repo** adicionado ao workspace:
   - Repos → Add Repo → URL deste repositório
4. Para tabelas **External**: uma UC External Location cobrindo o path de destino
5. Para compute **Classic**: cluster com DBR 14.3+

---

## Parâmetros (widgets)

Todos os notebooks aceitam parâmetros via `dbutils.widgets` — passados como `base_parameters` no job:

| Widget | Padrão | Aplicável a |
|--------|--------|-------------|
| `catalog` | `classic_stable_hj897w_catalog` | Todos |
| `schema` | Varia por variante | Todos |
| `external_location_base` | S3 do ambiente lakeflow | Somente external |
| `checkpoint_base` | `/dbfs/checkpoints/lakeflow/<schema>` | Somente classic |

---

## Guias detalhados por variante

- [`ingest/managed/README.md`](ingest/managed/README.md) — Static Managed
- [`ingest/external/README.md`](ingest/external/README.md) — Static External
- [`ingest/managed_dynamic/README.md`](ingest/managed_dynamic/README.md) — Dynamic Managed
- [`ingest/external_dynamic/README.md`](ingest/external_dynamic/README.md) — Dynamic External
- [`ingest/managed_foreach/README.md`](ingest/managed_foreach/README.md) — ForEach Managed
- [`ingest/external_foreach/README.md`](ingest/external_foreach/README.md) — ForEach External
