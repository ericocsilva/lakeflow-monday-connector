# Jobs — Classic Compute · External Tables

Todos os jobs abaixo utilizam **classic compute** (DBR 14.3+) e armazenam os dados como tabelas Delta **externas** no S3.

---

## Job 1 — Static External Classic

Pipeline simples com **1 task**. Os board IDs são fixos no código.

- **Nome no workspace:** `monday-external-static-classic`
- **Link:** https://e2-demo-field-eng.cloud.databricks.com/jobs/297203093123194
- **Notebook:** `ingest/external/monday_uc_external_classic_ingest.py`
- **Pipeline interno:** `ingestion_pipeline_uc_external.py` (Spark Structured Streaming + foreachBatch)

---

## Job 2 — Dynamic External Classic

**2 tasks encadeadas.** Descobre os boards automaticamente via API do Monday.com em runtime — nenhum board ID precisa ser hardcoded.

- **Nome no workspace:** `monday-external-dynamic-classic`
- **Link:** https://e2-demo-field-eng.cloud.databricks.com/jobs/549969933221067
- **Task 1:** `ingest/discover_boards.py` — lista todos os boards ativos e publica os IDs via Task Values
- **Task 2:** `ingest/external_dynamic/monday_uc_external_dynamic_classic_ingest.py` — lê os IDs e ingere todos os boards
- **Pipeline interno:** `ingestion_pipeline_uc_external.py`

---

## Job 3 — ForEach External Classic

**3 tasks.** Igual ao Dynamic, mas cada board é processado em uma task independente, em sequência.

- **Nome no workspace:** `monday-external-foreach-classic`
- **Link:** https://e2-demo-field-eng.cloud.databricks.com/jobs/35924933949654
- **Task 1:** `ingest/discover_boards.py` — descobre os boards e publica os IDs
- **Task 2 (ForEach):** `ingest/external_foreach/monday_uc_external_foreach_classic_ingest.py` — executa uma iteração por board (concurrency = 1)
- **Task 3:** `ingest/external_foreach/monday_uc_external_foreach_account_tables_classic.py` — ingere tabelas account-wide (`users`, `workspaces`, `teams`, `updates`) uma única vez após o ForEach
- **Pipeline interno:** `ingestion_pipeline_uc_external.py`

> **Por que a Task 3 separada?** Tabelas como `users` e `workspaces` não são filtradas por board. Se cada iteração do ForEach tentasse escrevê-las simultaneamente, ocorreria `ConcurrentWriteException`. A Task 3 roda uma única vez após todas as iterações.

---

## Comparação

| | Board IDs | Tasks | Paralelismo |
|---|---|---|---|
| **Static** | Fixos no código | 1 | — |
| **Dynamic** | Descobertos automaticamente | 2 | Todos os boards em uma task |
| **ForEach** | Descobertos automaticamente | 3 | 1 task por board (sequencial) |
