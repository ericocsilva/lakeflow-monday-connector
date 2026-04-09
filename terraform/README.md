# Terraform — Monday.com Lakeflow Classic Jobs

Cria **4 Databricks Jobs** para as variantes classic (Spark Structured Streaming) do conector Monday.com → Unity Catalog.

| Job | Padrão | Tabelas | Tasks |
|-----|--------|---------|-------|
| `monday-lakeflow-managed-classic` | Static | Gerenciadas | 1 |
| `monday-lakeflow-external-classic` | Static | Externas | 1 |
| `monday-lakeflow-managed-dynamic-classic` | Dynamic | Gerenciadas | 2 |
| `monday-lakeflow-external-dynamic-classic` | Dynamic | Externas | 2 |

> Variantes **serverless** e **ForEach** não são cobertas aqui — use os READMEs de cada pasta em `ingest/` para criá-las via CLI ou UI.

---

## Pré-requisitos

- Terraform >= 1.3
- Databricks workspace com Unity Catalog habilitado
- Cluster classic com **DBR 14.3+** já criado no workspace
- Secret `monday/api_token` configurado no workspace:
  ```bash
  databricks secrets create-scope monday
  databricks secrets put-secret monday api_token
  ```
- Repo `lakeflow-monday-connector` adicionado ao workspace em **Repos**
- Para jobs externos: UC External Location cobrindo os paths de storage

---

## Quickstart

```bash
cd terraform

# 1. Copiar e preencher as variáveis
cp terraform.tfvars.example terraform.tfvars
# edite terraform.tfvars

# 2. Exportar o token (não commitar)
export TF_VAR_databricks_token="dapi..."

# 3. Inicializar e aplicar
terraform init
terraform plan
terraform apply
```

Após o `apply`, os outputs mostram os IDs e URLs dos jobs criados:

```
Outputs:

managed_classic_job_url          = "https://workspace.cloud.databricks.com/jobs/123456"
external_classic_job_url         = "https://workspace.cloud.databricks.com/jobs/123457"
managed_dynamic_classic_job_url  = "https://workspace.cloud.databricks.com/jobs/123458"
external_dynamic_classic_job_url = "https://workspace.cloud.databricks.com/jobs/123459"
```

---

## Variáveis

### Obrigatórias

| Variável | Descrição |
|----------|-----------|
| `databricks_host` | URL do workspace (ex: `https://adb-xxx.azuredatabricks.net`) |
| `databricks_token` | Personal access token — passe via `TF_VAR_databricks_token` |
| `cluster_id` | ID do cluster classic existente (DBR 14.3+) |
| `repo_path` | Path do repo no Workspace (ex: `/Repos/user@email.com/lakeflow-monday-connector`) |
| `catalog` | Catálogo UC onde os schemas serão criados |
| `external_location_base` | Path de cloud storage para tabelas externas estáticas |
| `external_dynamic_location_base` | Path de cloud storage para tabelas externas dinâmicas |

### Opcionais (com defaults)

| Variável | Default | Descrição |
|----------|---------|-----------|
| `managed_schema` | `monday` | Schema para tabelas gerenciadas estáticas |
| `managed_dynamic_schema` | `monday_dynamic` | Schema para tabelas gerenciadas dinâmicas |
| `external_schema` | `monday_external` | Schema para tabelas externas estáticas |
| `external_dynamic_schema` | `monday_external_dynamic` | Schema para tabelas externas dinâmicas |
| `checkpoint_base` | `/dbfs/checkpoints/lakeflow` | Base DBFS para checkpoints do Structured Streaming |
| `schedule_cron` | `0 0 * * * ?` | Expressão Quartz cron (padrão: todo hora) |
| `schedule_timezone` | `America/Sao_Paulo` | Timezone do schedule |
| `schedule_pause_status` | `PAUSED` | `PAUSED` ou `UNPAUSED` |

---

## Arquivos

```
terraform/
├── versions.tf              ← provider databricks/databricks ~> 1.50
├── variables.tf             ← todas as variáveis de entrada
├── main.tf                  ← 4 recursos databricks_job
├── outputs.tf               ← IDs e URLs dos jobs criados
├── terraform.tfvars.example ← template de configuração
└── .gitignore               ← exclui tfvars, state e .terraform/
```

---

## Parâmetros passados aos notebooks

Cada job passa os parâmetros como `base_parameters` (widgets) no notebook. Os checkpoints são compostos automaticamente com sufixo do schema:

| Job | base_parameters |
|-----|----------------|
| managed-classic | `catalog`, `schema`, `checkpoint_base=/dbfs/.../monday` |
| external-classic | `catalog`, `schema`, `external_location_base`, `checkpoint_base=/dbfs/.../monday_external` |
| managed-dynamic-classic | `catalog`, `schema`, `checkpoint_base=/dbfs/.../monday_dynamic` |
| external-dynamic-classic | `catalog`, `schema`, `external_location_base`, `checkpoint_base=/dbfs/.../monday_external_dynamic` |

O `api_token` não é passado como parâmetro — os notebooks o leem diretamente via `dbutils.secrets.get(scope="monday", key="api_token")`.

---

## Destruir jobs

```bash
terraform destroy
```

Isso remove apenas os **jobs** do Workspace. Não apaga dados, tabelas Delta, checkpoints ou o cluster.

---

## Troubleshooting

### `Error: cannot access cluster`

O `cluster_id` não existe ou está inacessível pelo token configurado. Verifique:
```bash
databricks clusters get --cluster-id <cluster_id>
```

### `Error: Path not found`

O `repo_path` não existe no Workspace. Adicione o repo em **Repos** antes de aplicar o Terraform.

### `Error: PERMISSION_DENIED on External Location`

O identity do job não tem `WRITE FILES` na External Location. Conceda:
```sql
GRANT WRITE FILES ON EXTERNAL LOCATION <nome>
TO `seu-service-principal@tenant.com`;
```
