# Lakeflow Declarative Pipelines — o que validamos

> Spike feita em `dtb-iop-raw-elt` (2026-07-22/23) pra decidir se o Oracle deveria virar Lakeflow Declarative Pipelines em vez de script Python + Job. **Validado com sucesso, de ponta a ponta, via GitOps.** Este documento é a receita pra replicar o padrão em outra camada (ex.: silver).

## O que é

Lakeflow Declarative Pipelines (antigo Delta Live Tables) é o framework nativo do Databricks pra pipeline de dados **declarativo**: você define uma tabela como o resultado de uma query, e o framework cuida de orquestração, incremental, qualidade de dado e observabilidade sozinho — em vez de escrever o passo a passo imperativo (ler, transformar, escrever).

## Quando encaixa (e quando não)

| Encaixa bem | Não encaixa |
|---|---|
| "Declarar uma tabela a partir de uma query" — leituras de tabela, joins, agregações | Efeitos colaterais puros (ex.: copiar arquivo de um SMB pra um Volume) — não é uma transformação de tabela |
| Fontes que dão pra ler com `spark.read.table(...)` — Delta, Lakehouse Federation, JDBC | Lógica muito imperativa/stateful (ex.: loop de threads processando em blocos de ID) — dá pra forçar, mas é gambiarra |

## Arquitetura validada

```
GitHub (push/workflow_dispatch)
        │
        ▼
GitHub Action (environment: dev, OIDC — sem secret)
        │  autentica como service principal via Workload Identity Federation
        ▼
databricks bundle deploy --target dev   (cria/atualiza a pipeline)
databricks bundle run <nome-da-pipeline> --target dev
        │
        ▼
Lakeflow Pipeline (compute CLÁSSICO, não serverless)
        │  precisa ser clássico pra ter rota de rede pra fontes privadas
        │  (Oracle via VPN OCI, GED via VPN on-premises, etc.)
        ▼
Lê a fonte (ex.: oracle_med4u.tasy.<TABELA>, via Lakehouse Federation)
        │
        ▼
Materializa a tabela em Unity Catalog (ex.: iop.raw.<tabela>)
```

## Receita passo a passo

### 1. Service principal dedicado

Um SP por projeto/repo (não reaproveitar entre projetos — isola blast radius). Ver `sp-iop-elt` como referência.

```bash
databricks account service-principals create --json '{"displayName": "sp-<nome-projeto>", "active": true}'
```

### 2. Grants de dados (least-privilege, aditivo)

O SP precisa de:
- Grant no(s) catálogo(s)/schema(s) de **destino** (onde ele vai escrever).
- Grant nas fontes que ele vai **ler** — se for Lakehouse Federation, isso é em 2 camadas:
  ```sql
  GRANT USE_CONNECTION ON CONNECTION <connection> TO `sp-<nome>`;
  GRANT USE_CATALOG ON CATALOG <foreign_catalog> TO `sp-<nome>`;
  GRANT USE_SCHEMA, SELECT ON SCHEMA <foreign_catalog>.<schema> TO `sp-<nome>`;
  ```

### 3. `allow-cluster-create` — pegadinha descoberta na prática

Grant de Unity Catalog **não é suficiente** pra rodar uma pipeline em compute clássico — o SP também precisa da entitlement de **workspace** `allow-cluster-create`, senão a pipeline falha com `PERMISSION_DENIED: You are not authorized to create clusters` na hora de subir o cluster (o deploy passa, só falha ao executar).

```bash
# GET primeiro pra pegar o objeto atual, depois UPDATE (não PATCH -- patch deu erro de decode)
databricks service-principals update <workspace-scim-id> --json '{
  "schemas": ["urn:ietf:params:scim:schemas:core:2.0:ServicePrincipal"],
  "applicationId": "<application-id>",
  "displayName": "sp-<nome>",
  "active": true,
  "entitlements": [{"value": "allow-cluster-create"}]
}'
```

### 4. Federation policy (OIDC, GitHub Actions → Databricks, sem secret)

Duas pegadinhas descobertas só testando de verdade (a doc genérica não deixa isso claro):

1. **`audience`** não é o account id — é a **URL do endpoint OIDC do workspace específico**: `https://<workspace-host>/oidc/v1/token`.
2. **`subject`** inclui os **IDs imutáveis** de org e repo do GitHub, não só os nomes: `repo:<org>@<org_id>/<repo>@<repo_id>:environment:<dev|prod>`. Descubra o valor certo rodando uma vez e lendo o erro `TOKEN_SUBJECT_INVALID` — ele devolve o subject exato esperado.

```bash
databricks account service-principal-federation-policy create <sp-account-id> --json '{
  "oidc_policy": {
    "issuer": "https://token.actions.githubusercontent.com",
    "audiences": ["https://<workspace-host>/oidc/v1/token"],
    "subject": "repo:<org>@<org_id>/<repo>@<repo_id>:environment:<dev|prod>"
  }
}'
```

### 5. GitHub Environments (`dev`/`prod`)

`prod` restrito à branch `main` (`deployment_branch_policy`) e disparado só via `workflow_dispatch` — nenhuma automação dispara prod sozinha.

### 6. `databricks.yml` — resource da pipeline

```yaml
resources:
  pipelines:
    <nome_da_pipeline>:
      name: <nome_da_pipeline>
      catalog: <catalogo_destino>
      schema: <schema_destino>
      channel: PREVIEW        # exigido pra ler de Lakehouse Federation
      continuous: false       # roda uma vez e para (não streaming contínuo)
      serverless: false       # compute clássico -- necessário pra rede privada
      clusters:
        - label: default
          node_type_id: <mesmo node type dos clusters existentes>
          num_workers: 1

      libraries:
        - glob:
            include: ../pipelines/<pasta>/transformations/**
```

### 7. Transformação (Python)

```python
from pyspark import pipelines as dp

@dp.table(name="<nome_tabela_destino>")
def <nome_tabela_destino>():
    return spark.read.table("<catalogo_origem>.<schema>.<TABELA>")
```

### 8. GitHub Action (deploy + run)

```yaml
permissions:
  id-token: write
  contents: read

jobs:
  run-pipeline:
    environment: dev
    env:
      DATABRICKS_AUTH_TYPE: github-oidc
      DATABRICKS_HOST: https://<workspace-host>
      DATABRICKS_CLIENT_ID: <application-id-do-sp>
    steps:
      - uses: actions/checkout@v4
      - uses: databricks/setup-cli@main
      - run: databricks bundle deploy --target dev
      - run: databricks bundle run <nome_da_pipeline> --target dev
```

## Resultado da validação (2026-07-22/23)

Pipeline `oracle_pipeline_test` rodou de ponta a ponta: leu `oracle_med4u.tasy.PACIENTE_ATEND_MEDIC` via Lakehouse Federation, num cluster clássico próprio, e materializou `iop.raw._test_lakeflow_paciente_atend_medic` com o schema real da tabela Oracle (confirmado via `databricks tables get`). Zero secret armazenado no GitHub.
