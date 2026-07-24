# dtb-iop-raw-elt

ELT do IOP: extração do Oracle (Tasy) e do GED (arquivos on-prem via SMB) para a camada `iop.raw` do Unity Catalog, na Databricks.

> **Status:** projeto em modernização. Hoje a ELT roda 100% como notebooks Databricks legados, em produção, sem versionamento em Git. Este repositório é o destino da migração para um modelo GitOps (Databricks Asset Bundle). **Nenhuma lógica foi portada ainda** — ver [Estado da migração](#estado-da-migração).
>
> Este README documenta o estado real do ambiente, verificado via `databricks` CLI (read-only) em **2026-07-21**. Como o ambiente pode mudar, reconfirme contra o workspace antes de assumir que algo aqui ainda é verdade.

---

## Sumário

- [Visão geral](#visão-geral)
- [Arquitetura atual](#arquitetura-atual)
- [Jobs](#jobs)
- [Clusters](#clusters)
- [Conectividade Oracle (Lakehouse Federation)](#conectividade-oracle-lakehouse-federation)
- [Catálogo `iop`](#catálogo-iop)
- [Modelo de dados — o que cada notebook faz](#modelo-de-dados--o-que-cada-notebook-faz)
- [Notebooks auxiliares / não agendados](#notebooks-auxiliares--não-agendados)
- [Consumidores conhecidos do `iop.raw`](#consumidores-conhecidos-do-iopraw)
- [Ownership atual e bus factor](#ownership-atual-e-bus-factor)
- [Achados de governança](#achados-de-governança)
- [Estado da migração](#estado-da-migração)
- [Como rodar / deployar (planejado)](#como-rodar--deployar-planejado)
- [Spike: Lakeflow Declarative Pipelines pro lado Oracle](#spike-lakeflow-declarative-pipelines-pro-lado-oracle)
- [Riscos conhecidos](#riscos-conhecidos)
- [Referências](#referências)

---

## Visão geral

O IOP (sistema de oncologia) tem duas fontes de dados operacionais que precisam chegar ao lakehouse:

1. **Oracle (Tasy)** — banco relacional do sistema hospitalar, acessado via **Unity Catalog Lakehouse Federation** (foreign catalog `oracle_med4u`).
2. **GED** — arquivos (majoritariamente PDF) num compartilhamento SMB on-prem, copiados para um **Volume** do Unity Catalog.

Ambos os fluxos escrevem na camada `iop.raw`, que é **a fonte de dados de todo o resto do IOP** — hoje consumida ao menos pelo pipeline `clinical-doc-extractor` (que por sua vez alimenta o app `desfecho-comissao`). Qualquer mudança aqui tem efeito cascata.

Todo o pipeline hoje roda como **notebooks Databricks em pasta pessoal** (`jefferson.silva@spesia.com.br`), sem controle de versão, orquestrados por 2 Jobs agendados. O objetivo deste repositório é portar essa lógica para código versionado + Databricks Asset Bundle, sem downtime, com o padrão já validado no projeto `clinical-doc-extractor`.

---

## Arquitetura atual

```
┌─────────────────────────┐        ┌──────────────────────────────┐
│  Oracle (Tasy)           │        │  GED (compartilhamento SMB)   │
│  host: prod01.subnet...  │        │  on-prem, via smbprotocol     │
└────────────┬─────────────┘        └───────────────┬──────────────┘
             │ UC Lakehouse Federation               │ dbutils.secrets + smbclient
             │ connection: oracle_prod01             │ scope: med4u_files_ged
             ▼                                       ▼
   oracle_med4u.tasy.*                    Volume: /Volumes/iop/raw/files/GED/
             │                                       │
             │ job: extracao_tabelas_oracle          │ job: load_files_ged
             │ (10 tasks, DAG)                       │ (2 tasks)
             ▼                                       ▼
                        iop.raw.*  (Unity Catalog, catálogo `iop`)
                                     │
                                     ▼
                    clinical-doc-extractor (job separado, dev)
                                     │
                                     ▼
                    /Volumes/iop/silver/desfecho/dados_clinicos
                                     │
                                     ▼
                         App "desfecho-comissao"
```

Nenhum dos dois jobs usa job clusters — todos os tasks rodam em **clusters interativos (`all-purpose`)** que ficam alocados ao job (`existing_cluster_id`), não em `new_cluster` efêmero. Isso é um dos itens de modernização (ver M1 mais abaixo).

---

## Jobs

### `extracao_tabelas_oracle` (id `988459100693658`)

- **Schedule:** `47 0 4 * * ?` America/Sao_Paulo — **UNPAUSED**, roda toda madrugada às 4h.
- **Owner / run-as:** `jefferson.silva@spesia.com.br`
- **Notebooks:** `/Workspace/Users/jefferson.silva@spesia.com.br/camada_raw/oracle/notebooks/`

DAG (10 tasks):

```
EXTRACT_VIEW_IOP_BI_PROJ_REC_VIEW  (independente, cluster clt_oracle_jdbc)

load_oracle  ──┬─► LOAD_TB_EVOLUCAO_PACIENTE ─► LOAD_DS_EVOLUCAO_TB_EVOLUCAO_PACIENTE ─► ENRIQUECIMENTO_TB_EVOLUCAO
               ├─► LOAD_TB_PACIENTE_ATEND_MEDIC
               ├─► LOAD_TB_PROTOCOLO ─► LOAD_DS_PROTOCOLO_TB_PROTOCOLO ─► ENRIQUECIMENTO_TB_PROTOCOLO_DS_PROTOCOLO
               └─► EXTRACT_GED_ATENDIMENTO
```

| Task | Notebook | Cluster |
|---|---|---|
| `EXTRACT_VIEW_IOP_BI_PROJ_REC_VIEW` | `NTB10_EXTRACT_VIEW_IOP_BI_PROJ_REC_VIEW` | `clt_oracle_jdbc` (5410-175816-nafxjvpl) |
| `load_oracle` | `NTB02_extract_oracle_load_lake` | `clt_carga_multi_node_oracle` (5416-021719-1kanxpvs) |
| `LOAD_TB_EVOLUCAO_PACIENTE` | `NTB03_LOAD_TB_EVOLUCAO_ORACLE_TASY` | idem |
| `LOAD_DS_EVOLUCAO_TB_EVOLUCAO_PACIENTE` | `NTB06 - DS_EVOLUCAO - PACIENTE_EVOLUCAO - Oracle Federation` | idem |
| `ENRIQUECIMENTO_TB_EVOLUCAO` | `NTB07 - Enriquecimento TB_EVOLUCAO_PACIENTE - Engenharia` | idem |
| `LOAD_TB_PACIENTE_ATEND_MEDIC` | `NTB04_LOAD_TB_PACIENTE_ATEND_MEDIC` | idem |
| `LOAD_TB_PROTOCOLO` | `NTB05_LOAD_TB_PROTOCOLOS` | idem |
| `LOAD_DS_PROTOCOLO_TB_PROTOCOLO` | `NTB08 - DS_PROTOCOLO - PROTOCOLO - Oracle Federation` | idem |
| `ENRIQUECIMENTO_TB_PROTOCOLO_DS_PROTOCOLO` | `NTB09 - Enriquecimento TB_PROTOCOLO - DS_PROTOCOLO` (parametrizado: `tb_entrada=iop.raw.tb_protocolo_ds_protocolo`, `tb_saida=iop.raw.tb_protocolo`) | idem |
| `EXTRACT_GED_ATENDIMENTO` | `NTB14_EXTRACT_GED_ATENDIMENTO` | idem |

Notificação de falha por e-mail configurada apenas na task `load_oracle` (para o Jefferson). As demais tasks não alertam ninguém em caso de falha.

### `load_files_ged` (id `112037879934296`)

- **Schedule:** `0 0 4 * * ?` America/Sao_Paulo — **UNPAUSED**.
- **Owner / run-as:** `jefferson.silva@spesia.com.br`
- **Cluster:** `clt_carga_multi_node_GED` (5416-001239-ibchmqfc), único para as 2 tasks.
- **Notebooks:** `/Workspace/Users/jefferson.silva@spesia.com.br/camada_raw/files_GED/notebooks/`

| Task | Notebook | Descrição |
|---|---|---|
| `create_tb_diferencial_paths` | `NTB01_create_table_pathsfiles_onprimess` | Lista arquivos no SMB, grava `iop.raw.tb_pathfiles_ged_aprocessar` (overwrite) e calcula o diff contra o que já foi processado, gravando `iop.raw.tb_processados_join` |
| `carga_files_ged_raw` | `NTB02_extract_files_GED_load_lake` | Copia (via `mapInPandas` distribuído) só os arquivos com `processed_at IS NULL` para `/Volumes/iop/raw/files/GED/`, registra resultado incrementalmente em `iop.raw.tb_result_carga_files_ged`, roda `OPTIMIZE` no final |

Esta task tem `max_retries: 2` e alerta por e-mail tanto o Jefferson quanto o Lucas em caso de falha (única task com notificação dupla em todo o pipeline).

---

## Clusters

Todos `spark_version 18.1.x-scala2.13`, sem init scripts, sem `spark_conf` além do necessário para single-node. A conectividade com o Oracle **não depende do cluster** — vem da UC Connection (ver seção seguinte), então qualquer cluster clássico na mesma VPC serve.

| Cluster | id | Node type | Workers | `data_security_mode` | Autotermination |
|---|---|---|---|---|---|
| `clt_oracle_jdbc` | `5410-175816-nafxjvpl` | `n2-highmem-4` | 0 (single node) | `SINGLE_USER` | 30 min |
| `clt_carga_multi_node_oracle` | `5416-021719-1kanxpvs` | `e2-highmem-4` | autoscale | `USER_ISOLATION` | 10 min |
| `clt_carga_multi_node_GED` | `5416-001239-ibchmqfc` | `e2-highmem-2` | autoscale | `USER_ISOLATION` | 10 min |

Estado no momento do recon: os três estavam `TERMINATED` (sobem sob demanda quando o job dispara).

---

## Conectividade Oracle (Lakehouse Federation)

3 **UC Connections** do tipo `ORACLE`, todas com owner `jefferson.silva@spesia.com.br`:

| Connection | Host | Uso |
|---|---|---|
| `oracle` | `10.20.1.210:1521` | comment "teste" — não usada pelo foreign catalog ativo |
| `oracle_dtb` | `prod01.subnetprivate.vcn01.oraclevcn.com:1521` | não usada pelo foreign catalog ativo |
| `oracle_prod01` | `10.20.1.210:1521` | **usada pelo foreign catalog `oracle_med4u`** |

O foreign catalog `oracle_med4u` (schema `tasy`) usa a connection **`oracle_prod01`**, com `service_name = prod01.subnetprivate.vcn01.oraclevcn.com`. As demais connections (`oracle`, `oracle_dtb`) existem mas não estão em uso pelo catálogo ativo — possível resíduo de configuração/teste, vale confirmar com o Jefferson se ainda servem a algum propósito antes de removê-las.

Importante: isso é **Lakehouse Federation via UC Connection**, não JDBC configurado no cluster — portável para qualquer cluster clássico na mesma VPC. **Serverless não alcança** o Oracle (host privado) sem um Network Connectivity Config (NCC) configurado — ainda não existe.

Um dos notebooks (`NTB10_EXTRACT_VIEW_IOP_BI_PROJ_REC_VIEW`) foge do padrão de Federation e conecta via **JDBC direto** (`oracle.jdbc.OracleDriver`, `dbutils.secrets.get(scope="oracle_secrets", ...)`), rodando isoladamente no cluster `clt_oracle_jdbc`. Os secrets scopes `oracle_secrets` e `med4u_files_ged` existem no workspace e são usados por esse caminho e pela extração GED.

---

## Catálogo `iop`

Schemas: `default`, `elt`, `gold`, `raw`, `silver` (+ `information_schema`). Todos com owner `jefferson.silva@spesia.com.br`.

A camada `raw` é a única escrita por este pipeline. `silver`/`gold`/`elt` são consumidos ou escritos por pipelines a jusante (ex.: `iop.silver.desfecho`, ver [Consumidores](#consumidores-conhecidos-do-iopraw)).

### Grants no catálogo

Estado **antes** da migração (verificado em 2026-07-21):

```
principal: sp-biomarcadores (service principal, app id ab86adaf-f2b3-4122-9ef8-2b3a917b754e)  → USE_CATALOG
principal: account users                                                                       → ALL_PRIVILEGES
```

**Decisão da Etapa 1 (2026-07-21):** em vez de criar grupos dedicados a este projeto, reaproveitar os grupos de dados que já existem no nível da conta (`spesia-data-ro`/`rw`/`admins`), espelhando exatamente o modelo já usado no catálogo irmão `spesia`. Estratégia de rollout: **grants aditivos primeiro, revogação do grant amplo depois** — nada é retirado de ninguém até termos confiança de que nada depende só do `account users: ALL_PRIVILEGES` (candidato a dependência oculta: o app `desfecho-comissao`, que lê um Volume em `iop.silver` e pode estar se apoiando nesse grant amplo sem estar documentado em lugar nenhum).

**Aplicado em 2026-07-21** (aditivo, via `databricks grants update catalog iop`):

```sql
GRANT SELECT, USE_CATALOG, USE_SCHEMA, EXECUTE, READ_VOLUME ON CATALOG iop TO `spesia-data-ro`;
GRANT SELECT, USE_CATALOG, USE_SCHEMA, EXECUTE, READ_VOLUME,
      CREATE_SCHEMA, CREATE_TABLE, CREATE_FUNCTION, CREATE_VOLUME, MODIFY, WRITE_VOLUME
      ON CATALOG iop TO `spesia-data-rw`;
GRANT ALL PRIVILEGES ON CATALOG iop TO `spesia-data-admins`;
```

Estado atual do catálogo `iop`:

```
spesia-data-ro        → SELECT, USE_CATALOG, USE_SCHEMA, EXECUTE, READ_VOLUME
spesia-data-rw        → tudo do ro + CREATE_SCHEMA/TABLE/FUNCTION/VOLUME, MODIFY, WRITE_VOLUME
spesia-data-admins    → ALL_PRIVILEGES
sp-biomarcadores      → USE_CATALOG (inalterado)
sp-iop-elt            → USE_CATALOG (travessia) + USE_SCHEMA/SELECT/CREATE_TABLE/CREATE_VOLUME/CREATE_FUNCTION/MODIFY/WRITE_VOLUME/READ_VOLUME/EXECUTE só em iop.raw
sp-iop-silver-elt     → USE_CATALOG (travessia) + USE_SCHEMA/SELECT em iop.raw (leitura) + USE_SCHEMA/SELECT/CREATE_TABLE/CREATE_VOLUME/MODIFY em iop.silver (escrita) — SP dedicado ao repo dtb-iop-silver-elt, ver docs/silver-layer-handoff.md
account users         → ALL_PRIVILEGES (⚠️ ainda não revogado — de propósito, ver acima)
```

`spesia-data-admins` (grupo do qual Lucas e Leonardo Pinheiro fazem parte) já era metastore admin antes disso — pode revisar/conceder grants em `iop` livremente, sem depender do Jefferson. O grant explícito de `ALL_PRIVILEGES` ao grupo só deixa isso visível/auditável nos grants do catálogo, igual já era feito em `spesia`.

### Grupos e service principals da conta

Levantado em 2026-07-21 via `databricks account groups list` / `databricks account service-principals list`:

| Principal | Tipo | Membros / uso atual |
|---|---|---|
| `spesia-data-admins` | Grupo (account-level) | Lucas Santiago, Leonardo Pinheiro — metastore admins |
| `spesia-data-rw` | Grupo (account-level) | Ynara Favoretto — leitura+escrita em dados |
| `spesia-data-ro` | Grupo (account-level) | (sem membros no momento) — só leitura |
| `usr_dados_dtb` | Grupo (account-level) | (sem membros no momento) — propósito não confirmado, perguntar ao time antes de assumir para que serve |
| `sp-biomarcadores` | Service principal | usado pelo projeto `biomarcadores-app`/`clinical-doc-extractor` |
| `app-1zinji desfecho-comissao` | Service principal | auto-criado pelo Databricks App `desfecho-comissao`, não é algo a replicar manualmente |
| `sp-iop-silver-elt` | Service principal | **criado em 2026-07-23** — application id (client id) `d9304262-1a5a-498f-96ed-d9099bb9adff`, account id (SCIM) `211855069202202`. Dedicado à camada `iop.silver`, agora rodando no repo próprio [`GRUPOMED4U/dtb-iop-silver-elt`](https://github.com/GRUPOMED4U/dtb-iop-silver-elt), replicando o isolamento do `sp-iop-elt` (ver [`docs/silver-layer-handoff.md`](docs/silver-layer-handoff.md)). Grants aplicados (leitura em `iop.raw`, escrita em `iop.silver`), workspace access `USER` em dev+prod, entitlement `allow-cluster-create` em dev, GitHub Environments `dev`/`prod` e federation policies OIDC (dev+prod) já criados — **ainda não validados rodando de verdade** (a Action `test-connection.yml` desse repo é o primeiro teste real). |

**Decisão da Etapa 1:** este projeto terá seu próprio service principal dedicado para rodar os jobs do Asset Bundle (padrão `sp-biomarcadores`), a ser criado quando chegarmos na etapa de escrever o `databricks.yml` — com grants least-privilege (`USE CONNECTION` na `oracle_prod01` + acesso a `oracle_med4u` + escrita em `iop.raw`). Ainda não criado.

---

## Modelo de dados — o que cada notebook faz

### Carga em lote (`load_oracle` → `NTB02_extract_oracle_load_lake`)

Loop Python com lista **hardcoded** de tabelas Oracle. Para cada tabela: `spark.table("oracle_med4u.tasy.<T>").write.mode("overwrite").saveAsTable("iop.raw.tb_<t>")` — **full overwrite**, sem incremental, sem MERGE.

Tabelas atualmente na lista (28 ativas; 3 comentadas porque têm notebook dedicado no DAG — `EVOLUCAO_PACIENTE`, `PACIENTE_ATEND_MEDIC`, `PROTOCOLO`):

```
ATEND_CHECK_LIST_RESULT, ATENDIMENTO_SINAL_VITAL, AUTORIZACAO_CONVENIO, CAN_LOCO_REGIONAL,
CID_CATEGORIA, CID_DOENCA, COMPL_PESSOA_FISICA, MATERIAL, MED_AVALIACAO_PACIENTE,
MED_TIPO_VICIO, MED_VALOR_DOMINIO, MED_ITEM_AVALIAR, MED_TIPO_AVALIACAO, FUNCAO_PARAMETRO,
PACIENTE_ALERGIA, PACIENTE_ANTEC_CLINICO, PACIENTE_ATENDIMENTO, PACIENTE_HABITO_VICIO,
PACIENTE_SETOR, PESSOA_FISICA, PESSOA_FISICA_PRONT_ESTAB, TISS_AUTOR_ANEXO_DIAG,
nivel_capac_funcional_ecog, SAC_PESQUISA_RESULT, QUA_AVALIACAO_RESULT, ESTRUTURA_MATERIAL_V,
GED_TIPO_ARQUIVO, IOP_ESTABELECIMENTO_RELATORIO
```

`IOP_ESTABELECIMENTO_RELATORIO` foi adicionada em 2026-07-07 com o comentário "discovery biomarcadores — filtro de estabelecimento", ligando esse pipeline ao projeto `biomarcadores-app`/`clinical-doc-extractor`.

### `EVOLUCAO_PACIENTE` — 3 notebooks encadeados

1. **`NTB03_LOAD_TB_EVOLUCAO_ORACLE_TASY`** — full-load de `EVOLUCAO_PACIENTE` **exceto `DS_EVOLUCAO`** (CLOB/`LONG` grande, removido por performance) → `iop.raw.tb_evolucao_paciente_01` (tabela temporária).
2. **`NTB06 - DS_EVOLUCAO...`** — extrai só a coluna `DS_EVOLUCAO` (tipo Oracle `LONG`, ~2.7 KB/registro, ~825K registros / ~2.2 GB), em **blocos de 200 IDs com 20 threads paralelos**, filtrando por `WHERE date(DT_ATUALIZACAO) >= <watermark>` (**única parte com lógica incremental por watermark** hoje) → `append` em `iop.raw.evolucao_paciente_ds_evolucao`.
3. **`NTB07 - Enriquecimento TB_EVOLUCAO_PACIENTE`** — limpa o RTF de `DS_EVOLUCAO` (biblioteca `striprtf`, via Pandas UDF vetorizada) e faz join com a tabela principal por `CD_EVOLUCAO`, publicando `iop.raw.tb_evolucao_paciente` (`overwrite` + `mergeSchema`). Também detecta e registra **deletes** (`leftanti` join contra a versão anterior) em `iop.raw.tb_evolucao_paciente_deletes` — é o único ponto do pipeline que rastreia deleções. Termina com asserts de contagem de linhas.

### `PROTOCOLO` — 3 notebooks encadeados (mesmo padrão de `EVOLUCAO_PACIENTE`)

1. **`NTB05_LOAD_TB_PROTOCOLOS`** — full-load de `PROTOCOLO` exceto `DS_PROTOCOLO` → `iop.raw.tb_protocolo`.
2. **`NTB08 - DS_PROTOCOLO...`** — extrai `DS_PROTOCOLO` (`LONG`, ~0.8 KB/registro, ~135K registros / ~100MB) em **blocos de 999 IDs / 20 threads**, mas aqui a incrementalidade é por **anti-join contra o destino** (`WHERE CD_PROTOCOLO NOT IN (SELECT ... FROM destino)`), não por data → `append` em `iop.raw.tb_protocolo_ds_protocolo`.
3. **`NTB09 - Enriquecimento TB_PROTOCOLO`** — mesmo padrão de limpeza RTF + join, publica em `iop.raw.tb_protocolo` (parametrizado via `dbutils.widgets`, então tecnicamente reutilizável para outras tabelas LONG/RTF — mas hoje só é chamado para protocolo).

### Notebooks avulsos

| Notebook | Tabela destino | Modo de escrita | Observação |
|---|---|---|---|
| `NTB04_LOAD_TB_PACIENTE_ATEND_MEDIC` | `iop.raw.tb_paciente_atend_medic` | `overwrite` | 16 linhas, sem transformação |
| `NTB14_EXTRACT_GED_ATENDIMENTO` | `iop.raw.tb_ged_atendimento` | `CREATE OR REPLACE TABLE ... AS SELECT * EXCEPT(IM_ARQUIVO_BANCO)` | **Falha é engolida silenciosamente** (`try/except` genérico que só faz `print`, sem lançar erro nem alertar) — se a extração falhar, o job continua "verde" |
| `NTB10_EXTRACT_VIEW_IOP_BI_PROJ_REC_VIEW` | `iop.raw.tb_iop_bi_projecao_receita_view_chama` | **`append`** (não overwrite) | Único notebook via JDBC direto (não Federation); agrega receita por data/`TIPOUM` a partir da view `iop_bi_projecao_receita_view_chama`; tabela cresce a cada execução — não há dedupe nem controle de idempotência visível |

### GED

1. **`NTB01_create_table_pathsfiles_onprimess`** — conecta via SMB (`smbprotocol`), lista arquivos do share, grava `iop.raw.tb_pathfiles_ged_aprocessar` (overwrite) e cruza (`chave_filename = sha2(filename, 256)`) contra `iop.raw.tb_result_carga_files_ged` para montar `iop.raw.tb_processados_join`, marcando o que já foi processado.
2. **`NTB02_extract_files_GED_load_lake`** — lê `tb_processados_join WHERE processed_at IS NULL`, copia os bytes via SMB em paralelo (`mapInPandas`, batches de 5.000, ~40 partições) para `/Volumes/iop/raw/files/GED/`, grava resultado incremental (sucesso/erro por arquivo) em `iop.raw.tb_result_carga_files_ged`, e roda `OPTIMIZE` no final.

#### Porte do GED pra `.py` versionável (2026-07-24)

Os dois notebooks do job `load_files_ged` já foram portados como código testável (ainda não commitado, em revisão):

- **`NTB01` → `create_tb_diferencial_paths`**: lógica em [`src/dtb_iop_raw_elt/ged/diff_pathfiles.py`](src/dtb_iop_raw_elt/ged/diff_pathfiles.py), entrypoint em [`databricks/tasks/create_tb_diferencial_paths.py`](databricks/tasks/create_tb_diferencial_paths.py), testes em [`tests/ged/test_diff_pathfiles.py`](tests/ged/test_diff_pathfiles.py). Escreve nos nomes reais de produção (`tb_pathfiles_ged_aprocessar`, `tb_processados_join`).
- **`NTB02` → `carga_files_ged_raw`**: lógica em [`src/dtb_iop_raw_elt/ged/carga_files.py`](src/dtb_iop_raw_elt/ged/carga_files.py) — tabela/volume de destino são parâmetros de `run()`, não hardcoded, justamente para permitir apontar pra destinos de teste sem duplicar a lógica.
  - Entrypoint de produção: [`databricks/tasks/carga_files_ged_raw.py`](databricks/tasks/carga_files_ged_raw.py) (destinos reais, default).
  - Entrypoint de teste: [`databricks/tasks/carga_files_ged_raw_test.py`](databricks/tasks/carga_files_ged_raw_test.py) — mesma lógica, lê o backlog real de `tb_processados_join` (só leitura) e copia os arquivos reais do GED via SMB, mas grava em `iop.raw.tb_result_carga_files_ged_test` e `/Volumes/iop/raw/files/GED_test/` — não toca nos destinos reais usados pelo job legado.
  - Testes em [`tests/ged/test_carga_files.py`](tests/ged/test_carga_files.py) (só a lógica pura — `chunk_ids`/`copy_one_file` — não cobre `mapInPandas`/SMB de verdade, mesmo critério já usado no `NTB01`).

**Job manual criado pra validar isso**: [`resources/load_files_ged_job.yml`](resources/load_files_ged_job.yml) declara `load_files_ged_test`, com uma única task (`carga_files_ged_raw_test`), **sem schedule** (só dispara manualmente), `job_cluster` (não `existing_cluster_id` — primeiro passo rumo à modernização M1), `max_retries: 2`. Alerta por e-mail em falha só pra `lucas.santiago@spesia.com.br` — **decisão explícita de tirar o Jefferson** desse alerta (job novo, não o legado; o job legado `load_files_ged` continua alertando os dois, inalterado).

**Gatilho manual via GitHub Actions**: [`.github/workflows/run-ged-load-test.yml`](.github/workflows/run-ged-load-test.yml) — mesmo padrão do `run-oracle-lakeflow-test.yml` (só `workflow_dispatch`, nunca em push), autentica como `sp-iop-elt` via OIDC contra o workspace dev, `databricks bundle deploy --target dev` + `databricks bundle run load_files_ged_test --target dev`. `databricks bundle validate --target dev` passou localmente; **ainda não disparamos essa Action** — fica pra quando alguém clicar em "Run workflow".

---

## Notebooks auxiliares / não agendados

Existem notebooks nas mesmas pastas que **não fazem parte do DAG de nenhum job** — são exploratórios, de setup pontual, ou substituídos:

- `NTB01_cnn_oracle_federation` — script de criação da connection/foreign catalog (`CREATE CONNECTION`/`CREATE FOREIGN CATALOG`), usado uma vez para provisionar o `oracle_med4u`.
- `NTB_FIX01_ajuste_catalogo_oracleme4u` — recria o foreign catalog (drop + create), lista de tabelas para "aquecer"/validar o catálogo.
- `NTB11_VIEW_VALOR_ORACLE`, `NTB12_CNN_ORACLE_IOP_JDBC`, `NTB13_AVALIACAO_METADADOS_TB_ORACLE_JDBC` — exploração de funções PL/SQL do Tasy e metadados via JDBC direto; `NTB13` faz `%run` do `NTB12`.
- `oracle/notebooks/old/` — pasta com versões antigas, não inspecionada em detalhe.
- `oracle/notebooks/New Notebook 2026-06-21 23:07:14` — notebook sem nome definitivo.
- `files_GED/notebooks/NTB03_diffs_pathsfiles_onprimess` — parece uma versão anterior/experimental de `NTB01`.
- `files_GED/notebooks/pysmb/` — pasta auxiliar, não inspecionada em detalhe.
- `files_GED/notebooks/New Notebook 2026-05-12 22:43:56` — idem.

Nenhum desses entra no escopo da migração por ora — são ruído histórico da pasta pessoal do Jefferson. Vale reavaliar se algo ali é útil antes de migrar, mas não são necessários para o pipeline de produção rodar.

---

## Consumidores conhecidos do `iop.raw`

Levantado via `databricks jobs list` + `databricks apps list` (só existem 5 jobs e 1 app no workspace):

1. **`clinical-doc-extractor`** (job `858118383428704`, `[dev lucas_santiago]`) — consumidor declarado, extrai/processa documentos clínicos a partir de `iop.raw` (e possivelmente outras camadas do `iop`).
2. **App `desfecho-comissao`** (`david.cavallari@spesia.com.br`) — **consumidor indireto**. O backend (`server/main.py`) lê JSON estático de `/Volumes/iop/silver/desfecho/dados_clinicos/` (`patients.json`, `evolucoes.json`, `obituario.json`, `protocol_classifications.json`, `historico.jsonl`) — confirmado que esses arquivos existem e estão populados nesse Volume. Este Volume está em `iop.silver`, não `iop.raw` diretamente; é razoável supor que é alimentado pelo `clinical-doc-extractor`, mas isso **não foi confirmado neste recon** — vale checar antes do cutover.
3. **`start-desfecho-app` / `stop-desfecho-app`** (jobs `367070080212185` / `1016242371021848`, owner `david.cavallari@spesia.com.br`) — apenas ligam/desligam o app `desfecho-comissao` em horário comercial (9h/17h, seg-sex). Não leem `iop.raw` diretamente.

Não foram encontrados outros jobs, pipelines DLT/Lakeflow, ou dashboards SQL Warehouse referenciando `iop.raw` além destes. **Isso não é garantia de completude** — não foram varridas queries de SQL Warehouse/Genie nem notebooks fora dos workspaces de usuário já conhecidos.

### Projeto relacionado, mas **não sobreposto**: `iop-extract`

Existe um Git Folder em `/Repos/spesia_product/iop-extract` (`github.com/GRUPOMED4U/iop-extract`) que **não é este pipeline nem depende dele**. É um projeto Python (Poetry) separado, deployado via Docker/GKE (CronJob no Kubernetes), que:

- Conecta **direto no Oracle** via `python-oracledb`/Instant Client (rede privada GCP↔OCI), **sem passar pelo Databricks nem pelo `iop.raw`**.
- Faz extração + NLP/regex + unificação por paciente.
- Publica o resultado em **MongoDB** (via Beanie/Pydantic), para alimentar pipelines de ML — não em Unity Catalog.

Ou seja: hoje existem **dois caminhos de extração do mesmo Oracle**, independentes um do outro — este repositório (Databricks/`iop.raw`) e o `iop-extract` (GKE/MongoDB). Vale ter isso em mente ao avaliar carga no Oracle de origem, mas não há necessidade de unificá-los como parte desta migração.

---

## Ownership atual e bus factor

Hoje, três coisas críticas do pipeline pertencem a uma pessoa física (`jefferson.silva@spesia.com.br`), não a um grupo ou service principal:

- As 3 **UC Connections** Oracle (`oracle`, `oracle_dtb`, `oracle_prod01`).
- Todos os **notebooks** (pasta pessoal `/Workspace/Users/jefferson.silva@spesia.com.br/...`).
- Os 2 **Jobs** (`created_by` / `run_as_user_name` = Jefferson).

Reatribuir isso é **mecânico**, não bloqueante — quem tiver privilégio de metastore admin (`spesia-data-admins`, que inclui Lucas) pode trocar owner de connections/catalogs livremente, e jobs podem ser transferidos de owner via API/CLI. Não depende do Jefferson agir primeiro; é cortesia avisar.

---

## Achados de governança

- **`account users` tem `ALL_PRIVILEGES` no catálogo `iop` inteiro.** Isso é bem mais permissivo do que deveria — qualquer usuário da conta Databricks pode ler/escrever/conceder grants em qualquer schema/tabela de `iop`, incluindo dados clínicos sensíveis. **Em andamento (2026-07-21):** os grants corretos (`spesia-data-ro`/`rw`/`admins`, espelhando o catálogo `spesia`) já foram concedidos de forma aditiva, sem revogar nada ainda. O `account users: ALL_PRIVILEGES` continua ativo de propósito — será revogado só depois de um período observando quem efetivamente depende dele (candidato suspeito: o app `desfecho-comissao`, que lê um Volume em `iop.silver` sob uma identidade própria de service principal e pode estar contando com esse grant amplo).

---

## Estado da migração

| Componente | Estado |
|---|---|
| Repositório Git + `README.md` + `CLAUDE.md` | ✅ feito |
| Decisões da Etapa 1 (M4 fica pra depois, grupos reaproveitados, SP dedicado, grants aditivos) | ✅ feito (2026-07-21) |
| Grants aditivos `spesia-data-ro/rw/admins` no `iop` | ✅ feito (2026-07-21) — `account users: ALL_PRIVILEGES` ainda ativo de propósito |
| Git Folder no workspace (`/Repos/spesia_product/dtb-iop-raw-elt`) | ✅ feito (2026-07-22) |
| Service principal dedicado (`sp-iop-elt`) | ✅ criado (2026-07-22) — grants escopados só a `iop.raw` (corrigido depois de ter sido criado catalog-wide via `spesia-data-rw`; acesso ao workspace agora é direto, não por grupo) |
| Autenticação CI via Workload Identity Federation (OIDC, sem secret) | ✅ **validado de ponta a ponta** (2026-07-22) — policy de dev confirmada rodando na Action; policy de prod criada mas ainda não testada |
| GitHub Environments `dev`/`prod` | ✅ feito (2026-07-22) — `prod` sem revisor obrigatório ainda (ver Riscos) |
| Databricks Asset Bundle (`databricks.yml`) | 🟡 2 targets + `artifacts` (wheel) + 2 resources (`oracle_pipeline_test` spike, `load_files_ged_test` job manual) — `bundle validate` passa, nenhum job de produção ainda |
| GitHub Action de teste de conexão (`test-connection.yml`) | ✅ **rodou com sucesso em produção** (2026-07-22) — autenticou como `sp-iop-elt`, validou o bundle, não deploya nada |
| Spike Lakeflow Declarative Pipelines (Oracle) | ✅ **validado em produção** (2026-07-23, 1 tabela) — expandido pra ~28 tabelas do loop genérico + 11 do inventário Barramento Selena em 2026-07-24 (39 no total), **ainda não rodado** com o conjunto maior, ver [seção dedicada](#spike-lakeflow-declarative-pipelines-pro-lado-oracle) |
| GitHub Action de deploy automático (dev on push / prod manual) | ❌ não iniciado — só faz sentido quando houver um job completo pra deployar |
| Portar notebooks para `.py` versionável | 🟡 2 de ~14 portados localmente (`create_tb_diferencial_paths`/`NTB01` e `carga_files_ged_raw`/`NTB02`, job `load_files_ged` — ver [seção do GED](#ged)) — **ainda não commitado**, em revisão |
| Job clusters no lugar de clusters interativos | 🟡 usado no job de teste `load_files_ged_test` (`job_clusters`, não `existing_cluster_id`) — ainda não aplicado aos jobs reais em produção |
| CI (lint/testes) | 🟡 estrutura de testes criada (`pytest`), ainda não rodada em CI |
| Carga incremental (watermark/MERGE) | ❌ não iniciado — hoje só `EVOLUCAO_PACIENTE`/`PROTOCOLO` têm alguma incrementalidade parcial (extração da coluna LONG), o full-load principal continua overwrite total |
| Ownership de jobs/notebooks/connections legados | ❌ não iniciado — tudo ainda em nome do Jefferson |
| Revogar `account users: ALL_PRIVILEGES` no `iop` | ❌ não feito — de propósito, aguardando confiança de que nada depende só dele |
| Cutover em produção | ❌ não iniciado — **os 2 jobs legados continuam rodando exatamente como estão** |

A lógica de produção descrita nas seções acima (jobs `extracao_tabelas_oracle` e `load_files_ged`) ainda roda **exclusivamente** como notebooks na pasta pessoal do Jefferson, sem Git — o trabalho de porte está começando, notebook por notebook.

---

## Como rodar / deployar (planejado)

O modelo GitOps deste projeto tem duas partes, replicando o padrão já usado nos projetos irmãos `iop-extract` e `natural-language-extractors`/`clinical-doc-extractor`:

### 1. Git Folder no workspace (feito em 2026-07-22)

Um **Git Folder** (Databricks Repos) em `/Repos/spesia_product/dtb-iop-raw-elt`, apontando para este repositório (branch `main`), criado via:

```bash
databricks repos create "https://github.com/GRUPOMED4U/dtb-iop-raw-elt.git" gitHub \
  --path "/Repos/spesia_product/dtb-iop-raw-elt"
```

Isso dá uma cópia navegável/editável do código dentro do Databricks, sincronizada com o GitHub — o mesmo mecanismo já usado por `iop-extract` e `natural-language-extractors` (pasta compartilhada `/Repos/spesia_product/`, não pessoal). **Importante:** essa sincronização não é automática a cada push — é preciso rodar `databricks repos update` (ou usar a UI) para puxar commits novos para o Git Folder.

### 2. Databricks Asset Bundle (esqueleto de 2026-07-22 + primeiro job manual em 2026-07-24)

O deploy real dos jobs/clusters/schedules é feito via Asset Bundle (`databricks.yml`), replicando o padrão do `clinical-doc-extractor` (que foi deployado assim, não a partir do Git Folder). Além dos 2 targets, o bundle já declara um `artifacts` (build do wheel do pacote `dtb_iop_raw_elt` via `pip wheel . --no-deps -w dist`), o `resources/oracle_pipeline_test.pipeline.yml` (spike Lakeflow) e o `resources/load_files_ged_job.yml` (job manual `load_files_ged_test`, ver [seção do GED](#ged)) — ainda nenhum job real de produção:

```yaml
targets:
  dev:   # https://8259557250383794.4.gcp.databricks.com
  prod:  # https://8259561315104258.8.gcp.databricks.com
```

```bash
databricks bundle validate --target dev
databricks bundle deploy --target dev
databricks bundle deploy --target prod   # só com autorização explícita, após validação em paralelo
```

### 3. Identidade de execução: `sp-iop-elt` (criado em 2026-07-22)

Service principal dedicado para rodar deploys/jobs deste projeto — nunca usuário pessoal:

- **Nome:** `sp-iop-elt` — **application id (client id):** `83de5c11-53ff-4142-bee7-7fc04713f273` — **account id (SCIM):** `217897259508188`.
- **Grants no `iop` — escopados ao schema `raw`, não ao catálogo inteiro** (corrigido em 2026-07-22, ver [Riscos](#riscos-conhecidos) do porquê isso mudou): `USE_CATALOG` no catálogo `iop` (privilégio de travessia, não dá acesso a dado nenhum sozinho) + `USE_SCHEMA`, `SELECT`, `CREATE_TABLE`, `CREATE_VOLUME`, `CREATE_FUNCTION`, `MODIFY`, `WRITE_VOLUME`, `READ_VOLUME`, `EXECUTE` **só no schema `iop.raw`**. **Não** é membro de `spesia-data-rw` (que dá acesso a `iop` inteiro, incluindo `silver`/`gold`) — isso é proposital: um SP dedicado à camada raw não deveria conseguir escrever em silver/gold.
- **Acesso ao workspace:** atribuído **diretamente** ao SP (`USER`) em dev e prod — não é mais herdado via grupo, já que ele saiu do `spesia-data-rw`.
- **Padrão para futuras camadas (silver/gold):** se um dia existir um projeto/repo dedicado para `iop.silver` ou `iop.gold`, o padrão é replicar esta receita inteira — **repo novo + service principal novo + federation policies novas apontando pro repo novo** — não reaproveitar o `sp-iop-elt` nem suas policies. Isso mantém o blast radius de cada camada isolado: comprometer o CI de um projeto não dá acesso a outra camada.

### 4. Autenticação do CI: Workload Identity Federation (OIDC), sem secret nenhum

O GitHub Actions autentica como `sp-iop-elt` via **OIDC federation** — nenhum token/senha fica guardado no GitHub. Duas federation policies foram criadas no `sp-iop-elt` (uma por ambiente) e **validadas de ponta a ponta em produção** (`databricks current-user me` rodando na Action retornou `sp-iop-elt` corretamente, 2026-07-22).

```bash
databricks account service-principal-federation-policy create 217897259508188 --json '{
  "oidc_policy": {
    "issuer": "https://token.actions.githubusercontent.com",
    "audiences": ["https://<workspace-host>/oidc/v1/token"],
    "subject": "repo:<org>@<org_id>/<repo>@<repo_id>:environment:<dev|prod>"
  }
}'
```

**⚠️ Duas armadilhas descobertas só ao testar de verdade** (a doc genérica da Databricks não deixa isso claro):

1. **`audience`** não é o account id — é a **URL do endpoint OIDC do workspace específico**: `https://<workspace-host>/oidc/v1/token`. Cada policy (dev/prod) tem um audience diferente, porque cada uma autentica contra um workspace diferente.
2. **`subject`** não é só `repo:org/repo:environment:nome` — o GitHub inclui os **IDs imutáveis** de org e repo: `repo:GRUPOMED4U@197661296/dtb-iop-raw-elt@1308299119:environment:dev`. Isso protege contra hijacking se alguém renomear a org/repo, mas quebra silenciosamente qualquer policy configurada só com os nomes.

Os valores corretos e já validados (dev):
```json
{
  "issuer": "https://token.actions.githubusercontent.com",
  "audiences": ["https://8259557250383794.4.gcp.databricks.com/oidc/v1/token"],
  "subject": "repo:GRUPOMED4U@197661296/dtb-iop-raw-elt@1308299119:environment:dev"
}
```
(a policy de `prod` é idêntica, trocando o audience pelo host de prod e `:environment:prod` — **ainda não validada de verdade**, só a de dev rodou com sucesso até agora).

O `subject` amarra a autenticação a um **GitHub Environment** específico (`dev` ou `prod`) — só um workflow rodando sob aquele ambiente consegue o token OIDC com o claim certo para autenticar.

### 5. GitHub Environments (`dev` / `prod`)

Criados no repositório (`dev`: sem restrição; `prod`: restrito à branch `main` via `deployment_branch_policy`). **Ainda sem revisor obrigatório configurado** — decisão consciente por ora, para não precisar montar times/permissões no GitHub agora (ver [Riscos conhecidos](#riscos-conhecidos)). O gate de "prod só com autorização explícita" hoje é: o workflow de prod só roda via `workflow_dispatch` (disparo manual), nunca automaticamente por push.

### 6. GitHub Action `test-connection.yml`

Primeira Action do projeto (`.github/workflows/test-connection.yml`): roda em todo push para `main` e também sob demanda (`workflow_dispatch`). Autentica como `sp-iop-elt` contra o workspace **dev** via OIDC e roda `databricks current-user me` + `databricks bundle validate --target dev` — só valida que a cadeia de autenticação/config funciona, não faz deploy nem toca em nenhum dado.

---

## Spike: Lakeflow Declarative Pipelines pro lado Oracle

Depois de terminar a base de GitOps (Git Folder, Asset Bundle, SP, OIDC), avaliamos se o **Lakeflow Declarative Pipelines** (o framework declarativo nativo do Databricks pra pipelines de dados — antigo Delta Live Tables) seria uma base melhor que "script Python + Job" pra este projeto. Ver M4 no catálogo de melhorias original.

### Conclusão da avaliação (2026-07-22)

Não é tudo-ou-nada — depende de qual parte do pipeline:

| Parte do pipeline | Lakeflow encaixa? |
|---|---|
| Loop das ~28 tabelas Oracle (`NTB02`) e as tabelas simples (`PACIENTE_ATEND_MEDIC`, `GED_ATENDIMENTO`, view de receita, enriquecimento RTF) | ✅ bom encaixe — cada uma vira uma declaração de tabela |
| Extração da coluna `LONG` (`DS_EVOLUCAO`/`DS_PROTOCOLO`, hoje um loop de threads em blocos de ID) | 🟡 incerto — pode ser que o hack de blocos nem seja mais necessário com um read declarativo simples, mas não testamos ainda |
| Cópia de arquivos do GED (SMB → Volume) | ❌ não encaixa — é efeito colateral puro (conectar/copiar bytes), não uma transformação de tabela. Lakeflow é feito pra "declarar uma tabela a partir de uma query", não pra isso. **GED continua como Job comum** (é o que já portamos em `create_tb_diferencial_paths`). |

Achados técnicos (verificados via doc oficial, não só teoria):
- Lakeflow **roda em compute clássico** (não só serverless) — resolve a exigência de rede privada pro Oracle.
- **Lê de Lakehouse Federation** (nosso `oracle_med4u`) como fonte, mas exige `channel: PREVIEW` na pipeline.
- É um `resources.pipelines.<nome>` no Asset Bundle — mesmo mecanismo de deploy que já temos, não precisa de infraestrutura nova.

### Spike criado e validado em produção (2026-07-23)

Pipeline mínima, só pra provar viabilidade antes de decidir migrar o resto do lado Oracle:

- `resources/oracle_pipeline_test.pipeline.yml` — pipeline `oracle_pipeline_test`, catálogo/schema `iop.raw`, **`continuous: false`** (roda uma vez, não fica ligada), **compute clássico** (`serverless: false` + `clusters`, não serverless), `channel: PREVIEW`.
- `pipelines/oracle_test/transformations/paciente_atend_medic_test.py` — declara `_test_lakeflow_paciente_atend_medic` lendo `oracle_med4u.tasy.PACIENTE_ATEND_MEDIC` (a tabela mais simples e pequena do pipeline legado) e escrevendo em `iop.raw._test_lakeflow_paciente_atend_medic` (nome propositalmente marcado como teste — não encosta na tabela real `iop.raw.tb_paciente_atend_medic`).
- `pipelines/oracle_test/transformations/raw_tables_loop_test.py` — **adicionado em 2026-07-24**, depois da run de sucesso. Replica o mesmo padrão (`spark.read.table("oracle_med4u.tasy.<TABELA>")` → `_test_lakeflow_<tabela>`) para as **~28 tabelas do loop genérico de full-load** (`NTB02_extract_oracle_load_lake`), geradas dinamicamente num loop Python (mesmo espírito do notebook legado, que também é uma lista + loop). **Não inclui**: `EVOLUCAO_PACIENTE`/`PROTOCOLO` (extração de coluna `LONG` via threads + limpeza RTF — ainda incerto se encaixa em Lakeflow, ver avaliação acima) nem `GED_ATENDIMENTO`/a view de receita (JDBC direto, não passam pela Lakehouse Federation `oracle_med4u` — precisariam de outra fonte na pipeline). Ainda escreve só em tabelas `_test_lakeflow_*`, nenhuma tabela real de produção é tocada.
  - **Mesmo arquivo, expandido em 2026-07-24**: mais 11 tabelas (`BARRAMENTO_SELENA_TABLES`) levantadas a partir de `docs/Barramento_Selena_Inventario_Frontend.docx` — inventário das chamadas do frontend web (`apps/web`) à integração "Barramento Selena" (Tasy), feito por um time de app separado (`api-gateway` → RabbitMQ → microservice `integrations`, acesso OLTP em tempo real ao mesmo Oracle/Tasy — **não é o mesmo caminho desta ELT**, mas lê/escreve nas mesmas tabelas físicas). Tabelas que o doc mostra sendo usadas de verdade por fluxos ativos do frontend (checkout, protocolos, loco-regional, detalhes do paciente) e que ainda não estavam em `iop.raw`: `ATENDIMENTO_PACIENTE`, `PACIENTE_ATEND_PROC`, `ATEND_CATEGORIA_CONVENIO`, `CAN_TNM_CLASSIF`, `MED_RECEITA`, `ATESTADO_PACIENTE`, `PROTOCOLO_MEDIC_MATERIAL`, `PROTOCOLO_MEDICACAO`, `PROTOCOLO_MEDIC_PROC`, `TIPO_ALERGIA`, `PACIENTE_MEDIC_USO`. Ficaram de fora (só em código morto/endpoints sem caller no frontend, por ora menor prioridade): `MEDICO`, `USUARIO`, `PROCEDIMENTO`, `ATEND_PACIENTE_UNIDADE`, `CIDO_TOPOGRAFIA`.
- `.github/workflows/run-oracle-lakeflow-test.yml` — **só dispara manualmente** (`workflow_dispatch`, botão "Run workflow" no GitHub) — nunca em push, porque isso faz uma query de verdade contra o Oracle de **produção** e deve rodar fora do horário comercial.

Grants concedidos ao `sp-iop-elt` pra viabilizar isso (2026-07-22, connection/catálogo que antes não tinham grant nenhum além do owner) — cobrem qualquer tabela do schema `oracle_med4u.tasy`, então as 28 novas não precisaram de nenhum grant adicional:
```
oracle_prod01 (connection)     -> USE_CONNECTION
oracle_med4u (foreign catalog) -> USE_CATALOG
oracle_med4u.tasy (schema)     -> USE_SCHEMA, SELECT
```

**Rodou de verdade em produção em 2026-07-23** (`run-oracle-lakeflow-test.yml`, run [`29969421348`](https://github.com/GRUPOMED4U/dtb-iop-raw-elt/actions/runs/29969421348)) — `success`, depois de uma primeira tentativa que falhou (`29969174564`). A expansão pras 28 tabelas (2026-07-24) **ainda não rodou** — só validamos até aqui com a tabela única.

---

## Riscos conhecidos

- **Rede:** qualquer parte migrada para serverless precisa de Network Connectivity Config (NCC) para alcançar o Oracle (host privado) — hoje não existe. Sem isso, a conexão falha ou trava silenciosamente.
- **Outros consumidores do `iop.raw`:** confirmar antes do cutover que não há mais nada além do listado em [Consumidores](#consumidores-conhecidos-do-iopraw) — em particular, confirmar a ligação real entre `clinical-doc-extractor` e o Volume `iop.silver.desfecho`.
- **Full-overwrite → incremental:** cuidado com late-arriving data e updates/deletes no Oracle. `EVOLUCAO_PACIENTE` já tem rastreamento parcial de deletes (`tb_evolucao_paciente_deletes`); as demais 28 tabelas do loop genérico não têm nenhum.
- **Falhas silenciosas:** `NTB14_EXTRACT_GED_ATENDIMENTO` engole exceções com `try/except: print(...)` — uma falha na extração de `GED_ATENDIMENTO` não derruba o job nem alerta ninguém.
- **`tb_iop_bi_projecao_receita_view_chama`:** cresce indefinidamente via `append` sem dedupe — reprocessamento/backfill duplicaria dados.
- **Governança do `iop`:** o grant amplo de `account users` pode estar mascarando dependências (alguém pode estar lendo `iop.raw` sem que isso apareça em nenhum job/app conhecido) — revisar com cautela antes de restringir.
- **Notebooks auxiliares não inspecionados em profundidade** (`old/`, `pysmb/`, "New Notebook..."): podem conter lógica ainda relevante ou lixo histórico — não assumir nenhuma das duas coisas sem checar.
- **Grants por grupo catalog-wide vs. por schema:** ao dar acesso a um SP/pipeline novo, cuidado para não simplesmente adicioná-lo a `spesia-data-rw`/`ro` (que dão acesso ao catálogo `iop` inteiro) quando o pipeline só deveria mexer em um schema específico — foi exatamente esse erro que corrigimos no `sp-iop-elt` (ver acima). Preferir grants explícitos por schema para qualquer identidade nova.
- **Ambiente `prod` do GitHub sem revisor obrigatório:** hoje o único gate antes de um deploy em produção é o workflow ser `workflow_dispatch` (manual). Não há um segundo par de olhos forçado via GitHub Environment protection rule — considerar adicionar revisores nomeados (usuário ou time) quando a governança de times no GitHub estiver mais estruturada.

---

## Referências

- `docs/08-MELHORIAS-ELT-IOP.md` no repositório `biomarcadores-app` (branch `BRI-11-migration-databricks`) — análise original que motivou esta migração.
- `databricks.yml` do projeto `clinical-doc-extractor` — padrão de Asset Bundle GitOps a seguir.
- [`docs/lakeflow-declarative-pipelines.md`](docs/lakeflow-declarative-pipelines.md) — receita técnica completa de como validamos Lakeflow Declarative Pipelines + GitOps via OIDC (service principal, grants, federation policy, `databricks.yml`).
- [`docs/silver-layer-handoff.md`](docs/silver-layer-handoff.md) — contexto e checklist de handoff pro Leonardo, pra construir a camada `iop.silver` como projeto separado.
