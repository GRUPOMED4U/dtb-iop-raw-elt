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

### Grants no catálogo (verificado em 2026-07-21)

```
principal: <group-id> ab86adaf-f2b3-4122-9ef8-2b3a917b754e   → USE_CATALOG
principal: account users                                     → ALL_PRIVILEGES
```

Ver [Achados de governança](#achados-de-governança) — o grant de `account users` é bem mais permissivo do que deveria.

`spesia-data-admins` (grupo do qual Lucas e Leonardo Pinheiro fazem parte) é metastore admin — pode revisar/conceder grants em `iop` livremente, sem depender do Jefferson.

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

---

## Ownership atual e bus factor

Hoje, três coisas críticas do pipeline pertencem a uma pessoa física (`jefferson.silva@spesia.com.br`), não a um grupo ou service principal:

- As 3 **UC Connections** Oracle (`oracle`, `oracle_dtb`, `oracle_prod01`).
- Todos os **notebooks** (pasta pessoal `/Workspace/Users/jefferson.silva@spesia.com.br/...`).
- Os 2 **Jobs** (`created_by` / `run_as_user_name` = Jefferson).

Reatribuir isso é **mecânico**, não bloqueante — quem tiver privilégio de metastore admin (`spesia-data-admins`, que inclui Lucas) pode trocar owner de connections/catalogs livremente, e jobs podem ser transferidos de owner via API/CLI. Não depende do Jefferson agir primeiro; é cortesia avisar.

---

## Achados de governança

- **`account users` tem `ALL_PRIVILEGES` no catálogo `iop` inteiro.** Isso é bem mais permissivo do que deveria — qualquer usuário da conta Databricks pode ler/escrever/conceder grants em qualquer schema/tabela de `iop`, incluindo dados clínicos sensíveis. Não é bloqueante para a migração, mas é dívida técnica de segurança que vale revisar (possivelmente depois do cutover, para não mascarar dependências que dependam desse grant amplo).

---

## Estado da migração

| Componente | Estado |
|---|---|
| Repositório Git + `README.md` + `CLAUDE.md` | ✅ feito (este commit) |
| Portar notebooks para `.py` versionável | ❌ não iniciado |
| Databricks Asset Bundle (`databricks.yml`) | ❌ não iniciado |
| Job clusters no lugar de clusters interativos | ❌ não iniciado |
| CI (lint/testes) | ❌ não iniciado |
| Carga incremental (watermark/MERGE) | ❌ não iniciado — hoje só `EVOLUCAO_PACIENTE`/`PROTOCOLO` têm alguma incrementalidade parcial (extração da coluna LONG), o full-load principal continua overwrite total |
| Ownership em grupo/service principal | ❌ não iniciado — tudo ainda em nome do Jefferson |
| Cutover em produção | ❌ não iniciado — **os 2 jobs legados continuam rodando exatamente como estão** |

**Nada foi portado ainda.** Toda a lógica de produção descrita neste README ainda roda exclusivamente como notebooks na pasta pessoal do Jefferson, sem Git.

---

## Como rodar / deployar (planejado)

Ainda não implementado. O plano é replicar o padrão do projeto `clinical-doc-extractor`:

```bash
databricks bundle validate --target dev
databricks bundle deploy --target dev
databricks bundle deploy --target prod   # só com autorização explícita, após validação em paralelo
```

Detalhes de configuração (`databricks.yml`, targets, service principal de execução) serão adicionados aqui conforme forem implementados — **esta seção deve ser atualizada no mesmo commit/PR que introduzir o Asset Bundle.**

---

## Riscos conhecidos

- **Rede:** qualquer parte migrada para serverless precisa de Network Connectivity Config (NCC) para alcançar o Oracle (host privado) — hoje não existe. Sem isso, a conexão falha ou trava silenciosamente.
- **Outros consumidores do `iop.raw`:** confirmar antes do cutover que não há mais nada além do listado em [Consumidores](#consumidores-conhecidos-do-iopraw) — em particular, confirmar a ligação real entre `clinical-doc-extractor` e o Volume `iop.silver.desfecho`.
- **Full-overwrite → incremental:** cuidado com late-arriving data e updates/deletes no Oracle. `EVOLUCAO_PACIENTE` já tem rastreamento parcial de deletes (`tb_evolucao_paciente_deletes`); as demais 28 tabelas do loop genérico não têm nenhum.
- **Falhas silenciosas:** `NTB14_EXTRACT_GED_ATENDIMENTO` engole exceções com `try/except: print(...)` — uma falha na extração de `GED_ATENDIMENTO` não derruba o job nem alerta ninguém.
- **`tb_iop_bi_projecao_receita_view_chama`:** cresce indefinidamente via `append` sem dedupe — reprocessamento/backfill duplicaria dados.
- **Governança do `iop`:** o grant amplo de `account users` pode estar mascarando dependências (alguém pode estar lendo `iop.raw` sem que isso apareça em nenhum job/app conhecido) — revisar com cautela antes de restringir.
- **Notebooks auxiliares não inspecionados em profundidade** (`old/`, `pysmb/`, "New Notebook..."): podem conter lógica ainda relevante ou lixo histórico — não assumir nenhuma das duas coisas sem checar.

---

## Referências

- `docs/08-MELHORIAS-ELT-IOP.md` no repositório `biomarcadores-app` (branch `BRI-11-migration-databricks`) — análise original que motivou esta migração.
- `databricks.yml` do projeto `clinical-doc-extractor` — padrão de Asset Bundle GitOps a seguir.
