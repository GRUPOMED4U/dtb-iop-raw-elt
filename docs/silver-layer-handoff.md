# Contexto pra camada Silver (handoff pro Leonardo)

> Este documento é o ponto de partida pra quem for construir a camada `iop.silver`. A receita técnica detalhada (SP, grants, OIDC, YAML da pipeline) está em [`docs/lakeflow-declarative-pipelines.md`](./lakeflow-declarative-pipelines.md) — leia os dois juntos.

## Contexto: o que já existe

Este repositório (`dtb-iop-raw-elt`) cobre só a camada **raw**: extração do Oracle (Tasy) e do GED pro Unity Catalog (`iop.raw`). Nada daqui escreve em `iop.silver` — essa parte é um projeto novo, separado.

Validamos nesta semana (2026-07-22/23) um padrão de GitOps completo, de ponta a ponta, que funcionou:
- Repositório no GitHub → GitHub Actions autentica no Databricks via **OIDC** (Workload Identity Federation), **sem secret nenhum guardado**.
- Deploy via **Databricks Asset Bundle** (`databricks.yml`).
- Pra pipelines Oracle especificamente, validamos que **Lakeflow Declarative Pipelines** funciona bem (lê via Lakehouse Federation, roda em compute clássico pra ter rota de rede) — ver o README deste repo, seção "Spike: Lakeflow Declarative Pipelines pro lado Oracle".

A recomendação é a camada silver seguir **o mesmo padrão**, com **repositório e service principal próprios** — não reaproveitar o `sp-iop-elt` nem este repositório. Motivo: isolamento de blast radius — se o CI de um projeto for comprometido ou tiver bug, não deve conseguir agir no espaço do outro.

## Convenção de nomes sugerida

| Item | Raw (já existe) | Silver (sugestão) |
|---|---|---|
| Repositório GitHub | `GRUPOMED4U/dtb-iop-raw-elt` | `GRUPOMED4U/dtb-iop-silver-elt` |
| Service principal | `sp-iop-elt` | `sp-iop-silver-elt` |
| GitHub Environments | `dev` / `prod` | `dev` / `prod` (igual) |
| Catálogo/schema de escrita | `iop.raw` | `iop.silver` (schema já existe no catálogo `iop`) |

## O que o `sp-iop-silver-elt` vai precisar de grant

Pra "bater na raw" (ler `iop.raw`) e escrever em `iop.silver`:

```sql
GRANT USE_CATALOG ON CATALOG iop TO `sp-iop-silver-elt`;
GRANT USE_SCHEMA, SELECT ON SCHEMA iop.raw TO `sp-iop-silver-elt`;
GRANT USE_SCHEMA, SELECT, CREATE_TABLE, CREATE_VOLUME, MODIFY ON SCHEMA iop.silver TO `sp-iop-silver-elt`;
```

Isso é leitura restrita só ao schema `raw` (não o catálogo `iop` inteiro) + escrita restrita só ao `silver` — não dá acesso a `gold`/`elt` nem a outros catálogos.

## Checklist pra montar o esqueleto (passo a passo)

Segue exatamente a receita em `lakeflow-declarative-pipelines.md`, na ordem:

1. ✅ **Feito:** repositório [`dtb-iop-silver-elt`](https://github.com/GRUPOMED4U/dtb-iop-silver-elt) criado no GitHub (org `GRUPOMED4U`), com `README.md` (mesmo modelo deste repo). `CLAUDE.md` próprio ainda não foi criado lá — considerar replicar as regras operacionais deste repo antes de começar a escrever transformações.
2. ✅ **Feito (2026-07-23):** service principal `sp-iop-silver-elt` criado na conta Databricks — client id (application id) `d9304262-1a5a-498f-96ed-d9099bb9adff`, account id (SCIM) `211855069202202`.
3. ✅ **Feito (2026-07-23):** grants acima aplicados (`iop.raw` leitura, `iop.silver` escrita), de forma aditiva, via `databricks grants update schema/catalog`.
4. ✅ **Feito (2026-07-23):** entitlement `allow-cluster-create` concedida ao SP no workspace dev (**pegadinha real que já pegamos** — sem isso a pipeline falha na hora de subir cluster, mesmo com os grants de dados certos). Workspace access `USER` também concedido em dev e prod.
5. ✅ **Feito (2026-07-23):** federation policies OIDC (dev + prod) criadas no SP, apontando pro repo novo — subject/audience calculados a partir dos ids imutáveis reais (org id `197661296`, repo id `1309974122`), no mesmo padrão validado no `sp-iop-elt`. **Ainda não testadas rodando de verdade** — se `TOKEN_SUBJECT_INVALID` aparecer na primeira execução da Action, usar o subject que o próprio erro devolve.
6. ✅ **Feito (2026-07-23):** GitHub Environments `dev`/`prod` criados no repo novo (`prod` restrito à branch `main`, sem trigger automático).
7. ✅ **Feito (2026-07-23):** `databricks.yml` com os 2 targets (mesmos hosts de workspace do raw, já que é o mesmo metastore/conta) — sem nenhum resource ainda.
8. ✅ **Feito (2026-07-23):** Action `test-connection.yml` (mesmo modelo deste repo) criada e com push feito — **ainda não rodou** (só dispara em push pra `main` ou manualmente).
9. Só depois de tudo acima validado rodando de verdade, começar a escrever as transformações (`@dp.table`) que leem de `iop.raw` e escrevem em `iop.silver`. **Ainda não iniciado.**

## Referências

- [`docs/lakeflow-declarative-pipelines.md`](./lakeflow-declarative-pipelines.md) — receita técnica completa, com os comandos exatos.
- `README.md` deste repo — arquitetura da camada raw, pra entender o que exatamente vai estar disponível em `iop.raw` como fonte.
