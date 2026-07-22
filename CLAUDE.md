# CLAUDE.md

Instruções que se auto-carregam em toda sessão do Claude Code neste repositório.

## ⚠️ Regra obrigatória: README sempre em dia

Toda alteração de código, configuração (`databricks.yml`, jobs, clusters, schemas) ou arquitetura neste repositório **deve vir acompanhada da atualização correspondente no `README.md`** no mesmo commit/PR. Nunca considere uma tarefa concluída sem revisar se o README ainda reflete fielmente o estado atual do projeto.

Isso vale especialmente para:
- Adicionar, remover ou reordenar tasks em qualquer job do Asset Bundle.
- Mudar o modo de carga de uma tabela (full-overwrite → incremental/MERGE, ou vice-versa).
- Mudar ownership de jobs, connections ou catálogos.
- Portar um notebook legado para código versionado (atualizar a tabela "Estado da migração").
- Qualquer mudança nos grants do catálogo `iop`.

## Contexto do projeto

Este repositório é o destino da modernização da ELT do IOP (Oracle/Tasy + GED → `iop.raw`), hoje rodando como notebooks legados em produção na pasta pessoal de `jefferson.silva@spesia.com.br`. A migração segue o padrão GitOps já validado no projeto irmão `clinical-doc-extractor` (Databricks Asset Bundle + CI).

**Regras de segurança operacional (ver histórico do projeto para o prompt completo):**
- Nunca pausar, editar, mover ou apagar os jobs/notebooks/clusters legados em produção sem autorização explícita — eles alimentam consumidores em produção (`clinical-doc-extractor` e possivelmente outros).
- Investigação e leitura (via `databricks jobs get`, `clusters get`, `SELECT`/`DESCRIBE`) são livres. Qualquer ação de escrita/deploy/pausa/delete para e pergunta antes.
- Tudo que for construído de novo nasce em paralelo (job pausado, schema/tabela nova), nunca substituindo o que existe, até validação e aval explícito.
- Construir aos poucos: uma unidade pequena de trabalho por vez (um notebook, uma tabela), mostrar o resultado, esperar validação, só então seguir para a próxima.

Antes de assumir que qualquer fato operacional deste README ainda é verdade (jobs, clusters, connections, grants), reconfirme via CLI — o ambiente pode ter mudado desde a última verificação registrada no README.
