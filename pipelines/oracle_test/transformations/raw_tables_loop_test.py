"""Spike: mesma validação de paciente_atend_medic_test.py, mas para as ~28 tabelas
do loop genérico de full-load (NTB02_extract_oracle_load_lake). Tabelas de teste,
não fazem parte do pipeline real -- ver resources/oracle_pipeline_test.pipeline.yml.

Não inclui EVOLUCAO_PACIENTE/PROTOCOLO (extração de coluna LONG via threads+RTF,
ainda não validada em Lakeflow) nem GED_ATENDIMENTO/view de receita (JDBC direto,
não Lakehouse Federation) -- ver README, seção do spike.
"""

from pyspark import pipelines as dp

RAW_LOOP_TABLES = [
    "ATEND_CHECK_LIST_RESULT",
    "ATENDIMENTO_SINAL_VITAL",
    "AUTORIZACAO_CONVENIO",
    "CAN_LOCO_REGIONAL",
    "CID_CATEGORIA",
    "CID_DOENCA",
    "COMPL_PESSOA_FISICA",
    "MATERIAL",
    "MED_AVALIACAO_PACIENTE",
    "MED_TIPO_VICIO",
    "MED_VALOR_DOMINIO",
    "MED_ITEM_AVALIAR",
    "MED_TIPO_AVALIACAO",
    "FUNCAO_PARAMETRO",
    "PACIENTE_ALERGIA",
    "PACIENTE_ANTEC_CLINICO",
    "PACIENTE_ATENDIMENTO",
    "PACIENTE_HABITO_VICIO",
    "PACIENTE_SETOR",
    "PESSOA_FISICA",
    "PESSOA_FISICA_PRONT_ESTAB",
    "TISS_AUTOR_ANEXO_DIAG",
    "nivel_capac_funcional_ecog",
    "SAC_PESQUISA_RESULT",
    "QUA_AVALIACAO_RESULT",
    "ESTRUTURA_MATERIAL_V",
    "GED_TIPO_ARQUIVO",
    "IOP_ESTABELECIMENTO_RELATORIO",
]


def _make_test_table(table_name):
    @dp.table(name=f"_test_lakeflow_{table_name.lower()}")
    def _test_table():
        return spark.read.table(f"oracle_med4u.tasy.{table_name}")

    return _test_table


for _table_name in RAW_LOOP_TABLES:
    _make_test_table(_table_name)
