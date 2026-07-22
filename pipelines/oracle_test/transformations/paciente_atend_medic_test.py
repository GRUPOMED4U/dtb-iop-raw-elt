"""Spike: valida leitura do Oracle (via oracle_med4u/Lakehouse Federation) dentro de
uma Lakeflow Declarative Pipeline. Tabela de teste, não faz parte do pipeline real
-- ver resources/oracle_pipeline_test.pipeline.yml.
"""

from pyspark import pipelines as dp


@dp.table(name="_test_lakeflow_paciente_atend_medic")
def _test_lakeflow_paciente_atend_medic():
    return spark.read.table("oracle_med4u.tasy.PACIENTE_ATEND_MEDIC")
