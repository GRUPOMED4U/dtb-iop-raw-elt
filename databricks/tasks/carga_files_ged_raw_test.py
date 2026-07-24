"""Entrypoint de teste do job manual `load_files_ged_test`, task `carga_files_ged_raw_test`.

Roda a mesma lógica de dtb_iop_raw_elt.ged.carga_files, lendo o backlog real de
`iop.raw.tb_processados_join` (só leitura) e copiando os arquivos reais do GED via
SMB, mas escrevendo em destinos de teste -- não toca nos destinos reais de produção
usados pelo job legado `load_files_ged` (tabela de resultado e Volume).
"""

from databricks.sdk.runtime import dbutils, spark

from dtb_iop_raw_elt.ged.carga_files import run
from dtb_iop_raw_elt.ged.diff_pathfiles import SmbCredentials

RESULT_TABLE_TEST = "iop.raw.tb_result_carga_files_ged_test"
VOLUME_PATH_TEST = "/Volumes/iop/raw/files/GED_test/"


def main() -> None:
    creds = SmbCredentials(
        user=dbutils.secrets.get(scope="med4u_files_ged", key="user"),
        password=dbutils.secrets.get(scope="med4u_files_ged", key="pwd"),
        server_file_path=dbutils.secrets.get(scope="med4u_files_ged", key="file_path"),
    )
    run(spark, creds, result_table=RESULT_TABLE_TEST, volume_path=VOLUME_PATH_TEST)


if __name__ == "__main__":
    main()
