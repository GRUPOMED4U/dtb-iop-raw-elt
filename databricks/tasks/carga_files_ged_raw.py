"""Entrypoint do job `load_files_ged`, task `carga_files_ged_raw`.

Só resolve credenciais via dbutils.secrets e chama a lógica testável em
dtb_iop_raw_elt.ged.carga_files. Escreve nos destinos reais de produção
(RESULT_TABLE/VOLUME_PATH default) -- para rodar contra destinos de teste,
ver carga_files_ged_raw_test.py.
"""

from databricks.sdk.runtime import dbutils, spark

from dtb_iop_raw_elt.ged.carga_files import run
from dtb_iop_raw_elt.ged.diff_pathfiles import SmbCredentials


def main() -> None:
    creds = SmbCredentials(
        user=dbutils.secrets.get(scope="med4u_files_ged", key="user"),
        password=dbutils.secrets.get(scope="med4u_files_ged", key="pwd"),
        server_file_path=dbutils.secrets.get(scope="med4u_files_ged", key="file_path"),
    )
    run(spark, creds)


if __name__ == "__main__":
    main()
