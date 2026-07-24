"""Entrypoint do job `load_files_ged`, task `create_tb_diferencial_paths`.

Só resolve credenciais via dbutils.secrets e chama a lógica testável em
dtb_iop_raw_elt.ged.diff_pathfiles.
"""

from databricks.sdk.runtime import dbutils, spark

from dtb_iop_raw_elt.ged.diff_pathfiles import SmbCredentials, run


def main() -> None:
    creds = SmbCredentials(
        user=dbutils.secrets.get(scope="med4u_files_ged", key="user"),
        password=dbutils.secrets.get(scope="med4u_files_ged", key="pwd"),
        server_file_path=dbutils.secrets.get(scope="med4u_files_ged", key="file_path"),
    )
    run(spark, creds)


if __name__ == "__main__":
    main()
