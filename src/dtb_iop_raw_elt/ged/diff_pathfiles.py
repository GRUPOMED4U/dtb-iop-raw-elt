"""Lista os arquivos do compartilhamento SMB do GED e calcula o diff contra o que já foi processado.

Porta a lógica do notebook legado NTB01_create_table_pathsfiles_onprimess
(job `load_files_ged`, task `create_tb_diferencial_paths`).
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession

PATHFILES_TABLE = "iop.raw.tb_pathfiles_ged_aprocessar"
RESULT_TABLE = "iop.raw.tb_result_carga_files_ged"
DIFF_TABLE = "iop.raw.tb_processados_join"


@dataclass(frozen=True)
class SmbCredentials:
    user: str
    password: str
    server_file_path: str


def list_smb_files(creds: SmbCredentials) -> list[str]:
    """Lista os arquivos no compartilhamento SMB do GED."""
    from smbclient import listdir, register_session, reset_connection_cache

    reset_connection_cache()
    server = creds.server_file_path.replace("/", "\\").lstrip("\\").split("\\")[0]
    register_session(server, username=creds.user, password=creds.password, encrypt=True)
    return listdir(creds.server_file_path, encrypt=True)


def build_pathfiles_df(spark: SparkSession, filenames: list[str], server_file_path: str) -> DataFrame:
    """Monta o DataFrame de arquivos encontrados no GED, com o caminho completo de origem."""
    base_path = server_file_path.rstrip("/").rstrip("\\")
    rows = [(i, filename, f"{base_path}\\{filename}") for i, filename in enumerate(filenames)]
    return spark.createDataFrame(rows, ["file_id", "filename", "filepath_origem"])


def write_pathfiles(df: DataFrame) -> None:
    """Grava o DataFrame de arquivos encontrados na tabela de controle (full overwrite)."""
    df.write.mode("overwrite").saveAsTable(PATHFILES_TABLE)


def refresh_diff_table(spark: SparkSession) -> None:
    """Recria a tabela de diff: arquivos a processar vs. já processados (por hash do filename)."""
    spark.sql(f"""
        CREATE OR REPLACE TABLE {DIFF_TABLE} AS
        WITH tb_aprocess AS (
            SELECT DISTINCT
                tra.*,
                sha2(tra.filename, 256) AS chave_filename
            FROM {PATHFILES_TABLE} tra
        ),
        tb_process AS (
            SELECT
                tr.filename,
                max(tr.processed_at) AS processed_at,
                tr.success,
                concat_ws(", ", collect_set(tr.error_message)) AS error_message,
                sha2(tr.filename, 256) AS chave_filename
            FROM {RESULT_TABLE} tr
            GROUP BY ALL
        )
        SELECT DISTINCT
            tra.file_id,
            tra.filename,
            tra.filepath_origem,
            tr.processed_at,
            tr.error_message,
            tr.success
        FROM tb_aprocess tra
        LEFT JOIN tb_process tr ON tra.chave_filename = tr.chave_filename
    """)


def run(spark: SparkSession, creds: SmbCredentials) -> None:
    """Ponto de entrada: lista arquivos do GED, grava tabela de controle e recalcula o diff."""
    filenames = list_smb_files(creds)
    df = build_pathfiles_df(spark, filenames, creds.server_file_path)
    write_pathfiles(df)
    refresh_diff_table(spark)
