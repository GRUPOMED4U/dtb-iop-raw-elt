"""Copia os arquivos pendentes do GED (SMB) para um Volume do Unity Catalog,
registrando o resultado incrementalmente e compactando a tabela de resultado no final.

Porta a lógica do notebook legado NTB02_extract_files_GED_load_lake
(job `load_files_ged`, task `carga_files_ged_raw`). Tabela/volume de destino são
parametrizáveis para permitir rodar contra destinos de teste antes do cutover --
ver databricks/tasks/carga_files_ged_raw_test.py.
"""

from __future__ import annotations

import os
from collections.abc import Callable, Iterator

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import BooleanType, LongType, StringType, StructField, StructType

from dtb_iop_raw_elt.ged.diff_pathfiles import DIFF_TABLE, SmbCredentials

RESULT_TABLE = "iop.raw.tb_result_carga_files_ged"
VOLUME_PATH = "/Volumes/iop/raw/files/GED/"

BATCH_SIZE = 5_000
NUM_PARTITIONS = 40  # 10 nodes x 2 cores x 2 (workload I/O-bound)

RESULT_SCHEMA = StructType(
    [
        StructField("file_id", LongType(), False),
        StructField("filename", StringType(), False),
        StructField("success", BooleanType(), False),
        StructField("error_message", StringType(), True),
    ]
)


def chunk_ids(ids: list[int], batch_size: int = BATCH_SIZE) -> list[list[int]]:
    """Divide os file_ids em lotes -- evita um mapInPandas gigante numa única passada."""
    return [ids[i : i + batch_size] for i in range(0, len(ids), batch_size)]


def copy_one_file(
    open_file: Callable, dest_base: str, filename: str, filepath_origem: str, creds: SmbCredentials
) -> dict:
    """Copia um arquivo do SMB pro destino; nunca levanta -- erro vira linha de resultado."""
    try:
        dest_file = os.path.join(dest_base, filename)
        src_kwargs = {"mode": "rb", "username": creds.user, "password": creds.password}
        with (
            open_file(filepath_origem, **src_kwargs) as src,
            open(dest_file, "wb") as dst,
        ):
            dst.write(src.read())
        return {"success": True, "error_message": None}
    except Exception as e:
        return {"success": False, "error_message": f"{type(e).__name__}: {e}"}


def make_copy_batch_fn(creds: SmbCredentials, dest_base: str):
    """Constrói a função de cópia distribuída (mapInPandas), fechando sobre credenciais/destino."""

    def copy_files_batch(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        from smbclient import open_file, reset_connection_cache

        reset_connection_cache()
        os.makedirs(dest_base, exist_ok=True)

        results = []
        for batch in iterator:
            for row in batch.itertuples():
                result = copy_one_file(
                    open_file, dest_base, row.filename, row.filepath_origem, creds
                )
                results.append({"file_id": row.file_id, "filename": row.filename, **result})
        yield pd.DataFrame(results)

    return copy_files_batch


def write_results_batch(df: DataFrame, result_table: str) -> None:
    df.write.option("mergeSchema", "true").mode("append").saveAsTable(result_table)


def optimize_result_table(spark: SparkSession, result_table: str) -> None:
    spark.sql(f"OPTIMIZE {result_table}")


def run(
    spark: SparkSession,
    creds: SmbCredentials,
    *,
    result_table: str = RESULT_TABLE,
    volume_path: str = VOLUME_PATH,
) -> None:
    """Copia em lotes os arquivos pendentes do GED, com persistência incremental por lote."""

    pending_df = spark.table(DIFF_TABLE).where("processed_at is null")
    all_ids = [row.file_id for row in pending_df.select("file_id").collect()]
    copy_batch_fn = make_copy_batch_fn(creds, dest_base=volume_path)

    for batch_ids in chunk_ids(all_ids):
        batch_df = pending_df.filter(col("file_id").isin(batch_ids))
        results_df = (
            batch_df.repartition(NUM_PARTITIONS)
            .mapInPandas(copy_batch_fn, schema=RESULT_SCHEMA)
            .withColumn("file_id", col("file_id").cast("int"))
            .withColumn("processed_at", current_timestamp())
        )
        write_results_batch(results_df, result_table)

    optimize_result_table(spark, result_table)
