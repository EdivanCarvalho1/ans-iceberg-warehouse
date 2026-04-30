from __future__ import annotations

import os
import re

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipeline_utils.iceberg_catalog import _ensure_table_namespace, _quote_identifier
from pipeline_utils.pipeline_config import default_spark_local_dir


def iceberg_table_exists(
    spark: SparkSession,
    table_name: str,
) -> bool:
    try:
        spark.table(table_name).limit(1).count()
        return True
    except Exception:
        return False


def append_iceberg(
    spark: SparkSession,
    source_df: DataFrame,
    target_table: str,
) -> None:
    """
    Append puro.

    """
    if not iceberg_table_exists(spark, target_table):
        _ensure_table_namespace(spark, target_table)
        (
            source_df.writeTo(target_table)
            .using("iceberg")
            .tableProperty("format-version", "2")
            .create()
        )
        return

    source_df.writeTo(target_table).append()


def filter_new_source_files(
    spark: SparkSession,
    source_df: DataFrame,
    target_table: str,
    source_path_column: str = "_source_path",
    force_reprocess: bool = False,
) -> DataFrame:
    """
    Filtra arquivos já processados na Bronze.
    """
    if force_reprocess:
        return source_df

    if not iceberg_table_exists(spark, target_table):
        return source_df

    target_df = spark.table(target_table)

    if source_path_column not in target_df.columns:
        return source_df

    processed_files = (
        target_df
        .select(F.col(source_path_column).alias(source_path_column))
        .where(F.col(source_path_column).isNotNull())
        .distinct()
    )

    return source_df.join(
        processed_files,
        on=source_path_column,
        how="left_anti",
    )


def merge_iceberg_by_keys(
    spark: SparkSession,
    source_df: DataFrame,
    target_table: str,
    key_columns: list[str],
    source_view: str,
    compare_hash_column: str | None = "_record_hash",
    non_update_columns: list[str] | None = None,
) -> None:
    """
    MERGE SCD1 por chave natural/de negócio.
    """
    if non_update_columns is None:
        non_update_columns = []

    missing_keys = sorted(set(key_columns) - set(source_df.columns))

    if missing_keys:
        raise ValueError(f"Colunas de chave ausentes no DataFrame: {missing_keys}")

    if not iceberg_table_exists(spark, target_table):
        _ensure_table_namespace(spark, target_table)
        (
            source_df.writeTo(target_table)
            .using("iceberg")
            .tableProperty("format-version", "2")
            .create()
        )
        return

    source_df = _materialize_merge_source(spark, source_df)
    source_df.createOrReplaceTempView(source_view)
    all_columns = source_df.columns

    on_clause = " AND ".join([
        f"target.{_quote_identifier(column)} <=> source.{_quote_identifier(column)}"
        for column in key_columns
    ])

    updatable_columns = [
        column
        for column in all_columns
        if column not in key_columns and column not in non_update_columns
    ]

    insert_columns = ", ".join([_quote_identifier(column) for column in all_columns])
    insert_values = ", ".join([f"source.{_quote_identifier(column)}" for column in all_columns])

    if compare_hash_column and compare_hash_column in all_columns:
        matched_condition = (
            f"target.{_quote_identifier(compare_hash_column)} "
            f"<> source.{_quote_identifier(compare_hash_column)}"
        )
    else:
        matched_condition = "true"

    if updatable_columns:
        update_clause = ",\n            ".join([
            f"target.{_quote_identifier(column)} = source.{_quote_identifier(column)}"
            for column in updatable_columns
        ])

        spark.sql(f"""
            MERGE INTO {target_table} target
            USING {source_view} source
            ON {on_clause}
            WHEN MATCHED AND {matched_condition} THEN UPDATE SET
                {update_clause}
            WHEN NOT MATCHED THEN INSERT ({insert_columns})
            VALUES ({insert_values})
        """)
    else:
        spark.sql(f"""
            MERGE INTO {target_table} target
            USING {source_view} source
            ON {on_clause}
            WHEN NOT MATCHED THEN INSERT ({insert_columns})
            VALUES ({insert_values})
        """)


def replace_iceberg_by_partition_values(
    spark: SparkSession,
    source_df: DataFrame,
    target_table: str,
    partition_columns: list[str],
    source_view: str,
    keys_view: str,
) -> None:
    """
    Substitui os registros do destino para os valores de partição presentes na origem.
    """
    missing_columns = sorted(set(partition_columns) - set(source_df.columns))

    if missing_columns:
        raise ValueError(f"Colunas de partição ausentes no DataFrame: {missing_columns}")

    if not iceberg_table_exists(spark, target_table):
        _ensure_table_namespace(spark, target_table)
        (
            source_df.writeTo(target_table)
            .using("iceberg")
            .tableProperty("format-version", "2")
            .create()
        )
        return

    source_df = _materialize_merge_source(spark, source_df)
    source_df.createOrReplaceTempView(source_view)

    keys_df = source_df.select(*partition_columns).distinct()
    keys_df.createOrReplaceTempView(keys_view)

    on_clause = " AND ".join([
        f"target.{_quote_identifier(column)} <=> source.{_quote_identifier(column)}"
        for column in partition_columns
    ])

    spark.sql(f"""
        MERGE INTO {target_table} target
        USING {keys_view} source
        ON {on_clause}
        WHEN MATCHED THEN DELETE
    """)

    source_df.writeTo(target_table).append()


def _materialize_merge_source(spark: SparkSession, source_df: DataFrame) -> DataFrame:
    if spark.sparkContext.getCheckpointDir() is None:
        spark.sparkContext.setCheckpointDir(_default_checkpoint_dir(spark))

    return source_df.checkpoint(eager=True)


def _default_checkpoint_dir(spark: SparkSession) -> str:
    configured_dir = os.getenv("SPARK_CHECKPOINT_DIR")
    if configured_dir:
        return configured_dir

    hdfs_base_uri = os.getenv("HDFS_BASE_URI")
    if hdfs_base_uri:
        return f"{hdfs_base_uri.rstrip('/')}/tmp/spark-checkpoints"

    default_fs = spark.sparkContext.hadoopConfiguration().get("fs.defaultFS")
    if default_fs and default_fs != "file:///":
        return f"{default_fs.rstrip('/')}/tmp/spark-checkpoints"

    warehouse_dir = spark.conf.get("spark.sql.warehouse.dir", "")
    hdfs_match = re.match(r"^(hdfs://[^/]+)", warehouse_dir)
    if hdfs_match:
        return f"{hdfs_match.group(1)}/tmp/spark-checkpoints"

    return os.path.join(default_spark_local_dir(), "spark-checkpoints")
