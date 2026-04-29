from __future__ import annotations

import os
import re
import unicodedata
from typing import Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


NULL_TOKENS = ["", "NULL", "N/A", "NA", "NAN", "-", "NONE"]
VALID_UFS = [
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO",
    "MA", "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI",
    "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO"
]


def get_hdfs_base_uri() -> str:
    hdfs_base_uri = os.getenv("HDFS_BASE_URI")

    if not hdfs_base_uri:
        raise ValueError("Variável de ambiente HDFS_BASE_URI não definida.")

    return hdfs_base_uri.rstrip("/")


def normalize_column_name(name: str) -> str:
    name = unicodedata.normalize("NFKD", name)
    name = "".join(c for c in name if not unicodedata.combining(c))
    name = name.lower()
    name = re.sub(r"[^a-z0-9_]+", "_", name)
    name = re.sub(r"_+", "_", name)
    return name.strip("_")


def normalize_dataframe_columns(df: DataFrame) -> DataFrame:
    normalized_columns = [normalize_column_name(column) for column in df.columns]
    return df.toDF(*normalized_columns)


def create_namespace(
    spark: SparkSession,
    catalog: str,
    database: str,
    location: str,
) -> None:
    escaped_location = _escape_sql_string(location)

    if _is_session_catalog(catalog):
        iceberg_catalog = _configured_iceberg_hive_catalog(spark)

        if iceberg_catalog:
            namespace = _qualified_namespace(iceberg_catalog, database)
            spark.sql(f"""
                CREATE NAMESPACE IF NOT EXISTS {namespace}
                LOCATION '{escaped_location}'
            """)

        spark.sql(f"""
            CREATE DATABASE IF NOT EXISTS {_quote_identifier(database)}
            LOCATION '{escaped_location}'
        """)
    else:
        namespace = _qualified_namespace(catalog, database)
        spark.sql(f"""
            CREATE NAMESPACE IF NOT EXISTS {namespace}
            LOCATION '{escaped_location}'
        """)

    _assert_namespace_exists(spark, catalog, database)


def read_csv_raw(
    spark: SparkSession,
    path: str,
    sep: str = ";",
) -> DataFrame:
    return (
        spark.read
        .option("header", "true")
        .option("sep", sep)
        .option("inferSchema", "false")
        .option("mode", "PERMISSIVE")
        .option("recursiveFileLookup", "true")
        .csv(path)
    )


def cast_all_columns_to_string(df: DataFrame) -> DataFrame:
    return df.select([
        F.col(column).cast("string").alias(column)
        for column in df.columns
    ])


def clean_string(column_name: str):
    value = F.trim(F.col(column_name).cast("string"))

    return (
        F.when(value.isNull(), None)
        .when(F.upper(value).isin(NULL_TOKENS), None)
        .otherwise(value)
    )


def clean_upper(column_name: str):
    return F.upper(clean_string(column_name))


def only_digits(column_name: str):
    value = F.regexp_replace(clean_string(column_name), r"[^0-9]", "")

    return (
        F.when(value.isNull(), None)
        .when(value == "", None)
        .otherwise(value)
    )


def parse_long(column_name: str):
    value = F.regexp_replace(clean_string(column_name), r"[^0-9-]", "")

    return (
        F.when(value.rlike(r"^-?[0-9]+$"), value.cast("long"))
        .otherwise(F.lit(None).cast("long"))
    )


def parse_date(column_name: str):
    value = clean_string(column_name)

    return F.coalesce(
        F.to_date(value, "yyyy-MM-dd"),
        F.to_date(value, "dd/MM/yyyy"),
        F.to_date(value, "yyyyMMdd")
    )


def first_not_null(column_name: str):
    return F.first(F.col(column_name), ignorenulls=True).alias(column_name)


def _hash_expression(columns: Iterable[str]):
    return F.sha2(
        F.concat_ws(
            "||",
            *[
                F.coalesce(F.col(column).cast("string"), F.lit(""))
                for column in columns
            ]
        ),
        256
    )


def add_stable_bronze_id(
    df: DataFrame,
    id_columns: list[str],
    id_column_name: str = "_bronze_id",
) -> DataFrame:
    missing_columns = sorted(set(id_columns) - set(df.columns))

    if missing_columns:
        raise ValueError(f"Colunas ausentes para gerar {id_column_name}: {missing_columns}")

    return df.withColumn(id_column_name, _hash_expression(id_columns))


def add_record_hash(
    df: DataFrame,
    payload_columns: list[str],
    hash_column_name: str = "_record_hash",
) -> DataFrame:
    missing_columns = sorted(set(payload_columns) - set(df.columns))

    if missing_columns:
        raise ValueError(f"Colunas ausentes para gerar {hash_column_name}: {missing_columns}")

    return df.withColumn(hash_column_name, _hash_expression(payload_columns))


def add_bronze_metadata(
    df: DataFrame,
    source_system: str,
    batch_id: str,
) -> DataFrame:
    return (
        df
        .withColumn("_source_path", F.input_file_name())
        .withColumn("_source_system", F.lit(source_system))
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_updated_at", F.current_timestamp())
        .withColumn("_layer", F.lit("bronze"))
    )


def iceberg_table_exists(
    spark: SparkSession,
    table_name: str,
) -> bool:
    try:
        spark.table(table_name).limit(1).count()
        return True
    except Exception:
        return False


def _quote_identifier(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"


def _escape_sql_string(value: str) -> str:
    return value.replace("'", "''")


def _is_session_catalog(catalog: str) -> bool:
    return catalog.lower() in {"spark_catalog", "session"}


def _configured_iceberg_hive_catalog(spark: SparkSession) -> str | None:
    for catalog in ("iceberg",):
        try:
            catalog_class = spark.conf.get(f"spark.sql.catalog.{catalog}")
            catalog_type = spark.conf.get(f"spark.sql.catalog.{catalog}.type")
        except Exception:
            continue

        if (
            catalog_class == "org.apache.iceberg.spark.SparkCatalog"
            and catalog_type.lower() == "hive"
        ):
            return catalog

    return None


def _qualified_namespace(catalog: str, database: str) -> str:
    return ".".join([
        _quote_identifier(catalog),
        _quote_identifier(database),
    ])


def _assert_namespace_exists(
    spark: SparkSession,
    catalog: str,
    database: str,
) -> None:
    validation_catalog = (
        _configured_iceberg_hive_catalog(spark)
        if _is_session_catalog(catalog)
        else catalog
    )

    if validation_catalog:
        namespaces = [
            row[0]
            for row in spark.sql(
                f"SHOW NAMESPACES IN {_quote_identifier(validation_catalog)}"
            ).collect()
        ]
    elif _is_session_catalog(catalog):
        namespaces = [
            row[0]
            for row in spark.sql("SHOW DATABASES").collect()
        ]
    else:
        namespaces = [
            row[0]
            for row in spark.sql(f"SHOW NAMESPACES IN {_quote_identifier(catalog)}").collect()
        ]

    if database not in namespaces:
        raise RuntimeError(
            f"Namespace {catalog}.{database} não foi criado ou não está visível "
            "no metastore Hive."
        )


def _default_namespace_location(database: str) -> str:
    return f"{get_hdfs_base_uri()}/user/hive/warehouse/{database}.db"


def _ensure_table_namespace(
    spark: SparkSession,
    table_name: str,
) -> None:
    parts = table_name.split(".")

    if len(parts) < 3:
        return

    catalog = parts[0]
    database = parts[1]

    create_namespace(
        spark=spark,
        catalog=catalog,
        database=database,
        location=_default_namespace_location(database),
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
    if non_update_columns is None:
        non_update_columns = []

    missing_keys = sorted(set(key_columns) - set(source_df.columns))

    if missing_keys:
        raise ValueError(f"Colunas de chave ausentes no DataFrame: {missing_keys}")

    source_df.createOrReplaceTempView(source_view)

    if not iceberg_table_exists(spark, target_table):
        _ensure_table_namespace(spark, target_table)
        (
            source_df.writeTo(target_table)
            .using("iceberg")
            .tableProperty("format-version", "2")
            .create()
        )
        return

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

    update_clause = ",\n            ".join([
        f"target.{_quote_identifier(column)} = source.{_quote_identifier(column)}"
        for column in updatable_columns
    ])

    insert_columns = ", ".join([_quote_identifier(column) for column in all_columns])
    insert_values = ", ".join([f"source.{_quote_identifier(column)}" for column in all_columns])

    if compare_hash_column and compare_hash_column in all_columns:
        matched_condition = (
            f"target.{_quote_identifier(compare_hash_column)} "
            f"<> source.{_quote_identifier(compare_hash_column)}"
        )
    else:
        matched_condition = "true"

    spark.sql(f"""
        MERGE INTO {target_table} target
        USING {source_view} source
        ON {on_clause}
        WHEN MATCHED AND {matched_condition} THEN UPDATE SET
            {update_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_columns})
        VALUES ({insert_values})
    """)

def validate_expected_columns(df: DataFrame, expected_columns: list[str]) -> None:
    missing_columns = sorted(set(expected_columns) - set(df.columns))

    if missing_columns:
        raise ValueError(f"Colunas ausentes: {missing_columns}")


def get_latest_batch_id(
    spark: SparkSession,
    table_name: str,
    batch_column: str = "_batch_id",
) -> str:
    result = (
        spark.table(table_name)
        .select(batch_column)
        .where(F.col(batch_column).isNotNull())
        .distinct()
        .orderBy(F.col(batch_column).desc())
        .limit(1)
        .collect()
    )

    if not result:
        raise ValueError(f"Nenhum batch encontrado em {table_name}.")

    return result[0][batch_column]


def add_silver_metadata(
    df: DataFrame,
    batch_id: str,
) -> DataFrame:
    return (
        df
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_silver_ingested_at", F.current_timestamp())
        .withColumn("_layer", F.lit("silver"))
    )


def build_null_condition(required_columns: list[str]):
    condition = None

    for column_name in required_columns:
        current_condition = F.col(column_name).isNull()

        if condition is None:
            condition = current_condition
        else:
            condition = condition | current_condition

    return condition


def build_rejection_reason(required_columns: list[str]):
    return F.concat_ws(
        "; ",
        *[
            F.when(F.col(column_name).isNull(), F.lit(f"{column_name} ausente"))
            for column_name in required_columns
        ]
    )
