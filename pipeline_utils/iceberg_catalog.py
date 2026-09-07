from __future__ import annotations

import re

from pyspark.sql import SparkSession



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


def validate_table_columns(
    spark: SparkSession,
    table_name: str,
    expected_columns: tuple[str, ...] | list[str],
) -> None:
    rows = spark.sql(f"DESCRIBE {table_name}").collect()
    actual_columns = {
        str(row[0]).lower()
        for row in rows
        if row[0] and not str(row[0]).startswith("#")
    }
    missing_columns = sorted(set(expected_columns) - actual_columns)
    if missing_columns:
        raise ValueError(f"Colunas ausentes em {table_name}: {missing_columns}")


def tag_current_snapshot(
    spark: SparkSession,
    table_name: str,
    tag_name: str,
) -> int | None:
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_-]*", tag_name):
        raise ValueError(f"Nome de tag Iceberg inválido: {tag_name}")

    snapshots = spark.sql(
        f"""
        SELECT snapshot_id
        FROM {table_name}.snapshots
        ORDER BY committed_at DESC
        LIMIT 1
        """
    ).collect()
    if not snapshots:
        return None

    snapshot_id = int(snapshots[0][0])
    escaped_tag = tag_name.replace("`", "``")
    spark.sql(
        f"ALTER TABLE {table_name} CREATE OR REPLACE TAG `{escaped_tag}` "
        f"AS OF VERSION {snapshot_id}"
    )
    return snapshot_id


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
    return f"{_quote_identifier(catalog)}.{_quote_identifier(database)}"


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
