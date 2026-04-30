from __future__ import annotations

from pyspark.sql import SparkSession

from pipeline_utils.pipeline_config import get_hdfs_base_uri


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
