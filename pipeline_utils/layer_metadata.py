from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


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
        .withColumn("_layer", F.lit("bronze"))
    )


def add_silver_metadata(
    df: DataFrame,
    batch_id: str,
) -> DataFrame:
    return (
        df
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_layer", F.lit("silver"))
    )


def add_gold_metadata(
    df: DataFrame,
    batch_id: str,
) -> DataFrame:
    return (
        df
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_gold_ingested_at", F.current_timestamp())
        .withColumn("_layer", F.lit("gold"))
    )
