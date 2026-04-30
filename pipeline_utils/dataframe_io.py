from __future__ import annotations

import re
import unicodedata

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


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
