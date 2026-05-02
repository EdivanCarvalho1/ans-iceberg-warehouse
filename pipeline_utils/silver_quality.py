from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


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


def build_null_condition(required_columns: list[str]):
    condition = None

    for column_name in required_columns:
        current_condition = F.col(column_name).isNull()

        if condition is None:
            condition = current_condition
        else:
            condition = condition | current_condition

    return condition if condition is not None else F.lit(False)


def build_rejection_reason(required_columns: list[str]):
    rejection_reason = F.concat_ws(
        "; ",
        *[
            F.when(F.col(column_name).isNull(), F.lit(f"{column_name} ausente"))
            for column_name in required_columns
        ]
    )

    return F.when(rejection_reason != "", rejection_reason)


def deduplicate_by_keys(
    df: DataFrame,
    key_columns: list[str],
    order_columns: list[str],
    rank_column: str = "_dedup_rank",
) -> DataFrame:
    """
    Deduplicação determinística para Silver.
    """
    missing_keys = sorted(set(key_columns) - set(df.columns))
    missing_order = sorted(set(order_columns) - set(df.columns))

    if missing_keys:
        raise ValueError(f"Colunas de chave ausentes para deduplicação: {missing_keys}")

    if missing_order:
        raise ValueError(f"Colunas de ordenação ausentes para deduplicação: {missing_order}")

    window = Window.partitionBy(*[F.col(column) for column in key_columns]).orderBy(
        *[F.col(column).desc_nulls_last() for column in order_columns]
    )

    return (
        df
        .withColumn(rank_column, F.row_number().over(window))
        .where(F.col(rank_column) == 1)
        .drop(rank_column)
    )
