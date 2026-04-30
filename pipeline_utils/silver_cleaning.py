from __future__ import annotations

from pyspark.sql import functions as F

from pipeline_utils.constants import NULL_TOKENS


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
