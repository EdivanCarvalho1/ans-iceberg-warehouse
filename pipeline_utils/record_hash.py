from __future__ import annotations

from typing import Iterable

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


_HASH_NULL_TOKEN = "__NULL__"


def _hash_expression(columns: Iterable[str]):
    """
    Fingerprint técnico para comparação de conteúdo.
    """
    return F.sha2(
        F.concat_ws(
            "||",
            *[
                F.coalesce(F.col(column).cast("string"), F.lit(""))
                for column in columns
            ]
        ),
        256,
    )


def _hash_key_expression(columns: Iterable[str], namespace: str | None = None):
    """
    Hash determinístico para surrogate key dimensional.

    O namespace evita colisão semântica entre dimensões diferentes que possam
    usar o mesmo valor natural. Ex.: cd_operadora='123' e cd_municipio='123'.
    """
    expressions = []

    if namespace:
        expressions.append(F.lit(namespace))

    expressions.extend([
        F.coalesce(
            F.trim(F.col(column).cast("string")),
            F.lit(_HASH_NULL_TOKEN),
        )
        for column in columns
    ])

    return F.sha2(F.concat_ws("||", *expressions), 256)


def add_record_hash(
    df: DataFrame,
    payload_columns: list[str],
    hash_column_name: str = "_record_hash",
) -> DataFrame:
    """
    Adiciona hash técnico para comparação de payload.
    """
    missing_columns = sorted(set(payload_columns) - set(df.columns))

    if missing_columns:
        raise ValueError(f"Colunas ausentes para gerar {hash_column_name}: {missing_columns}")

    return df.withColumn(hash_column_name, _hash_expression(payload_columns))


def add_hash_key(
    df: DataFrame,
    key_columns: list[str],
    key_column_name: str,
    key_namespace: str | None = None,
) -> DataFrame:
    """
    Adiciona surrogate key determinística baseada em hash.
    """
    missing_columns = sorted(set(key_columns) - set(df.columns))

    if missing_columns:
        raise ValueError(f"Colunas ausentes para gerar {key_column_name}: {missing_columns}")

    return df.withColumn(
        key_column_name,
        _hash_key_expression(key_columns, namespace=key_namespace),
    )
