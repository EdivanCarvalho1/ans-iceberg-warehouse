from __future__ import annotations

import ast
import json
import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]

NOTEBOOKS = (
    ROOT / "load_bronze_layer.ipynb",
    ROOT / "load_silver_layer.ipynb",
    ROOT / "load_gold_layer.ipynb",
    ROOT / "beneficiarios_reports.ipynb",
)

SQL_ROOT = ROOT / "pipeline_utils" / "sql"

LAYER_SQL_FILES = {
    "load_bronze_layer.ipynb": (SQL_ROOT / "bronze_insert.sql",),
    "load_silver_layer.ipynb": tuple(sorted(SQL_ROOT.glob("silver_*.sql"))),
    "load_gold_layer.ipynb": tuple(sorted(SQL_ROOT.glob("gold_*.sql"))),
}

OPERATIONAL_PYTHON = (
    ROOT / "utils.py",
    ROOT / "pipeline_utils",
)

METADATA_COLUMNS = (
    "_source_path",
    "_source_system",
    "_batch_id",
    "_ingested_at",
    "_gold_ingested_at",
    "_layer",
    "_record_hash",
    "_bronze_record_hash",
    "_bronze_ingested_at",
    "_rejected_at",
    "_rejection_reason",
)

FORBIDDEN_DATAFRAME_PATTERNS = (
    r"from\s+pyspark\.sql\s+import\s+functions",
    r"from\s+pyspark\.sql\.window\s+import\s+Window",
    r"\bDataFrame\b",
    r"\.select\s*\(",
    r"\.where\s*\(",
    r"\.filter\s*\(",
    r"\.withColumn\s*\(",
    r"\.drop\s*\(",
    r"\.join\s*\(",
    r"\.groupBy\s*\(",
    r"\.agg\s*\(",
    r"\.orderBy\s*\(",
    r"\.distinct\s*\(",
    r"\.union\s*\(",
    r"\.writeTo\s*\(",
    r"spark\.table\s*\(",
    r"spark\.read\b",
    r"createOrReplaceTempView\s*\(",
    r"\.transform\s*\(",
)

BUSINESS_COLUMNS = (
    "id_cmpt_movel",
    "cd_operadora",
    "nm_razao_social",
    "nr_cnpj",
    "modalidade_operadora",
    "sg_uf",
    "cd_municipio",
    "nm_municipio",
    "tp_sexo",
    "de_faixa_etaria",
    "de_faixa_etaria_reaj",
    "cd_plano",
    "tp_vigencia_plano",
    "de_contratacao_plano",
    "de_segmentacao_plano",
    "de_abrg_geografica_plano",
    "cobertura_assist_plan",
    "tipo_vinculo",
    "qt_beneficiario_ativo",
    "qt_beneficiario_aderido",
    "qt_beneficiario_cancelado",
    "dt_carga",
)


def notebook_source(path: Path) -> str:
    notebook = json.loads(path.read_text(encoding="utf-8"))
    return "\n".join(
        "".join(cell.get("source", []))
        for cell in notebook["cells"]
        if cell.get("cell_type") == "code"
    )


def python_sources() -> list[tuple[Path, str]]:
    paths = [OPERATIONAL_PYTHON[0], *sorted(OPERATIONAL_PYTHON[1].glob("*.py"))]
    return [(path, path.read_text(encoding="utf-8")) for path in paths]


def layer_source(path: Path) -> str:
    sql_source = "\n".join(
        sql_path.read_text(encoding="utf-8")
        for sql_path in LAYER_SQL_FILES.get(path.name, ())
    )
    return notebook_source(path) + "\n" + sql_source


class SqlPipelineContractTests(unittest.TestCase):
    def test_notebooks_are_valid_json(self) -> None:
        for path in NOTEBOOKS:
            with self.subTest(path=path.name):
                json.loads(path.read_text(encoding="utf-8"))

    def test_notebook_code_is_valid_python(self) -> None:
        for path in NOTEBOOKS:
            with self.subTest(path=path.name):
                ast.parse(notebook_source(path), filename=str(path))

    def test_layers_use_explicit_atomic_sql_writes(self) -> None:
        for path in NOTEBOOKS[:3]:
            source = layer_source(path)
            with self.subTest(path=path.name):
                self.assertIn("spark.sql", source)
                self.assertIn("CREATE TABLE IF NOT EXISTS", source)
                self.assertIn("INSERT OVERWRITE", source)
                self.assertNotRegex(source, r"\bMERGE\s+INTO\b")
                self.assertNotRegex(source, r"\bDELETE\s+FROM\b")
                self.assertNotRegex(source, r"\.append\s*\(")

    def test_layer_sql_contains_business_contract_and_sql_transformations(self) -> None:
        for path in NOTEBOOKS[:3]:
            source = layer_source(path)
            with self.subTest(path=path.name):
                for column in BUSINESS_COLUMNS:
                    self.assertIn(column, source)

        silver = layer_source(NOTEBOOKS[1])
        gold = layer_source(NOTEBOOKS[2])
        self.assertIn("ROW_NUMBER() OVER", silver)
        self.assertIn("CASE WHEN", silver)
        self.assertIn("sha2", silver.lower())
        self.assertNotIn("_rejection_reason", silver)
        self.assertIn("ROW_NUMBER() OVER", gold)
        self.assertIn("sha2", gold.lower())
        self.assertIn("JOIN", gold)

    def test_reports_execute_each_query_directly_with_spark_sql(self) -> None:
        source = notebook_source(NOTEBOOKS[3])
        self.assertNotIn("def query(", source)
        self.assertGreaterEqual(source.count("spark.sql("), 8)
        self.assertNotRegex(source, r"\.limit\s*\(")
        self.assertIn("ICEBERG_GOLD_TAG", source)
        self.assertIn(".tag_", source)

    def test_operational_code_has_no_dataframe_processing_or_row_metadata(self) -> None:
        sources = [(path, layer_source(path)) for path in NOTEBOOKS[:3]]
        sources.append((NOTEBOOKS[3], notebook_source(NOTEBOOKS[3])))
        sources.extend(python_sources())
        sources.extend(
            (path, path.read_text(encoding="utf-8"))
            for paths in LAYER_SQL_FILES.values()
            for path in paths
        )
        for path, source in sources:
            with self.subTest(path=path):
                for pattern in FORBIDDEN_DATAFRAME_PATTERNS:
                    self.assertNotRegex(source, pattern)
                for column in METADATA_COLUMNS:
                    self.assertNotIn(column, source)

    def test_python_is_limited_to_configuration_contracts_and_sql_orchestration(self) -> None:
        for path, source in python_sources():
            tree = ast.parse(source, filename=str(path))
            with self.subTest(path=path):
                for node in ast.walk(tree):
                    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        self.assertNotRegex(node.name, r"^(build|add|clean|parse|deduplicate|merge|append)")

    def test_no_sql_builder_or_fragment_helpers_are_present(self) -> None:
        sources = [(path, layer_source(path)) for path in NOTEBOOKS[:3]]
        sources.append((NOTEBOOKS[3], notebook_source(NOTEBOOKS[3])))
        sources.extend(python_sources())
        for path, source in sources:
            with self.subTest(path=path):
                self.assertNotRegex(source, r"def\s+(build|make|render|compose).*sql")
                self.assertNotRegex(source, r"sql\s*=\s*\[")
                self.assertNotRegex(source, r"\.join\s*\(.*SELECT")

    def test_snapshots_and_tags_are_the_operational_lineage_boundary(self) -> None:
        for path in NOTEBOOKS[:3]:
            with self.subTest(path=path.name):
                self.assertIn("tag_current_snapshot", notebook_source(path))

        catalog = (ROOT / "pipeline_utils" / "iceberg_catalog.py").read_text(encoding="utf-8")
        self.assertIn(".snapshots", catalog)
        self.assertIn("CREATE OR REPLACE TAG", catalog)

    def test_removed_dataframe_helpers_are_not_production_modules(self) -> None:
        for filename in (
            "dataframe_io.py",
            "layer_metadata.py",
            "record_hash.py",
            "silver_cleaning.py",
            "iceberg_writes.py",
            "silver_quality.py",
        ):
            with self.subTest(filename=filename):
                self.assertFalse((ROOT / "pipeline_utils" / filename).exists())


if __name__ == "__main__":
    unittest.main()
