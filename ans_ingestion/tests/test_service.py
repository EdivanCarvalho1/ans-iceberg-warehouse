from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from zipfile import ZipFile

from config import IngestionConfig
from service import IngestionService


class AllowAllFilter:
    def is_allowed(self, filename: str) -> bool:
        return True


class StaticFileLister:
    def __init__(self, files: list[str]) -> None:
        self.files = files

    def list_files(self) -> list[str]:
        return self.files


class CopyingDownloader:
    def __init__(self, source_zip: Path) -> None:
        self.source_zip = source_zip

    def download(self, source_url: str, destination_path: Path) -> None:
        destination_path.write_bytes(self.source_zip.read_bytes())


class FailingDownloader:
    def download(self, source_url: str, destination_path: Path) -> None:
        raise RuntimeError("download failed")


class MemoryStorageClient:
    def __init__(self) -> None:
        self.paths: set[str] = set()
        self.uploaded: list[tuple[str, str]] = []
        self.deleted: list[str] = []
        self.moved: list[tuple[str, str]] = []

    def mkdir(self, path: str) -> None:
        self.paths.add(path)

    def delete_path(self, path: str) -> None:
        self.deleted.append(path)
        self.paths.discard(path)

    def exists(self, path: str) -> bool:
        return path in self.paths

    def move(self, source_path: str, destination_path: str) -> None:
        self.moved.append((source_path, destination_path))
        if source_path not in self.paths:
            raise FileNotFoundError(source_path)
        self.paths.remove(source_path)
        self.paths.add(destination_path)

    def upload(self, local_path: Path, destination_dir: str) -> None:
        self.uploaded.append((local_path.name, destination_dir))


class IngestionServiceTest(unittest.TestCase):
    def test_extract_zip_rejects_path_traversal(self) -> None:
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            zip_path = tmp_path / "unsafe.zip"
            extract_dir = tmp_path / "extract"

            with ZipFile(zip_path, "w") as zip_file:
                zip_file.writestr("../unsafe.csv", "bad")

            with self.assertRaisesRegex(ValueError, "Caminho inseguro"):
                IngestionService._extract_zip(zip_path, extract_dir)

    def test_run_normalizes_extracted_csv_extension_before_upload(self) -> None:
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            zip_path = tmp_path / "input.zip"
            with ZipFile(zip_path, "w") as zip_file:
                zip_file.writestr("pda-024-icb-SP-2026_02.CSV", "header\nvalue\n")

            storage_client = MemoryStorageClient()

            service = IngestionService(
                config=IngestionConfig(
                    source_url="https://example.com/source/",
                    hdfs_base_uri="hdfs://localhost:9000",
                    hdfs_web_url="http://localhost:9870",
                    hdfs_user=None,
                    hdfs_destination_dir="hdfs://localhost:9000/dados/raw/ans/",
                    local_tmp_dir=tmp_path / "work",
                ),
                file_lister=StaticFileLister(["pda-024-icb-SP-2026_02.zip"]),
                file_filter=AllowAllFilter(),
                downloader=CopyingDownloader(zip_path),
                storage_client=storage_client,
            )

            service.run()

            self.assertEqual(storage_client.uploaded[0][0], "pda-024-icb-SP-2026_02.csv")

    def test_run_uploads_extracted_files_to_staging_before_publishing(self) -> None:
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            zip_path = tmp_path / "input.zip"
            with ZipFile(zip_path, "w") as zip_file:
                zip_file.writestr("pda-024-icb-SP-2026_02.csv", "header\nvalue\n")

            storage_client = MemoryStorageClient()
            storage_client.paths.add("hdfs://localhost:9000/dados/raw/ans/")

            service = IngestionService(
                config=IngestionConfig(
                    source_url="https://example.com/source/",
                    hdfs_base_uri="hdfs://localhost:9000",
                    hdfs_web_url="http://localhost:9870",
                    hdfs_user=None,
                    hdfs_destination_dir="hdfs://localhost:9000/dados/raw/ans/",
                    local_tmp_dir=tmp_path / "work",
                ),
                file_lister=StaticFileLister(["pda-024-icb-SP-2026_02.zip"]),
                file_filter=AllowAllFilter(),
                downloader=CopyingDownloader(zip_path),
                storage_client=storage_client,
            )

            service.run()

            self.assertEqual(len(storage_client.uploaded), 1)
            self.assertEqual(storage_client.uploaded[0][0], "pda-024-icb-SP-2026_02.csv")
            self.assertIn("_staging_", storage_client.uploaded[0][1])
            self.assertTrue(
                any(
                    destination == "hdfs://localhost:9000/dados/raw/ans/"
                    for _, destination in storage_client.moved
                )
            )
            self.assertTrue(storage_client.exists("hdfs://localhost:9000/dados/raw/ans/"))

    def test_run_does_not_move_destination_when_download_fails(self) -> None:
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            storage_client = MemoryStorageClient()
            destination_dir = "hdfs://localhost:9000/dados/raw/ans/"
            storage_client.paths.add(destination_dir)

            service = IngestionService(
                config=IngestionConfig(
                    source_url="https://example.com/source/",
                    hdfs_base_uri="hdfs://localhost:9000",
                    hdfs_web_url="http://localhost:9870",
                    hdfs_user=None,
                    hdfs_destination_dir=destination_dir,
                    local_tmp_dir=tmp_path / "work",
                ),
                file_lister=StaticFileLister(["pda-024-icb-SP-2026_02.zip"]),
                file_filter=AllowAllFilter(),
                downloader=FailingDownloader(),
                storage_client=storage_client,
            )

            with self.assertRaisesRegex(RuntimeError, "download failed"):
                service.run()

            self.assertTrue(storage_client.exists(destination_dir))
            self.assertEqual(storage_client.moved, [])
