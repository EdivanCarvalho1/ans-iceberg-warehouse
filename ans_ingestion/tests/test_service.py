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


class StaticDirectoryLister:
    def __init__(self, directories: list[str]) -> None:
        self.directories = directories

    def list_directories(self) -> list[str]:
        return self.directories


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


def make_config(tmp_path: Path, destination_dir: str = "hdfs://localhost:9000/dados/raw/ans/") -> IngestionConfig:
    return IngestionConfig(
        source_url="https://example.com/source/",
        source_start_period=None,
        source_end_period=None,
        hdfs_base_uri="hdfs://localhost:9000",
        hdfs_web_url="http://localhost:9870",
        hdfs_user=None,
        hdfs_destination_dir=destination_dir,
        local_tmp_dir=tmp_path / "work",
    )


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
                config=make_config(tmp_path),
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
                config=make_config(tmp_path),
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
                config=make_config(tmp_path, destination_dir),
                file_lister=StaticFileLister(["pda-024-icb-SP-2026_02.zip"]),
                file_filter=AllowAllFilter(),
                downloader=FailingDownloader(),
                storage_client=storage_client,
            )

            with self.assertRaisesRegex(RuntimeError, "download failed"):
                service.run()

            self.assertTrue(storage_client.exists(destination_dir))
            self.assertEqual(storage_client.moved, [])

    def test_run_processes_latest_period_directory_into_partitioned_destination(self) -> None:
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            zip_path = tmp_path / "input.zip"
            with ZipFile(zip_path, "w") as zip_file:
                zip_file.writestr("pda-024-icb-SP-2026_02.csv", "header\nvalue\n")

            storage_client = MemoryStorageClient()
            service = IngestionService(
                config=make_config(tmp_path),
                file_lister=StaticFileLister([]),
                file_filter=AllowAllFilter(),
                downloader=CopyingDownloader(zip_path),
                storage_client=storage_client,
                source_directory_lister=StaticDirectoryLister(["202601", "202602"]),
                file_lister_factory=lambda source_url: StaticFileLister(
                    [f"pda-024-icb-SP-{source_url.rstrip('/')[-4:]}_02.zip"]
                ),
            )

            service.run()

            published_destinations = {destination for _, destination in storage_client.moved}
            self.assertNotIn("hdfs://localhost:9000/dados/raw/ans/202601", published_destinations)
            self.assertIn("hdfs://localhost:9000/dados/raw/ans/202602", published_destinations)
            self.assertEqual(len(storage_client.uploaded), 1)

    def test_run_processes_only_latest_period(self) -> None:
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            zip_path = tmp_path / "input.zip"
            with ZipFile(zip_path, "w") as zip_file:
                zip_file.writestr("pda-024-icb-SP-2026_02.csv", "header\nvalue\n")

            storage_client = MemoryStorageClient()
            service = IngestionService(
                config=make_config(tmp_path),
                file_lister=StaticFileLister([]),
                file_filter=AllowAllFilter(),
                downloader=CopyingDownloader(zip_path),
                storage_client=storage_client,
                source_directory_lister=StaticDirectoryLister(
                    [
                        "202501",
                        "202502",
                        "202503",
                        "202504",
                        "202505",
                        "202506",
                        "202507",
                        "202508",
                        "202509",
                        "202510",
                        "202511",
                        "202512",
                        "202601",
                        "202602",
                        "202603",
                        "202604",
                        "202605",
                        "202606",
                        "202607",
                        "202608",
                        "202609",
                        "202610",
                        "202611",
                        "202612",
                        "202701",
                        "202702",
                        "202703",
                        "202704",
                    ]
                ),
                file_lister_factory=lambda source_url: StaticFileLister(
                    ["pda-024-icb-SP-2026_02.zip"]
                ),
            )

            service.run()

            published_destinations = {destination for _, destination in storage_client.moved}
            self.assertNotIn("hdfs://localhost:9000/dados/raw/ans/202501", published_destinations)
            self.assertNotIn("hdfs://localhost:9000/dados/raw/ans/202502", published_destinations)
            self.assertNotIn("hdfs://localhost:9000/dados/raw/ans/202503", published_destinations)
            self.assertNotIn("hdfs://localhost:9000/dados/raw/ans/202504", published_destinations)
            self.assertNotIn("hdfs://localhost:9000/dados/raw/ans/202505", published_destinations)
            self.assertIn("hdfs://localhost:9000/dados/raw/ans/202704", published_destinations)
            self.assertEqual(len(storage_client.uploaded), 1)

    def test_run_skips_period_when_destination_already_exists(self) -> None:
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            zip_path = tmp_path / "input.zip"
            with ZipFile(zip_path, "w") as zip_file:
                zip_file.writestr("pda-024-icb-SP-2026_02.csv", "header\nvalue\n")

            storage_client = MemoryStorageClient()
            storage_client.paths.add("hdfs://localhost:9000/dados/raw/ans/202601")

            service = IngestionService(
                config=make_config(tmp_path),
                file_lister=StaticFileLister([]),
                file_filter=AllowAllFilter(),
                downloader=CopyingDownloader(zip_path),
                storage_client=storage_client,
                source_directory_lister=StaticDirectoryLister(["202601", "202602"]),
                file_lister_factory=lambda source_url: StaticFileLister(["pda-024-icb-SP-2026_02.zip"]),
            )

            service.run()

            published_destinations = {destination for _, destination in storage_client.moved}
            self.assertNotIn("hdfs://localhost:9000/dados/raw/ans/202601", published_destinations)
            self.assertIn("hdfs://localhost:9000/dados/raw/ans/202602", published_destinations)
            self.assertEqual(len(storage_client.uploaded), 1)
