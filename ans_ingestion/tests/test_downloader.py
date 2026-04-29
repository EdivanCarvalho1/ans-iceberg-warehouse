from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
from urllib.error import HTTPError, URLError

from downloader import HttpFileDownloader


class HttpFileDownloaderTest(unittest.TestCase):
    def test_download_retries_transient_error_and_removes_partial_file(self) -> None:
        with TemporaryDirectory() as tmp:
            destination_path = Path(tmp) / "input.zip"
            downloader = HttpFileDownloader(
                user_agent="test",
                chunk_size_bytes=1024,
                timeout_seconds=1,
                max_attempts=2,
                retry_backoff_seconds=0,
            )

            attempts = 0

            def download_once(*args: object) -> None:
                nonlocal attempts
                attempts += 1
                if attempts == 1:
                    destination_path.write_bytes(b"partial")
                    raise URLError(ConnectionResetError("reset"))
                destination_path.write_bytes(b"complete")

            with (
                patch.object(
                    downloader,
                    "_download_once",
                    side_effect=download_once,
                ) as download_once_mock,
                patch("downloader.logger.warning"),
                patch("downloader.time.sleep") as sleep,
            ):
                downloader.download("https://example.com/input.zip", destination_path)

            self.assertEqual(download_once_mock.call_count, 2)
            sleep.assert_called_once_with(0)
            self.assertEqual(destination_path.read_bytes(), b"complete")

    def test_download_does_not_retry_non_retryable_http_error(self) -> None:
        with TemporaryDirectory() as tmp:
            destination_path = Path(tmp) / "input.zip"
            downloader = HttpFileDownloader(
                user_agent="test",
                chunk_size_bytes=1024,
                timeout_seconds=1,
                max_attempts=3,
                retry_backoff_seconds=0,
            )
            error = HTTPError(
                url="https://example.com/input.zip",
                code=404,
                msg="not found",
                hdrs=None,
                fp=None,
            )

            with patch.object(
                downloader,
                "_download_once",
                side_effect=error,
            ) as download_once:
                with self.assertRaises(HTTPError):
                    downloader.download("https://example.com/input.zip", destination_path)

            self.assertEqual(download_once.call_count, 1)


if __name__ == "__main__":
    unittest.main()
