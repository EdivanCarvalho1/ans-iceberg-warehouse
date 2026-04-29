from __future__ import annotations

import logging
import shutil
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)


class HttpFileDownloader:
    def __init__(
        self,
        user_agent: str,
        chunk_size_bytes: int,
        timeout_seconds: int,
        max_attempts: int = 3,
        retry_backoff_seconds: float = 5.0,
    ) -> None:
        self.user_agent = user_agent
        self.chunk_size_bytes = chunk_size_bytes
        self.timeout_seconds = timeout_seconds
        self.max_attempts = max(1, max_attempts)
        self.retry_backoff_seconds = max(0.0, retry_backoff_seconds)

    def download(self, source_url: str, destination_path: Path) -> None:
        request = Request(
            source_url,
            headers={"User-Agent": self.user_agent}
        )

        destination_path.parent.mkdir(parents=True, exist_ok=True)

        last_error: Exception | None = None
        for attempt in range(1, self.max_attempts + 1):
            try:
                self._download_once(request, destination_path)
                return
            except (ConnectionError, TimeoutError, OSError, URLError) as error:
                if isinstance(error, HTTPError) and not self._is_retryable_http_error(error):
                    raise

                last_error = error
                self._delete_partial_file(destination_path)

                if attempt == self.max_attempts:
                    break

                logger.warning(
                    "Falha no download de %s na tentativa %s/%s: %s. Tentando novamente.",
                    source_url,
                    attempt,
                    self.max_attempts,
                    error,
                )
                time.sleep(self.retry_backoff_seconds * attempt)

        if last_error is not None:
            raise last_error

    def _download_once(self, request: Request, destination_path: Path) -> None:
        with (
            urlopen(request, timeout=self.timeout_seconds) as response,
            open(destination_path, "wb") as file,
        ):
            shutil.copyfileobj(
                response,
                file,
                length=self.chunk_size_bytes
            )

    @staticmethod
    def _is_retryable_http_error(error: HTTPError) -> bool:
        return error.code in {408, 429, 500, 502, 503, 504}

    @staticmethod
    def _delete_partial_file(destination_path: Path) -> None:
        if destination_path.exists():
            destination_path.unlink()
