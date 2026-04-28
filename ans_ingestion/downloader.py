from __future__ import annotations

import shutil
from pathlib import Path
from urllib.request import Request, urlopen


class HttpFileDownloader:
    def __init__(self, user_agent: str, chunk_size_bytes: int, timeout_seconds: int) -> None:
        self.user_agent = user_agent
        self.chunk_size_bytes = chunk_size_bytes
        self.timeout_seconds = timeout_seconds

    def download(self, source_url: str, destination_path: Path) -> None:
        request = Request(
            source_url,
            headers={"User-Agent": self.user_agent}
        )

        destination_path.parent.mkdir(parents=True, exist_ok=True)

        with urlopen(request, timeout=self.timeout_seconds) as response, open(destination_path, "wb") as file:
            shutil.copyfileobj(
                response,
                file,
                length=self.chunk_size_bytes
            )
