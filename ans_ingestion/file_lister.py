from __future__ import annotations

import os
from urllib.parse import unquote, urlparse
from urllib.request import Request, urlopen

from html_parser import DirectoryIndexParser

class HttpDirectoryFileLister:

    def __init__(self, base_url: str, user_agent: str, timeout_seconds: int) -> None:
        self.base_url = base_url
        self.user_agent = user_agent
        self.timeout_seconds = timeout_seconds

    def list_files(self) -> list[str]:
        html = self._fetch_html()
        parser = DirectoryIndexParser()
        parser.feed(html)

        files: list[str] = []

        for link in parser.links:
            filename = self._filename_from_link(link)
            if filename is None:
                continue

            files.append(filename)

        return sorted(set(files))

    def _fetch_html(self) -> str:
        request = Request(self.base_url, headers={"User-Agent": self.user_agent})

        with urlopen(request, timeout=self.timeout_seconds) as response:
            return response.read().decode("utf-8", errors="ignore")

    @staticmethod
    def _is_directory_or_parent(link: str) -> bool:
        return link in ("../", "/") or link.endswith("/")

    @staticmethod
    def _filename_from_link(link: str) -> str | None:
        decoded_link = unquote(link)
        parsed_link = urlparse(decoded_link)
        path = parsed_link.path

        if not path or HttpDirectoryFileLister._is_directory_or_parent(path):
            return None

        filename = os.path.basename(path)
        return filename or None
