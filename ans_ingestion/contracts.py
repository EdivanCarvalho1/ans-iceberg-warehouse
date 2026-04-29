from __future__ import annotations

from pathlib import Path
from typing import Protocol


class FileLister(Protocol):
    def list_files(self) -> list[str]:
        ...


class SourceDirectoryLister(Protocol):
    def list_directories(self) -> list[str]:
        ...


class FileFilter(Protocol):
    def is_allowed(self, filename: str) -> bool:
        ...


class Downloader(Protocol):
    def download(self, source_url: str, destination_path: Path) -> None:
        ...


class StorageClient(Protocol):
    def mkdir(self, path: str) -> None:
        ...

    def delete_path(self, path: str) -> None:
        ...

    def exists(self, path: str) -> bool:
        ...

    def move(self, source_path: str, destination_path: str) -> None:
        ...

    def upload(self, local_path: Path, destination_dir: str) -> None:
        ...
