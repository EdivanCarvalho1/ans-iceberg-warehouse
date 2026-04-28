from __future__ import annotations

import posixpath
from pathlib import Path
from urllib.parse import urlparse


class HdfsClient:
    def __init__(self, web_url: str, base_uri: str | None = None, user: str | None = None) -> None:
        self.client = self._create_client(web_url, user)
        self.default_path_prefix = ""

        if base_uri:
            self.default_path_prefix = self._path_from_uri(base_uri)

    def mkdir(self, path: str) -> None:
        self.client.makedirs(self._hdfs_path(path))

    def delete_path(self, path: str) -> None:
        self.client.delete(self._hdfs_path(path), recursive=True)

    def exists(self, path: str) -> bool:
        return self.client.status(self._hdfs_path(path), strict=False) is not None

    def upload(self, local_path: Path, destination_dir: str) -> None:
        hdfs_destination_dir = self._hdfs_path(destination_dir)
        self.client.makedirs(hdfs_destination_dir)

        destination_path = posixpath.join(hdfs_destination_dir.rstrip("/"), local_path.name)
        self.client.upload(destination_path, str(local_path), overwrite=True)

    def move(self, source_path: str, destination_path: str) -> None:
        self.client.rename(self._hdfs_path(source_path), self._hdfs_path(destination_path))

    def _hdfs_path(self, path: str) -> str:
        parsed_path = urlparse(path)

        if parsed_path.scheme:
            return self._path_from_uri(path)

        if not self.default_path_prefix:
            raise ValueError(
                "Caminho HDFS sem URI completa. Use hdfs://host:porta/caminho "
                "ou defina HDFS_BASE_URI."
            )

        return posixpath.join(self.default_path_prefix, path.lstrip("/"))

    @staticmethod
    def _path_from_uri(uri: str) -> str:
        parsed_uri = urlparse(uri)

        if parsed_uri.scheme != "hdfs":
            raise ValueError(f"URI HDFS invalida: {uri}")

        if not parsed_uri.hostname:
            raise ValueError(f"URI HDFS sem host: {uri}")

        return parsed_uri.path or "/"

    @staticmethod
    def _create_client(web_url: str, user: str | None):
        try:
            from hdfs import InsecureClient
        except ImportError as exc:
            raise RuntimeError(
                "A biblioteca Python `hdfs` nao esta instalada. Instale com `pip install hdfs`."
            ) from exc

        return InsecureClient(web_url, user=user)
