from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse, urlunparse

@dataclass(frozen=True)
class IngestionConfig:
    source_url: str
    hdfs_base_uri: str | None
    hdfs_web_url: str
    hdfs_user: str | None
    hdfs_destination_dir: str
    local_tmp_dir: Path
    user_agent: str = "Mozilla/5.0"
    chunk_size_bytes: int = 8 * 1024 * 1024
    request_timeout_seconds: int = 60

    @staticmethod
    def from_env() -> "IngestionConfig":
        hdfs_base_uri = os.getenv("HDFS_BASE_URI")
        hdfs_web_url = os.getenv("HDFS_WEB_URL")
        hdfs_user = os.getenv("HDFS_USER")

        if hdfs_web_url is None:
            if hdfs_base_uri is None:
                raise ValueError("Defina HDFS_WEB_URL ou HDFS_BASE_URI para conectar ao WebHDFS.")

            hdfs_web_url = IngestionConfig._default_web_url_from_base_uri(hdfs_base_uri)

        source_url = os.getenv(
            "ANS_SOURCE_URL",
            "https://dadosabertos.ans.gov.br/FTP/PDA/"
            "informacoes_consolidadas_de_beneficiarios-024/202602/"
        )

        hdfs_destination_dir = os.getenv("ANS_HDFS_DIR")
        if hdfs_destination_dir is None:
            if hdfs_base_uri is None:
                raise ValueError("Defina HDFS_BASE_URI ou ANS_HDFS_DIR para o destino no HDFS.")

            hdfs_destination_dir = f"{hdfs_base_uri.rstrip('/')}/dados/raw/ans/"

        local_tmp_dir = Path(os.getenv("ANS_LOCAL_TMP_DIR", "/tmp/ans"))
        request_timeout_seconds = int(os.getenv("ANS_REQUEST_TIMEOUT_SECONDS", "60"))

        return IngestionConfig(
            source_url=source_url,
            hdfs_base_uri=hdfs_base_uri,
            hdfs_web_url=hdfs_web_url,
            hdfs_user=hdfs_user,
            hdfs_destination_dir=hdfs_destination_dir,
            local_tmp_dir=local_tmp_dir,
            request_timeout_seconds=request_timeout_seconds,
        )

    @staticmethod
    def _default_web_url_from_base_uri(hdfs_base_uri: str) -> str:
        parsed_uri = urlparse(hdfs_base_uri)

        if parsed_uri.scheme != "hdfs" or not parsed_uri.hostname:
            raise ValueError(f"HDFS_BASE_URI invalida: {hdfs_base_uri}")

        return urlunparse(("http", f"{parsed_uri.hostname}:9870", "", "", "", ""))
