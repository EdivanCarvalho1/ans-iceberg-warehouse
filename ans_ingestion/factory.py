from __future__ import annotations

from config import IngestionConfig
from downloader import HttpFileDownloader
from file_lister import HttpDirectoryFileLister
from filters import CompositeFileFilter, ExcludeTokenFileFilter, FileExtensionFilter
from hdfs_client import HdfsClient
from service import IngestionService

class IngestionFactory():
    @staticmethod
    def create() -> IngestionService:
        config = IngestionConfig.from_env()

        file_lister = HttpDirectoryFileLister(
            base_url=config.source_url,
            user_agent=config.user_agent,
            timeout_seconds=config.request_timeout_seconds,
        )

        file_filter = CompositeFileFilter([
            FileExtensionFilter(".zip"),
            ExcludeTokenFileFilter("XX"),
        ])

        downloader = HttpFileDownloader(
            user_agent=config.user_agent,
            chunk_size_bytes=config.chunk_size_bytes,
            timeout_seconds=config.request_timeout_seconds,
        )

        storage_client = HdfsClient(
            web_url=config.hdfs_web_url,
            base_uri=config.hdfs_base_uri,
            user=config.hdfs_user,
        )

        return IngestionService(
            config=config,
            file_lister=file_lister,
            file_filter=file_filter,
            downloader=downloader,
            storage_client=storage_client,
        )
