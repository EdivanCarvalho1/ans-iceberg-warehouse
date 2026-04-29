from __future__ import annotations

from config import IngestionConfig
from downloader import HttpFileDownloader
from file_lister import HttpDirectoryFileLister, HttpPeriodDirectoryLister
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
        source_directory_lister = None
        file_lister_factory = None

        if not IngestionFactory._source_url_points_to_period(config.source_url):
            source_directory_lister = HttpPeriodDirectoryLister(
                base_url=config.source_url,
                user_agent=config.user_agent,
                timeout_seconds=config.request_timeout_seconds,
                start_period=config.source_start_period,
                end_period=config.source_end_period,
            )
            file_lister_factory = lambda source_url: HttpDirectoryFileLister(
                base_url=source_url,
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
            max_attempts=config.download_retries,
            retry_backoff_seconds=config.download_retry_backoff_seconds,
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
            source_directory_lister=source_directory_lister,
            file_lister_factory=file_lister_factory,
        )

    @staticmethod
    def _source_url_points_to_period(source_url: str) -> bool:
        last_path_part = source_url.rstrip("/").rsplit("/", 1)[-1]
        return len(last_path_part) == 6 and last_path_part.isdigit()
