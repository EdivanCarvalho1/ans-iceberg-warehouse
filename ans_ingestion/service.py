from __future__ import annotations

import logging
import posixpath
import shutil
import uuid
from pathlib import Path
from urllib.parse import urljoin, urlparse, urlunparse
from zipfile import ZipFile

from config import IngestionConfig
from contracts import Downloader, FileFilter, FileLister, StorageClient

logger = logging.getLogger(__name__)


class IngestionService:
    def __init__(
        self,
        config: IngestionConfig,
        file_lister: FileLister,
        file_filter: FileFilter,
        downloader: Downloader,
        storage_client: StorageClient,
    ) -> None:
        self.config = config
        self.file_lister= file_lister
        self.file_filter = file_filter
        self.downloader = downloader
        self.storage_client = storage_client

    def run(self) -> None:
        logger.info("Iniciando ingestao")
        logger.info("Origem: %s", self.config.source_url)
        logger.info("Destino: %s", self.config.hdfs_destination_dir)

        self.config.local_tmp_dir.mkdir(parents=True, exist_ok=True)

        files = self.file_lister.list_files()
        allowed_files = self._filter_files(files)
        staging_dir = self._run_sibling_hdfs_path(self.config.hdfs_destination_dir, "staging")
        backup_dir = self._run_sibling_hdfs_path(self.config.hdfs_destination_dir, "backup")

        logger.info("Arquivos encontrados: %s", len(files))
        logger.info("Arquivos apos filtro: %s", len(allowed_files))

        try:
            self.storage_client.delete_path(staging_dir)
            self.storage_client.mkdir(staging_dir)

            for filename in allowed_files:
                self._process_file(filename, staging_dir)

            self._publish_staging_dir(staging_dir, self.config.hdfs_destination_dir, backup_dir)
        except Exception:
            self.storage_client.delete_path(staging_dir)
            raise

        self._cleanup_tmp_dir()

        logger.info("Processo finalizado")

    def _filter_files(self, files: list[str]) -> list[str]:
        allowed_files: list[str] = []

        for filename in files:
            if self.file_filter.is_allowed(filename):
                allowed_files.append(filename)
            else:
                logger.info("Arquivo ignorado: %s", filename)

        return allowed_files

    def _process_file(self, filename: str, hdfs_destination_root: str) -> None:
        source_file_url = urljoin(self.config.source_url, filename)
        local_file_path = self.config.local_tmp_dir / filename
        extract_dir = self.config.local_tmp_dir / local_file_path.stem

        logger.info("----------------------------------------")
        logger.info("Arquivo: %s", filename)

        try:
            logger.info("Baixando arquivo localmente")
            self.downloader.download(source_file_url, local_file_path)

            logger.info("Extraindo arquivo")
            extracted_files = self._extract_zip(local_file_path, extract_dir)

            logger.info("Enviando %s arquivo(s) extraido(s) para o HDFS", len(extracted_files))
            for extracted_file in extracted_files:
                extracted_file = self._normalize_csv_extension(extracted_file)
                relative_parent = extracted_file.relative_to(extract_dir).parent
                hdfs_destination_dir = self._join_hdfs_path(
                    hdfs_destination_root,
                    relative_parent.as_posix(),
                )
                self.storage_client.upload(
                    local_path=extracted_file,
                    destination_dir=hdfs_destination_dir,
                )

            logger.info("Upload concluido")

        finally:
            self._delete_local_file(local_file_path)
            self._delete_local_dir(extract_dir)

    @staticmethod
    def _extract_zip(zip_path: Path, extract_dir: Path) -> list[Path]:
        extract_dir.mkdir(parents=True, exist_ok=True)

        with ZipFile(zip_path) as zip_file:
            for member in zip_file.infolist():
                member_path = Path(member.filename)
                if member_path.is_absolute() or ".." in member_path.parts:
                    raise ValueError(f"Caminho inseguro dentro do ZIP: {member.filename}")

            zip_file.extractall(extract_dir)

        return sorted(path for path in extract_dir.rglob("*") if path.is_file())

    @staticmethod
    def _normalize_csv_extension(file_path: Path) -> Path:
        if file_path.suffix.lower() != ".csv" or file_path.suffix == ".csv":
            return file_path

        normalized_path = file_path.with_suffix(".csv")
        if normalized_path.exists():
            raise ValueError(f"Arquivo CSV duplicado apos normalizacao: {normalized_path.name}")

        file_path.rename(normalized_path)
        return normalized_path

    @staticmethod
    def _delete_local_file(local_file_path: Path) -> None:
        if local_file_path.exists():
            local_file_path.unlink()
            logger.info("Arquivo local removido")

    @staticmethod
    def _delete_local_dir(local_dir_path: Path) -> None:
        if local_dir_path.exists():
            shutil.rmtree(local_dir_path)
            logger.info("Diretorio local removido")

    def _cleanup_tmp_dir(self) -> None:
        try:
            self.config.local_tmp_dir.rmdir()
        except OSError:
            pass

    @staticmethod
    def _join_hdfs_path(base_path: str, relative_path: str) -> str:
        if not relative_path or relative_path == ".":
            return base_path

        return posixpath.join(base_path.rstrip("/"), relative_path)

    def _publish_staging_dir(self, staging_dir: str, destination_dir: str, backup_dir: str) -> None:
        logger.info("Publicando staging no destino final")
        logger.info("Staging: %s", staging_dir)
        logger.info("Destino final: %s", destination_dir)

        self.storage_client.delete_path(backup_dir)

        destination_moved = False
        try:
            if self.storage_client.exists(destination_dir):
                logger.info("Destino existente encontrado; movendo para backup temporario")
                self.storage_client.move(destination_dir, backup_dir)
                destination_moved = True

            logger.info("Movendo staging para destino final")
            self.storage_client.move(staging_dir, destination_dir)
            self.storage_client.delete_path(backup_dir)
            logger.info("Publicacao concluida")
        except Exception:
            logger.exception("Falha ao publicar staging")
            if destination_moved and not self.storage_client.exists(destination_dir):
                logger.info("Restaurando destino original a partir do backup")
                self.storage_client.move(backup_dir, destination_dir)
            raise

    @staticmethod
    def _run_sibling_hdfs_path(destination_dir: str, suffix: str) -> str:
        run_id = uuid.uuid4().hex
        return IngestionService._sibling_hdfs_path(destination_dir, suffix, run_id)

    @staticmethod
    def _sibling_hdfs_path(destination_dir: str, suffix: str, run_id: str) -> str:
        parsed_dir = urlparse(destination_dir)
        path = parsed_dir.path if parsed_dir.scheme else destination_dir
        normalized_path = path.rstrip("/")
        parent_path = posixpath.dirname(normalized_path)
        base_name = posixpath.basename(normalized_path)
        sibling_path = posixpath.join(parent_path, f".{base_name}_{suffix}_{run_id}")

        if not parsed_dir.scheme:
            return sibling_path

        return urlunparse(parsed_dir._replace(path=sibling_path))
