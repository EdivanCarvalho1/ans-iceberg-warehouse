from __future__ import annotations

from contracts import FileFilter


class FileExtensionFilter:
    def __init__(self, extension: str) -> None:
        self.extension = extension.lower()

    def is_allowed(self, filename: str) -> bool:
        return filename.lower().endswith(self.extension)


class ExcludeTokenFileFilter:
    def __init__(self, token: str) -> None:
        self.token = token.upper()

    def is_allowed(self, filename: str) -> bool:
        return self.token not in filename.upper()

class CompositeFileFilter:
    def __init__(self, filters: list[FileFilter]) -> None:
        self.filters = filters

    def is_allowed(self, filename: str) -> bool:
        return all(file_filter.is_allowed(filename) for file_filter in self.filters)
