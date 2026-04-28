from __future__ import annotations

from html.parser import HTMLParser

class DirectoryIndexParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != 'a':
            return

        attrs_dict = dict(attrs)
        href = attrs_dict.get("href")

        if(href):
            self.links.append(href)
