import unittest

from file_lister import HttpDirectoryFileLister


class StaticHtmlFileLister(HttpDirectoryFileLister):
    def __init__(self, html: str) -> None:
        super().__init__(
            base_url="https://example.com/source/",
            user_agent="test",
            timeout_seconds=1,
        )
        self.html = html

    def _fetch_html(self) -> str:
        return self.html


class HttpDirectoryFileListerTest(unittest.TestCase):
    def test_list_files_ignores_directory_index_sort_links(self) -> None:
        lister = StaticHtmlFileLister(
            """
            <html>
              <body>
                <a href="?C=D;O=A">Last modified</a>
                <a href="?C=M;O=A">Size</a>
                <a href="?C=N;O=D">Name</a>
                <a href="?C=S;O=A">Description</a>
                <a href="../">Parent Directory</a>
                <a href="pda-024-icb-SP-2026_02.zip">SP</a>
              </body>
            </html>
            """
        )

        self.assertEqual(lister.list_files(), ["pda-024-icb-SP-2026_02.zip"])


if __name__ == "__main__":
    unittest.main()
