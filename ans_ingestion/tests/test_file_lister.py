import unittest

from file_lister import HttpDirectoryFileLister, HttpPeriodDirectoryLister


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


class StaticHtmlPeriodLister(HttpPeriodDirectoryLister):
    def __init__(self, html: str, start_period: str | None = None, end_period: str | None = None) -> None:
        super().__init__(
            base_url="https://example.com/source/",
            user_agent="test",
            timeout_seconds=1,
            start_period=start_period,
            end_period=end_period,
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

    def test_list_directories_returns_periods_in_configured_range(self) -> None:
        lister = StaticHtmlPeriodLister(
            """
            <html>
              <body>
                <a href="../">Parent Directory</a>
                <a href="201903/">201903</a>
                <a href="201904/">201904</a>
                <a href="202602/">202602</a>
                <a href="202603/">202603</a>
                <a href="not-a-period/">not-a-period</a>
              </body>
            </html>
            """,
            start_period="201904",
            end_period="202602",
        )

        self.assertEqual(lister.list_directories(), ["201904", "202602"])


if __name__ == "__main__":
    unittest.main()
