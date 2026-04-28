import unittest

from html_parser import DirectoryIndexParser


class DirectoryIndexParserTest(unittest.TestCase):
    def test_directory_index_parser_collects_links(self) -> None:
        parser = DirectoryIndexParser()

        parser.feed(
            """
            <html>
              <body>
                <a href="../">Parent</a>
                <a href="pda-024-icb-SP-2026_02.zip">SP</a>
                <a href="?C=N;O=D">Sort</a>
              </body>
            </html>
            """
        )

        self.assertEqual(
            parser.links,
            [
                "../",
                "pda-024-icb-SP-2026_02.zip",
                "?C=N;O=D",
            ],
        )
