import unittest

from filters import CompositeFileFilter, ExcludeTokenFileFilter, FileExtensionFilter


class FileFilterTest(unittest.TestCase):
    def test_composite_filter_accepts_only_expected_zip_files(self) -> None:
        file_filter = CompositeFileFilter([
            FileExtensionFilter(".zip"),
            ExcludeTokenFileFilter("XX"),
        ])

        self.assertTrue(file_filter.is_allowed("pda-024-icb-SP-2026_02.zip"))
        self.assertFalse(file_filter.is_allowed("?C=N;O=D"))
        self.assertFalse(file_filter.is_allowed("pda-024-icb-XX-2026_02.zip"))
        self.assertFalse(file_filter.is_allowed("pda-024-icb-SP-2026_02.csv"))
