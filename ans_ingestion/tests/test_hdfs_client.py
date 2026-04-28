import unittest

from hdfs_client import HdfsClient


class HdfsClientTest(unittest.TestCase):
    def test_path_from_hdfs_uri_returns_only_path(self) -> None:
        self.assertEqual(
            HdfsClient._path_from_uri("hdfs://localhost:9000/dados/raw/ans/"),
            "/dados/raw/ans/",
        )

    def test_path_from_hdfs_uri_rejects_non_hdfs_scheme(self) -> None:
        with self.assertRaisesRegex(ValueError, "URI HDFS invalida"):
            HdfsClient._path_from_uri("http://localhost:9870/dados/raw/ans/")
