import unittest
import os
from io import BytesIO
from unittest.mock import MagicMock


from zero.worker import Worker


from .configure import IntegrationTestingContext

CACHE_DIR = "test_cache_dir/"
DB_PATH = "state.db"
PATH = "yo"
FILE_CONTENT = b"Some file content"
CACHE_SIZE = 0.01  # GB


class WorkerTest(unittest.TestCase):

    def setUp(self):
        # Todo mock this and pass fake data here (?)
        self.api = MagicMock()
        self.context = IntegrationTestingContext(self.api)
        self.worker = Worker(
            cache=self.context.cache,
            api=self.api,
            ranker=self.context.ranker,
            target_disk_usage=CACHE_SIZE,
        )
        file_path = self.context.create_file(PATH, FILE_CONTENT)
        self.inode = self.context.inode_store.get_inode(file_path)
        self.converter = self.context.converter

    def tearDown(self):
        self.context.tear_down()

    def test_clean_path(self):
        self.worker._clean_inode(self.inode)

    def test_create_dummy(self):
        self.worker._clean_inode(self.inode)
        cache_path = self.converter.to_cache_path(PATH)
        assert os.path.exists(cache_path)
        self.context.cache.create_dummy(self.inode)
        assert not os.path.exists(cache_path)
        assert os.path.exists(self.converter.add_dummy_ending(cache_path))

    def test_replace_dummy(self):
        self.worker._clean_inode(self.inode)
        self.context.cache.create_dummy(self.inode)
        self.worker.api.download.return_value = BytesIO(FILE_CONTENT)
        self.context.cache.replace_dummy(self.inode)
        cache_path = self.converter.to_cache_path(PATH)
        assert not os.path.exists(self.converter.add_dummy_ending(cache_path))
        assert os.path.exists(cache_path)
