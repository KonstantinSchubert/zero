import unittest
import os
from io import BytesIO
from unittest.mock import MagicMock


from zero.worker import Worker
from zero.cache import Cache
from zero.path_converter import PathConverter
from zero.state_store import StateStore
from zero.inode_store import InodeStore
from zero.rank_store import RankStore
from zero.ranker import Ranker

from .utils import remove_recursive_silently

CACHE_DIR = "test_cache_dir/"
PATH = "yo"
FILE_CONTENT = b"Some file content"
DB_PATH = "state.db"


class WorkerTest(unittest.TestCase):

    def setUp(self):
        # Todo mock this and pass fake data here (?)
        self._reset()
        os.mkdir(CACHE_DIR)
        self.converter = PathConverter(CACHE_DIR)
        state_store = StateStore(DB_PATH)
        self.inode_store = InodeStore(DB_PATH)
        rank_store = RankStore(DB_PATH)
        ranker = Ranker(rank_store, self.inode_store)
        self.api = MagicMock()
        self.cache = Cache(
            self.converter, state_store, self.inode_store, ranker, self.api
        )
        self.worker = Worker(self.cache, self.api)
        self.inode = self._create_file()

    def tearDown(self):
        self._reset()

    def test_clean_path(self):
        self.worker._clean_inode(self.inode)

    def test_create_dummy(self):
        self.worker._clean_inode(self.inode)
        cache_path = self.converter.to_cache_path(PATH)
        assert os.path.exists(cache_path)
        self.cache.create_dummy(self.inode)
        assert not os.path.exists(cache_path)
        assert os.path.exists(self.converter.add_dummy_ending(cache_path))

    def test_replace_dummy(self):
        self.worker._clean_inode(self.inode)
        self.cache.create_dummy(self.inode)
        self.worker.api.download.return_value = BytesIO(FILE_CONTENT)
        self.cache.replace_dummy(self.inode)
        cache_path = self.converter.to_cache_path(PATH)
        assert not os.path.exists(self.converter.add_dummy_ending(cache_path))
        assert os.path.exists(cache_path)

    def _reset(self):
        remove_recursive_silently(DB_PATH)
        remove_recursive_silently(CACHE_DIR)

    def _create_file(self):
        self.cache.create(PATH, 33204)
        with open(self.converter.to_cache_path(PATH), "w+b") as file:
            file.write(FILE_CONTENT)
        return self.inode_store.get_inode(PATH)
