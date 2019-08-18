import os
from ..utils import remove_recursive_silently


from zero.worker import Worker
from zero.cache import Cache
from zero.path_converter import PathConverter
from zero.inode_store import InodeStore
from zero.rank_store import RankStore
from zero.ranker import Ranker
from zero.metadata_store import MetaData


CACHE_DIR = "test_cache_dir/"
DB_PATH = "state.db"
CACHE_SIZE = 0.01  # GB


class IntegrationTestingContext:

    def __init__(self, api):
        self._reset()
        self.api = api
        os.mkdir(CACHE_DIR)
        self.converter = PathConverter(CACHE_DIR)
        self.inode_store = InodeStore(DB_PATH)
        self.rank_store = RankStore(DB_PATH)
        self.metadata_store = MetaData(DB_PATH)
        self.ranker = Ranker(self.rank_store, self.inode_store)
        self.cache = Cache(
            converter=self.converter,
            inode_store=self.inode_store,
            metadata_store=self.metadata_store,
            api=self.api,
        )
        self.worker = Worker(self.cache, self.ranker, api, CACHE_SIZE)

    def create_file(self, path, content):
        self.cache.create(path, 33204)
        with open(self.converter.to_cache_path(path), "w+b") as file:
            file.write(content)
        return path

    def tear_down(self):
        self._reset()

    def _reset(self):
        remove_recursive_silently(DB_PATH)
        remove_recursive_silently(CACHE_DIR)
