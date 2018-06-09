import unittest
import os
from io import BytesIO
from unittest.mock import MagicMock


from zero.worker import Worker
from zero.cache import Cache
from zero.paths import PathConverter

from .utils import remove_recursive_silently

CACHE_DIR = "test_cache_dir/"
PATH = "yo"
FILE_CONTENT = b"Some file content"


class WorkerTest(unittest.TestCase):

    def setUp(self):
        # Todo mock this and pass fake data here (?)
        self._reset()
        os.mkdir(CACHE_DIR)
        self.converter = PathConverter(CACHE_DIR)
        self.cache = Cache(self.converter)
        self.api = MagicMock()
        self.worker = Worker(self.cache, self.api)
        self._create_file()

    def tearDown(self):
        self._reset()

    def test_clean_path(self):
        self.worker._clean_path(PATH)

    def test_create_dummy(self):
        self.worker._clean_path(PATH)
        cache_path = self.converter.to_cache_path(PATH)
        assert os.path.exists(cache_path)
        self.worker._create_dummy(PATH)
        assert not os.path.exists(cache_path)
        assert os.path.exists(self.converter.add_dummy_ending(cache_path))

    def test_replace_dummy(self):
        self.worker._clean_path(PATH)
        self.worker._create_dummy(PATH)
        self.worker.api.download.return_value = BytesIO(FILE_CONTENT)
        self.worker._replace_dummy(PATH)
        cache_path = self.converter.to_cache_path(PATH)
        assert not os.path.exists(self.converter.add_dummy_ending(cache_path))
        assert os.path.exists(cache_path)

    def _reset(self):
        remove_recursive_silently("state.db")
        remove_recursive_silently(CACHE_DIR)

    def _create_file(self):
        self.cache.create(PATH, 33204)
        with open(self.converter.to_cache_path(PATH), "w+b") as file:
            file.write(FILE_CONTENT)
