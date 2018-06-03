import unittest
import os
import shutil
from threading import Lock
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
        self.reset()
        os.mkdir(CACHE_DIR)
        self.converter = PathConverter(CACHE_DIR)
        self.cache = Cache(self.converter)
        self.worker = Worker(self.cache)
        self.worker.api = MagicMock()
        self.cache.create(PATH, 33204)
        with open(self.converter.to_cache_path(PATH), "w+b") as file:
            file.write(FILE_CONTENT)

    def tearDown(self):
        self.reset()

    def reset(self):
        remove_recursive_silently("state.db")
        remove_recursive_silently(CACHE_DIR)

    def test_clean_file(self):
        self.worker._clean_path(PATH)

    def test_create_dummy(self):
        cache_path = self.converter.to_cache_path(PATH)
        assert os.path.exists(cache_path)
        self.worker._create_dummy(PATH)
        assert not os.path.exists(cache_path)
        assert os.path.exists(self.converter.add_dummy_ending(cache_path))

    def test_replace_dummy(self):
        self.worker._create_dummy(PATH)
        self.worker.api.download.return_value = BytesIO(FILE_CONTENT)
        self.worker.replace_dummy(PATH)
        cache_path = self.converter.to_cache_path(PATH)
        assert not os.path.exists(self.converter.add_dummy_ending(cache_path))
        assert os.path.exists(cache_path)
