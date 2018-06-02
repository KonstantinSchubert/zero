import unittest
import os
import shutil
from unittest.mock import MagicMock

from zero.worker import Worker
from zero.cache import Cache
from zero.paths import PathConverter

from .utils import remove_recursive_silently

CACHE_DIR = "test_cache_dir/"
SIMPLE_PATH = "yo"


class WorkerTest(unittest.TestCase):

    def setUp(self):
        # Todo mock this and pass fake data here (?)
        api = MagicMock()
        self.reset()
        os.mkdir(CACHE_DIR)
        converter = PathConverter(CACHE_DIR)
        self.worker = Worker(converter, api)
        self.cache = Cache(converter, self.worker)
        self.cache.create(SIMPLE_PATH, 33204)

    def tearDown(self):
        self.reset()

    def reset(self):
        remove_recursive_silently("state.db")
        remove_recursive_silently(CACHE_DIR)

    def test_clean_file(self):
        self.worker._clean_path(SIMPLE_PATH)
