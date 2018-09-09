from unittest import TestCase
from fuse import FuseOSError
from unittest.mock import MagicMock

from zero.operations import Filesystem

from .configure import IntegrationTestingContext

PATH = "yo"
FILE_CONTENT = b"Some file content"


class OperationTest(TestCase):

    def create_local_file(self):
        return self.context.create_file(PATH, FILE_CONTENT)


class AccessTest(OperationTest):
    """Covers the following operations:
        - access
    """

    def setUp(self):
        self.api = MagicMock()
        self.context = IntegrationTestingContext(api=self.api)
        self.filesystem = Filesystem(self.context.cache)

    def test_access_local_file(self):
        # create a local file
        self.create_local_file()
        # make sure the access call succeeds
        self.filesystem.access(PATH, 0)
        # assert sure another access call fails
        with self.assertRaises(FuseOSError):
            self.filesystem.access(PATH, 7)

    def test_access_remote_file(self):
        # create a local file with default permissions
        path = self.create_local_file()
        # make file remote
        inode = self.context.inode_store.get_inode(path)
        self.context.worker._clean_inode(inode)
        self.context.cache.create_dummy(inode)
        # assert that a specific access call succeeds
        self.filesystem.access(path, 0)
        # assert sure another access call fails
        with self.assertRaises(FuseOSError):
            self.filesystem.access(path, 7)


class CreateWriteReadTest(OperationTest):
    """Covers the following operations:
        - create
        - write
        - flush
        - release
        - open
        - read
    """

    def setUp(self):
        self.api = MagicMock()
        self.context = IntegrationTestingContext(api=self.api)
        self.filesystem = Filesystem(self.context.cache)

    def _create_file(self, path):
        file_handle = self.filesystem.create(PATH, 33204)
        assert type(file_handle) == int
        num_bytes_written = self.filesystem.write(
            path, FILE_CONTENT, 0, file_handle
        )
        self.filesystem.flush(path, file_handle)
        self.filesystem.release(path, file_handle)
        return num_bytes_written

    def _read_file(self, path):
        file_handle = self.filesystem.open(path, 0)
        response = self.filesystem.read(path, 100, 0, file_handle)
        return response

    def test_create_and_read_file(self):
        self._create_file(path=PATH)
        content = self._read_file(PATH)
        assert content == FILE_CONTENT

    def test_path_is_ignored_during_read(self):
        self._create_file(path=PATH)
        file_handle = self.filesystem.open(PATH, 0)
        content = self.filesystem.read("/wrong/path", 100, 0, file_handle)
        assert content == FILE_CONTENT
