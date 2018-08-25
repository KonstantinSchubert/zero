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

    def setUp(self):
        self.api = MagicMock()
        self.context = IntegrationTestingContext(api=self.api)
        self.filesystem = Filesystem(self.context.cache)

    def test_access_local_file(self):
        # create a local file
        path = self.create_local_file()
        # make sure the access call succeeds
        self.filesystem.access(path, 0)
        # assert sure another access call fails
        with self.assertRaises(FuseOSError):
            self.filesystem.access(path, 7)

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
