from unittest import TestCase
from fuse import FuseOSError

from zero.b2_api import FileAPI
from zero.b2_file_info_store import FileInfoStore
from zero.main import get_config
from zero.operations import Filesystem

from .configure import TestingContext, DB_PATH

PATH = "yo"
FILE_CONTENT = b"Some file content"


class OperationTest(TestCase):

    def setUp(self):
        file_info_store = FileInfoStore(DB_PATH)
        config = get_config()
        self.api = FileAPI(
            file_info_store=file_info_store,
            account_id=config["accountId"],
            application_key=config["applicationKey"],
            bucket_id=config["bucketId"],
        )
        self.context = TestingContext(api=self.api)
        self.filesystem = Filesystem(self.context.cache)

    def create_local_file(self):
        return self.context.create_file(PATH, FILE_CONTENT)


class AccessTest(OperationTest):

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
        todo
        # assert that a specific access call succeeds
        todo
        # assert sure another access call fails
        with assertRaises(FuseOSError):
            self.filesystem.access(path, ??)

    def test_access_permissions_persistet(self):
        todo
        pass
        # create a local file
        # set weird ownership and permissions
        # make file remote
        # assert that ownership and permissions are the same still
        # make file local
        # assert that ownership and permissions are the same still
