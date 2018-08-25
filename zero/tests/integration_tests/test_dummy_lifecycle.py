from unittest import TestCase
import stat
from zero.b2_api import FileAPI
from zero.b2_file_info_store import FileInfoStore
from zero.main import get_config
from zero.operations import Filesystem
from .configure import IntegrationTestingContext
from ..utils import remove_recursive_silently


PATH = "yo"
FILE_CONTENT = b"Some file content"
DB_PATH = "test.db"


class DummyLifeCycleTest(TestCase):

    def create_local_file(self):
        return self.context.create_file(PATH, FILE_CONTENT)

    def setUp(self):
        remove_recursive_silently(DB_PATH)
        file_info_store = FileInfoStore(DB_PATH)
        config = get_config()
        self.api = FileAPI(
            file_info_store=file_info_store,
            account_id=config["accountId"],
            application_key=config["applicationKey"],
            bucket_id=config["bucketId"],
        )
        self.context = IntegrationTestingContext(api=self.api)
        self.filesystem = Filesystem(self.context.cache)

    def test_stat_attributes_are_persistent(self):

        # todo: this test should test all stat attributes, not just the st_mode

        # Create a local file
        path = self.create_local_file()
        # By default, file is not executable
        assert self.filesystem.getattr(path)["st_mode"] == 33204
        # Make file executable
        self.filesystem.chmod(
            path, stat.S_IWOTH | stat.S_IROTH | stat.S_IWUSR | stat.S_IRUSR
        )
        # File should now have different permissions
        assert self.filesystem.getattr(path)["st_mode"] == 33158
        # make file remote
        inode = self.context.inode_store.get_inode(path)
        self.context.worker._clean_inode(inode)
        self.context.cache.create_dummy(inode)
        # assert that ownership and permissions are the same still
        assert self.filesystem.getattr(path)["st_mode"] == 33158
        self.context.cache.replace_dummy(inode)
        # make file local
        assert self.filesystem.getattr(path)["st_mode"] == 33158
        # assert that ownership and permissions are the same still
