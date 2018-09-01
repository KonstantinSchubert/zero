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

        # Create a local file
        path = self.create_local_file()
        # By default, file is not executable
        stat_before_chmod = self.filesystem.getattr(path)
        # Make file executable
        self.filesystem.chmod(
            path, stat.S_IWOTH | stat.S_IROTH | stat.S_IWUSR | stat.S_IRUSR
        )
        # File should now have different permissions
        print(self.filesystem.getattr(path))
        stat_after_chmod = self.filesystem.getattr(path)
        self.assert_stat_unequal(stat_after_chmod, stat_before_chmod)
        # make file remote
        inode = self.context.inode_store.get_inode(path)
        self.context.worker._clean_inode(inode)
        self.context.cache.create_dummy(inode)
        self.assert_stat_equal(stat_after_chmod, self.filesystem.getattr(path))
        # make file local
        self.context.cache.replace_dummy(inode)
        self.assert_stat_equal(stat_after_chmod, self.filesystem.getattr(path))

    def assertDictAlmostEqual(self, first, second):
        for key in first.keys():
            self.assertAlmostEqual(first[key], second[key])

    def create_local_file(self):
        return self.context.create_file(PATH, FILE_CONTENT)

    def assert_stat_equal(self, first, second):
        # We are not managing the ctime at the moment
        # because it can only be written by the system
        # and we would have to permanently obtain it from a different
        # storage place, such a meta data file
        first = first.copy()
        second = second.copy()
        first.pop("st_ctime")
        second.pop("st_ctime")
        assert first == second

    def assert_stat_unequal(self, first, second):
        # We are not managing the ctime at the moment
        # because it can only be written by the system
        # and we would have to permanently obtain it from a different
        # storage place, such a meta data file
        first = first.copy()
        second = second.copy()
        first.pop("st_ctime")
        second.pop("st_ctime")
        assert first != second
