import io
from unittest import TestCase
from zero.b2_api import FileAPI
from zero.b2_file_info_store import FileInfoStore
from zero.main import get_config
from ..utils import remove_recursive_silently


TEST_BINARY_DATA = b"some _data"
TEST_INODE = 2
DB_PATH = "state.db"


class B2APITest(TestCase):

    def setUp(self):
        # Todo mock this and pass fake data here (?)
        remove_recursive_silently(DB_PATH)
        file_info_store = FileInfoStore(DB_PATH)
        config = get_config()
        self.fileAPI = FileAPI(
            file_info_store=file_info_store,
            account_id=config["accountId"],
            application_key=config["applicationKey"],
            bucket_id=config["bucketId"],
        )

    def _upload_file(self):
        # Tested code expects a file object, but binary stream shoudl have sam eAPI.
        binary_stream = io.BytesIO(TEST_BINARY_DATA)
        self.fileAPI.upload(binary_stream, TEST_INODE)

    def test_upload_file(self):
        self._upload_file()

    def test_delete_file(self):
        self._upload_file()
        self.fileAPI.delete(TEST_INODE)

    def test_download_file(self):
        self._upload_file()
        file = self.fileAPI.download(TEST_INODE)
        assert file.read() == TEST_BINARY_DATA
