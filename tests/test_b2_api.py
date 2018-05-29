import unittest
import io

from zero.b2_api import FileAPI
from zero.b2_real_credentials import account_id, application_key, bucket_id

TEST_BINARY_DATA = b"some _data"
TEST_PATH = "home/kon/whatever/yo"


class B2APITest(unittest.TestCase):

    def setUp(self):
        # Todo mock this and pass fake data here (?)
        self.fileAPI = FileAPI(account_id, application_key, bucket_id)

    def test_upload_file(self):
        # Tested code expects a file object, but binary stream shoudl have sam eAPI.
        binary_stream = io.BytesIO(TEST_BINARY_DATA)
        self.fileAPI.upload(binary_stream, TEST_PATH)
