import unittest
import io
from b2.account_info.in_memory import InMemoryAccountInfo

from zero.b2_api import FileAPI
from .b2_real_credentials import account_id, application_key, bucket_id


class B2APITest(unittest.TestCase):

    def setUp(self):
        # Todo mock this and pass fake data here (?)
        self.fileAPI = FileAPI(
            InMemoryAccountInfo(), account_id, application_key, bucket_id
        )

    def test_upload_file(self):
        # Tested code expects a file object, but binary stream shoudl have sam eAPI.
        binary_stream = io.BytesIO(b"Some data")
        self.fileAPI.upload(binary_stream)
