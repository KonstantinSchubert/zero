import unittest
from b2_real_credentials import account_id, application_key, bucket_id
from b2.account_info.in_memory import InMemoryAccountInfo


class B2APITest(unittest.TestCase):

    def setUp(self):
        # Todo mock this and pass fake data here
        self.account_info = InMemoryAccountInfo()
        self.account_id = account_id
        self.application_key = application_key
        self.bucket_id = bucket_id

    def test_upload_file(self):
        pass
