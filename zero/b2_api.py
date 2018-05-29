from b2.api import B2Api
from b2.bucket import Bucket
from b2.account_info.in_memory import InMemoryAccountInfo


class FileAPI:

    def __init__(self, account_id, application_key, bucket_id):
        account_info = InMemoryAccountInfo()
        self.api = B2Api(account_info)
        self.api.authorize_account("production", account_id, application_key)
        self.bucket_api = Bucket(self.api, bucket_id)

    def upload(self, file, identifier):
        data = file.read()
        self.bucket_api.upload_bytes(data, identifier)
