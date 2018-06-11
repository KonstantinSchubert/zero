"""B2 file back-end.
The lifecycle settings on the bucket must be configured to
'keep only the last version.
"""
from io import BytesIO
from b2.api import B2Api
from b2.bucket import Bucket
from b2.account_info.in_memory import InMemoryAccountInfo
from b2.download_dest import DownloadDestBytes


class FileAPI:

    def __init__(self, file_info_store, account_id, application_key, bucket_id):
        account_info = InMemoryAccountInfo()
        self.api = B2Api(account_info)
        self.api.authorize_account("production", account_id, application_key)
        self.bucket_api = Bucket(self.api, bucket_id)
        self.file_info_store = file_info_store

    def upload(self, file, identifier):
        data = file.read()
        file_info = self.bucket_api.upload_bytes(data, identifier)
        self.file_info_store.set_file_id(
            identifier, file_info.as_dict().get("fileId")
        )

    def delete(self, identifier):
        file_id = self.file_info_store.get_file_id(identifier)
        self.bucket_api.delete_file_version(file_id, identifier)

    def download(self, identifier):
        download_dest = DownloadDestBytes()
        file_id = self.file_info_store.get_file_id(identifier)
        self.bucket_api.download_file_by_id(file_id, download_dest)
        return BytesIO(download_dest.get_bytes_written())
