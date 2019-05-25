"""B2 file back-end.
The lifecycle settings on the bucket must be configured to
'keep only the last version.
"""
from io import BytesIO
from b2.api import B2Api
from b2.bucket import Bucket
from b2.account_info.in_memory import InMemoryAccountInfo
from b2.download_dest import DownloadDestBytes
from b2.exception import B2ConnectionError
from .b2_file_info_store import FileInfoStore


class FileAPI:

    def __init__(self, account_id, application_key, bucket_id, db_file):
        try:
            account_info = InMemoryAccountInfo()
            self.api = B2Api(account_info)
            self.api.authorize_account(
                "production", account_id, application_key
            )
        except B2ConnectionError as e:
            print(e)
            raise ConnectionError
        self.bucket_api = Bucket(self.api, bucket_id)
        self.file_info_store = FileInfoStore(db_file)

    def upload(self, file, file_uuid, file_uuid_to_replace=None):
        if file_uuid_to_replace is not None:
            self.delete(file_uuid_to_replace)
        data = file.read()
        file_info = self.bucket_api.upload_bytes(data, str(file_uuid))
        self.file_info_store.set_file_id(
            file_uuid, file_info.as_dict().get("fileId")
        )

    def delete(self, file_uuid):
        file_id = self.file_info_store.get_file_id(file_uuid)
        if not file_id:
            # No file ID means file was never synched to remote
            # TODO: I belive that we will no longer have this case since we now catch it in the worker
            # by not finding a uuid: `if file_uuid is None:`...
            print(
                f"Not deleting {file_uuid} because no file_id in file info store. "
                f"This should not happen"
            )
            return
        print("deleting file from remote")
        self.bucket_api.delete_file_version(file_id, str(file_uuid))
        self.file_info_store.remove_entry(file_uuid)

    def download(self, file_uuid):
        download_dest = DownloadDestBytes()
        file_id = self.file_info_store.get_file_id(file_uuid)
        try:
            self.bucket_api.download_file_by_id(file_id, download_dest)
        except B2ConnectionError:
            raise ConnectionError
        return BytesIO(download_dest.get_bytes_written())
