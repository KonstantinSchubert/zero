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

    def upload(self, file, inode):
        data = file.read()
        file_info = self.bucket_api.upload_bytes(data, str(inode))
        self.file_info_store.set_file_id(
            inode, file_info.as_dict().get("fileId")
        )

    def delete(self, inode):
        file_id = self.file_info_store.get_file_id(inode)
        if not file_id:
            # No file ID means file was never synched to remote
            print(
                f"Not deleting {inode} because no file_id in file info store. "
                f"Maybe had never been synched to remote"
            )
            return
        self.bucket_api.delete_file_version(file_id, str(inode))
        self.file_info_store.remove_entry(inode)

    def download(self, inode):
        download_dest = DownloadDestBytes()
        file_id = self.file_info_store.get_file_id(inode)
        self.bucket_api.download_file_by_id(file_id, download_dest)
        return BytesIO(download_dest.get_bytes_written())
