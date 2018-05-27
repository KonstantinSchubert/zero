from b2.api import B2Api
from b2.bucket import Bucket


class FileAPI:

    def __init__(self, account_info, account_id, application_key, bucket_id):
        self.api = B2Api(account_info)
        self.api.authorize_account("production", account_id, application_key)
        self.bucket_api = Bucket(self.api, bucket_id)

    def upload(self, file):
        data = file.read(0, len(file))
        print(len(data))
        self.bucket_api.upload_bytes(bytes(data), self.file_info["fileName"])
        # self.b2fuse._update_directory_structure() //<-?? copyp-pasted from b2_fuse
        # self.file_info = self.b2fuse._directories.get_file_info(self.file_info['fileName']) //<-?? copyp-pasted from b2_fuse
