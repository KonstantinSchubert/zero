from b2.api import B2Api
from b2.bucket import Bucket


class FileAPI:

    def __init__(self, account_info, account_id, application_key, bucket_id):
        self.api = B2Api(account_info)
        self.api.authorize_account("production", account_id, application_key)
        self.bucket_api = Bucket(self.api, bucket_id)

    @staticmethod
    def _encode_identifier(identifier):
        # Identifiers can not have leading slash in backblaze. Thus
        # we strip it before upload.
        if not identifier[0] == "/":
            raise Exception("Identifier must start with leading slash")
        return identifier[1:]

    @staticmethod
    def _decode_identifier(identifier):
        return "/" + identifier

    def upload(self, file, identifier):
        data = file.read()
        self.bucket_api.upload_bytes(
            data, FileAPI._encode_identifier(identifier)
        )
        # self.b2fuse._update_directory_structure() //<-?? copyp-pasted from b2_fuse
        # self.file_info = self.b2fuse._directories.get_file_info(self.file_info['fileName']) //<-?? copyp-pasted from b2_fuse
