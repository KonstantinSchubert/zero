from .state_store import StateStore


class Worker:

    def __init__(self, converter, api):
        self.api = api
        self.converter = converter
        self.state_store = StateStore()

    def _clean_path(self, path):
        self.state_store.set_cleaning(path)
        with open(self.converter.to_cache_path(path)) as file_to_upload:
            self.api.upload(file_to_upload, path)
        self.state_store.set_clean(path)

    def _delete_path(self, path):
        # Todo: Obtin path lock or make operation atomic in sqlite
        self.state_store.set_deleting(path)
        self.api.delete(path)
        self.state_store.set_deleted(path)
