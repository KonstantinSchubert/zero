from .state_store import StateStore


class Worker:

    def __init__(self, converter, api):
        self.api = api
        self.converter = converter
        self.state_store = StateStore()

    def _clean_path(self, path):
        # Todo: Obtain path lock or make operation atomic in sqlite
        if not self.state_store.is_dirty(path):
            return
        self.state_store.set_cleaning(path)
        # Todo: release path lock
        with open(self.converter.to_cache_path(path)) as file_to_upload:
            self.api.upload(file_to_upload)
        # Todo: Obtain path lock: or make operation atomic in sqlite
        if self.state_store.is_cleaning:
            self.state_store.remove(path)

    def _delete_path(self, path):
        # Todo: Obtin path lock or make operation atomic in sqlite
        if not self.state_store.is_todelete(path):
            return
