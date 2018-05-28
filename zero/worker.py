from .state_store import StateStore


class Worker:

    def __init__(self, converter, api):
        self.api = api
        self.converter = converter
        self.state_store = StateStore()

    def _clean_path(self, path):
        """Cleans path, assumes that path is dirty"""
        self.state_store.set_cleaning(path)
        with open(self.converter.to_cache_path(path)) as file_to_upload:
            self.api.upload(file_to_upload)
        self.state_store.remove(path)
