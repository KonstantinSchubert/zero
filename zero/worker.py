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

    def replace_dummy(self, path):
        # Todo: Worry about settings permissions and timestamps
        # Todo: Worry about concurrency
        # Todo: should this function go to the Cache class and
        # instead of a worker I pass api to the cache class and an instance
        # of cache to the worker?
        with open(path, "w+b") as file:
            file.write(self.api.download(path))
        os.remove(self.converter.add_dummy_ending(path))
        print("hi")

    def create_dummy(self, path):
        # Todo: Worry about settings permissions and timestamps
        # Todo: Worry about concurrency
        # Todo: should this function go to the Cache class and
        # instead of a worker I pass api to the cache class and an instance
        # of cache to the worker?
        with open(path, "r+b") as file:
            self.api.upload(file, path)
        os.remove(path)
        with open(self.converter.add_dummy_ending(path)) as dummy_file:
            # Todo: set permissions and timestamps and stuff
            # For now, just touch:
            os.utime(dummy_file.fileno())
