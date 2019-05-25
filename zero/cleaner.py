import logging
import time
from multiprocessing import Process
from .locking import NodeLockedException, PathLock
from .remote_identifiers import RemoteIdentifiers
from .events import EventListener, FileUpdateOrCreateEvent
from .dirty_flags import DirtyFlags
from .states import StateMachine
from .path_converter import PathConverter

logger = logging.getLogger("spam_application")


class UploadAbortException(Exception):
    pass


def upload(api, file_to_upload, new_uuid):
    api.upload(file=file_to_upload, file_uuid=new_uuid)


class Cleaner:

    def __init__(self, cache_folder, inode_store, api):
        self.converter = PathConverter(cache_folder)
        self.api = api
        self.converter = self.converter
        self.inode_store = inode_store
        self.states = StateMachine(cache_folder=cache_folder)
        self.remote_identifiers = RemoteIdentifiers(cache_folder)
        self.dirty_flags = DirtyFlags(cache_folder)

    def run_watcher(self):
        with EventListener(FileUpdateOrCreateEvent.topic) as cleaning_listener:
            while True:
                time.sleep(1)
                for message in cleaning_listener.yield_events():

                    # It is impotant to take it easy here - often, after receiving a message,
                    # the file is still being written. There is no point starting to upload
                    # immediately
                    time.sleep(1)
                    # Of course, in the long run we have to think of smarter ways to do de-duplication here.

                    path = message["path"]
                    print("Received message to clean " + path)
                    if not self.states.current_state_is_dirty(path):
                        print(
                            "No dirty flag found on path, aborting. This can happen"
                        )
                        continue
                    with PathLock(
                        path,
                        self.inode_store,
                        acquisition_max_retries=10,
                        exclusive_lock_on_leaf=True,
                        high_priority=False,
                        lock_creator="Cleaner",
                    ) as lock:
                        # - check if file exists and is still dirty
                        if not self.states.current_state_is_dirty(path):
                            print(
                                "No dirty flag found on path, aborting. This can happen"
                            )
                            continue

                        # - get old uuid if it exists
                        old_uuid = self.remote_identifiers.get_uuid_or_none(
                            path
                        )
                        if old_uuid:
                            # - If yes, delete old version of file on remote
                            self.api.delete(old_uuid)

                        try:
                            new_uuid = self.upload_file(path=path, lock=lock)
                            self.states.dirty_to_clean(path)
                            self.remote_identifiers.set_uuid(
                                path=path, uuid=new_uuid
                            )
                        except UploadAbortException:
                            print(f"upload of {path} was ABORTED")

    def upload_file(self, path, lock):
        new_uuid = RemoteIdentifiers.generate_uuid()
        with open(self.converter.to_cache_path(path), "rb") as file_to_upload:
            print(f"cleaning {path}")
            # since I don't want to mess with the b2 library code
            # but I do want to be able to interrupt the upload
            # the best option seems to be using python multiprocessing
            # https://docs.python.org/3/library/multiprocessing.html#the-process-class
            upload_process = Process(
                target=upload, args=(self.api, file_to_upload, new_uuid)
            )
            upload_process.start()
            while upload_process.is_alive():
                print(f"upload of {path} is alive")
                time.sleep(0.1)
                if lock.abort_requested():
                    upload_process.terminate()
                    raise UploadAbortException
        return new_uuid
