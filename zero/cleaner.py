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

    def __init__(self, cache_folder, api):
        self.converter = PathConverter(cache_folder)
        self.api = api
        self.converter = self.converter
        self.states = StateMachine(cache_folder=cache_folder)
        self.remote_identifiers = RemoteIdentifiers(cache_folder)
        self.dirty_flags = DirtyFlags(cache_folder)

    def run_watcher(self):
        with EventListener(FileUpdateOrCreateEvent.topic) as cleaning_listener:
            while True:
                time.sleep(1)
                for message in cleaning_listener.yield_events():

                    path = message["path"]
                    print("Received message to clean " + path)
                    if not self.states.current_state_is_dirty(path):
                        print(
                            "No dirty flag found on path. Probably already uploaded"
                        )
                        continue
                    # It is impotant to take it easy here - often, after receiving a message,
                    # the file is still being written. There is no point starting to upload
                    # immediately
                    # By having this sleep AFTER the first dirty check, we can speed through messages that
                    # tell us a file is dirty that isn't actually dirty.
                    # By having this sleep BEFORE the path lock, we give fuse a chance to finish writing the file
                    # before we are attempting an upload.
                    time.sleep(1)
                    # In the long run we may have to think of smarter ways to handle duplicate messages here.
                    # We might want to keep an internal de-duplciated backlog of file that we still have to clean,
                    # and then have one worker to fill the backlog and (eliminating duplicates) and another
                    # worker working off it
                    with PathLock(
                        path,
                        acquisition_max_retries=10,
                        exclusive_lock_on_leaf=False,
                        high_priority=False,
                        lock_creator="Cleaner",
                    ) as lock:
                        # - check if file exists and is still dirty
                        if not self.states.current_state_is_dirty(path):
                            print(
                                "No dirty flag found on path. Some other thread cleaned it within the last second."
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
