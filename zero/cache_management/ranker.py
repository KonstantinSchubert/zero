import time
import os

from zero.states import StateMachine
from zero.events import (
    EventListener,
    FileAccessEvent,
    FileUpdateOrCreateEvent,
    FileDeleteEvent,
    FileRenameOrMoveEvent,
    FolderRenameOrMoveEvent,
    FileEvictedFromCacheEvent,
    FileLoadedIntoCacheEvent,
)

from .rank_store import RankStore


class Ranker:
    """The ranker tracks file access and write events and uses an
    algorithm to rank files by importance.
    In the first iteration, the ranker will not try to track files across
    moves, and simply deal with the fact that some of it's rank store
    entries are non-existant, and that its rank store is missing some entries.
    In a next iteration, the ranker will be updated to also follow file move events.
    However, due to the interderministic nature of the message system, this will never
    be perfect and it will always be necessary to handle mismatches between the rank store
    and the reality in the file system.

    The ranker also needs to keep track of which files are remote and which are local.
    Again, our tracking precision is only approximately correct.
    """

    def __init__(self, db_file, cache_folder):
        self.rank_store = RankStore(db_path=db_file)
        self.access_times = {}
        self.states_read_only = StateMachine(cache_folder=cache_folder)

    def handle_file_access(self, path):
        """Update ranking in reaction to the access event"""
        if not self._was_accessed_recently(path):
            print("RECORDING ACCESS")
            self._record_access_time(path)
            self.rank_store.record_access(path, time.time())

    def handle_file_delete(self, path):
        """Update ranking in reaction to a file being deleted"""
        self.rank_store.remove_or_ignore(path)

    def handle_cache_insertion(self, path):
        self.rank_store.mark_as_local(path)

    def handle_cache_eviction(self, path):
        self.rank_store.mark_as_remote(path)

    def is_sufficiently_sorted(self):
        """Return true the files in cache are the highest ranking files.
        It is possible to allow for some leniency here in order to have
        some more stability towards fast rank changes
        """
        # For now, check for the "hard" condition: Completely sorted
        return self.rank_store.ranks_are_sorted()

    def re_index(self, path):
        if not os.path.isfile(path):
            self.rank_store.remove_or_ignore(path)
            return
        # TODO: Try-Catch the case that file is concurrently removed after the above check.
        # In this case, we just do nothing. Re-index doesn't need to be 100% reliable.
        if self.states.current_state_is_remote(path):
            self.rank_store.mark_as_remote(path)
        else:
            self.rank_store.mark_as_local(path)

    def watch_events(self):
        # We use a single event listener in order to preserve the sequence of messages (avoid sharding).
        # This should improve our chances of keeping track of file and folder moves/renames before correctly.
        with EventListener(
            (
                FileUpdateOrCreateEvent.topic,
                FileAccessEvent.topic,
                FileDeleteEvent.topic,
                FileEvictedFromCacheEvent.topic,
                FileLoadedIntoCacheEvent.topic,
                FileRenameOrMoveEvent.topic,
                FolderRenameOrMoveEvent.topic,
            )
        ) as multi_topic_listener:
            while True:
                time.sleep(0.1)
                for message in multi_topic_listener.yield_events():
                    print(message)
                    message_topic = message["topic"]
                    if message_topic in (
                        FileUpdateOrCreateEvent.topic,
                        FileAccessEvent.topic,
                    ):
                        self.handle_file_access(message["path"])
                    elif message_topic == FileDeleteEvent.topic:
                        self.handle_file_delete(message["path"])
                    elif message_topic == FileEvictedFromCacheEvent.topic:
                        self.handle_cache_eviction(message["path"])
                    elif message_topic == FileLoadedIntoCacheEvent.topic:
                        self.handle_cache_insertion(message["path"])
                    elif message_topic == FileRenameOrMoveEvent.topic:
                        # TODO: rename path in the rank store
                        pass
                    elif message_topic == FolderRenameOrMoveEvent.topic:
                        # TODO: rename all affected paths in the rank store
                        pass
                    else:
                        raise Exception("Unexpected event")

    def scan(self):
        # TODO: Because the message system is not determinisistc, we need to sporadically
        # scan the file system to
        # Insert paths into rank_store that are missing
        # Remove paths from rank_store that do actually no longer exist (for this I need to iterate over the table!)
        # Correct the FILE_LOCATION (Local vs remote) of a path -> call re_index(path) for all discovered paths
        pass

    # The next two functions should be refactored into their own class
    def _record_access_time(self, inode):
        self.access_times[inode] = time.time()

    def _was_accessed_recently(self, inode):
        last_access = self.access_times.get(inode)
        return last_access is not None and (time.time() - last_access < 10 * 60)
