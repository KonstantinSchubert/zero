import time
from rank_store import RankStore


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

    def __init__(self, rank_store, inode_store):
        self.rank_store = rank_store
        self.inode_store = inode_store
        self.access_times = {}

    def handle_file_access(self, path):
        """Update ranking in reaction to the access event"""
        if not self._was_accessed_recently(path):
            print("RECORDING ACCESS")
            self._record_access_time(path)
            self.rank_store.record_access(path, time.time())

    def handle_file_delete(self, path):
        """Update ranking in reaction to a file being deleted"""
        self.rank_store.remove_or_ignore(path)

    def get_eviction_candidates(self, limit):
        return self.rank_store.get_clean_and_low_rank_inodes(limit)
        # TODO: Check if eviction candidates actually exist in file system

    def get_priming_candidates(self, limit):
        return self.rank_store.get_remote_and_high_rank_inodes(limit)
        # TODO: Check if priming candidates actually exist in file system

    def is_sufficiently_sorted(self):
        """Return true the files in cache are the highest ranking files.
        It is possible to allow for some leniency here in order to have
        some more stability towards fast rank changes
        """
        # For now, check for the "hard" condition: Completely sorted
        return self.rank_store.ranks_are_sorted()

    # The next couple of functions should maybe be running concurrently

    def watch_access_events(self):
        # TODO: Implement this, call handle_file_access
        pass

    def watch_delete_events(self):
        # TODO: Implement this, call handle_file_delete
        pass

    def watch_file_rename_or_move_events(self):
        # TODO: Implement this in second iteration
        pass

    def watch_folder_rename_or_move_events(self):
        # TODO: Implement this in second iteration
        pass

    def watch_cache_evictions(self):
        # TODO: Implement this, call rank_store.record_cache_eviction
        pass

    def watch_cache_insertions(self):
        # TODO: Implement this, call rank_store.record_cache_insertion
        pass

    def scan_file_system(self):
        # TODO: Because the message system is not determinisistc, we need to sporadically
        # scan the file system to
        # Insert paths into rank_store that are missing
        # Remove paths from rank_store that do actually no longer exist
        # Correct the FILE_LOCATION (Local vs remote) of a path
        pass

    # The next two functions should be refactored into their own class
    def _record_access_time(self, inode):
        self.access_times[inode] = time.time()

    def _was_accessed_recently(self, inode):
        last_access = self.access_times.get(inode)
        return last_access is not None and (time.time() - last_access < 10 * 60)
