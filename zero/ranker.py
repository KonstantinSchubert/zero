import time


class Ranker:
    """Ranks are stored in the rank table which is a table containing
    all paths in the filesystem as primary keys.
    """

    def __init__(self, rank_store, inode_store):
        self.rank_store = rank_store
        self.inode_store = inode_store
        self.access_times = {}

    def handle_inode_access(self, inode):
        """Update ranking in reaction to the access event"""
        if not self._was_accessed_recently(inode):
            print("RECORDING ACCESS")
            self._record_access_time(inode)
            self.rank_store.record_access(inode, time.time())

    def handle_inode_delete(self, inode):
        """Update ranking in reaction to a file being deleted"""
        self.rank_store.remove_inode(inode)

    def _record_access_time(self, inode):
        self.access_times[inode] = time.time()

    def _was_accessed_recently(self, inode):
        last_access = self.access_times.get(inode)
        return last_access is not None and (time.time() - last_access < 10 * 60)

    def get_eviction_candidates(self, limit):
        return self.rank_store.get_clean_and_low_rank_inodes(limit)

    def get_priming_candidates(self, limit):
        return self.rank_store.get_remote_and_high_rank_inodes(limit)

    def is_sufficiently_sorted(self):
        """Return true the files in cache are the highest ranking files.
        It is possible to allow for some leniency here in order to have
        some more stability towards fast rank changes
        """
        # For now, check for the "hard" condition: Completely sorted
        return self.rank_store.ranks_are_sorted()
