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
        # Current algorithm is:
        # - Raise importance of accessed path's inode by 3 points
        # - Ignore any repeat access within 10 minutes
        if not self._was_accessed_recently(inode):
            print("RECORDING ACCESS")
            self._record_access_time(inode)
            self.rank_store.change_rank_on_inode(inode, 3)
        # Potential improvements:
        # - Also raise importance of files in the same directory
        # and in directories above by 2 points
        # - Also raise importance of files in directories below
        # the directory of the accessed file by 1 point
        # We should also consider the form of access, whether the file
        # was just ls-ted or read or written.

        # TODO: PROBLEM IS THAT BIG FILES HAVE MANY READS/WRITES
        # AND EACH READ/WRITE INCREASES THEIR RANK.
        # WE COULD MAYBE NORMALIZE BY FILE SIZE.

    def _record_access_time(self, inode):
        self.access_times[inode] = time.time()

    def _was_accessed_recently(self, inode):
        last_access = self.access_times.get(inode)
        return last_access is not None and (time.time() - last_access < 10 * 60)

    def handle_inode_delete(self, inode):
        """Update ranking in reaction to a file being deleted"""
        self.rank_store.remove_inode(inode)

    def decay_rank(self):
        self.rank_store.apply_rank_factor(0.955)
        # Rank is removed periodically such that atfer
        # 100 days, 90% of rank is lost. This works out to a
        # daily factor of 0.955 applied to all rank values.

        # This method is expected to be run from a daily cron job
        # This also means that if the program is not run, rank does not decay
        # Which seems right.

        # An alternative may be to deay rank by number of file accesses instead of by time.

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
