class Ranker:
    """Ranks are stored in the rank table which is a table containing
    all paths in the filesystem as primary keys.
    """

    def __init__(self, rank_store, inode_store):
        self.rank_store = rank_store
        self.inode_store = inode_store

    def handle_path_access(self, path):
        """Update ranking in reaction to the access event"""
        # Current algorithm is:
        # - Raise importance of accessed path's inode by 3 points
        inode = self.inode_store.get_inode(path)
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
