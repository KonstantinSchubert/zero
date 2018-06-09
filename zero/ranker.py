import os
from .state_store import StateStore
from .b2_api import FileAPI


class Ranker:
    """Ranks are stored in the rank table which is a table containing
    all paths in the filesystem as primary keys.
    """

    def __init__(self):
        pass


    def handle_access(path):
        """Update ranking in reaction to the access event"""
        # Current algorithm is:
        # - Raise importance of accessed file by 3 points
        # - Raise importance of files in the same directory
        # and in directories above by 2 points
        # - Raise importance of files in directories below
        # the directory of the accessed file by 1 point


    def decay_rank(path):
        # Rank points are remvoed periodically such that atfer
        # 100 days, 90% of rank is lost. This works out to a
        # daily factor of 0.955 applied to all rank values.

        # Since this method will not necessarily be run regularily,
        # it should check when if it was last run more than a day ago
        # and only apply the factor in this case

        # This also means that if the program is not run, rank does not decay
        # Which seems right.

        # An alternative may be to deay rank by number of file accesses instead of by time.