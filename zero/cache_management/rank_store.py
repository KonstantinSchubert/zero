import sqlite3


class RankStore:
    """ This class is NOT thread safe"""

    def __init__(self, db_path):
        self.connection = sqlite3.connect(db_path, timeout=5)
        with self.connection:
            self.connection.execute(
                """CREATE TABLE IF NOT EXISTS ranks (inode integer primary key, rank real)"""
            )

    def record_access(self, inode, timestamp):
        with self.connection:
            # The rank is simply the timestamp of the last usage:
            # recent usage <-> high timestamp  <-> high rank
            self.connection.execute(
                """INSERT OR REPLACE INTO ranks (inode, rank) VALUES (?, ?)""",
                (inode, timestamp),
            )

    def remove_inode(self, inode):
        with self.connection:
            self.connection.execute(
                """DELETE from ranks WHERE inode = ?""", (inode,)
            )

    def apply_rank_factor(self, factor):
        with self.connection:
            self.connection.execute(
                """UPDATE ranks SET rank = rank * ?""", (factor,)
            )

    def get_clean_and_low_rank_inodes(self, limit):
        with self.connection:
            cursor = self.connection.execute(
                """SELECT ranks.inode FROM ranks INNER JOIN states on ranks.inode=states.inode WHERE states.state = 'CLEAN' ORDER BY ranks.rank ASC LIMIT ? """,
                (limit,),
            )
            results = cursor.fetchall()
        return [int(result[0]) for result in results]

    def get_remote_and_high_rank_inodes(self, limit):
        with self.connection:
            cursor = self.connection.execute(
                """SELECT ranks.inode FROM ranks INNER JOIN states on ranks.inode=states.inode WHERE states.state = 'REMOTE' ORDER BY ranks.rank DESC LIMIT ? """,
                (limit,),
            )
            results = cursor.fetchall()
        return [int(result[0]) for result in results]

    def ranks_are_sorted(self):
        with self.connection:
            cursor = self.connection.execute(
                """SELECT ranks.rank FROM ranks INNER JOIN states on ranks.inode=states.inode WHERE states.state = 'REMOTE' ORDER BY ranks.rank DESC LIMIT 1 """
            )
            results = cursor.fetchone()
            highest_remote_rank = results and results[0]

            cursor = self.connection.execute(
                """SELECT ranks.rank FROM ranks INNER JOIN states on ranks.inode=states.inode WHERE states.state = 'CLEAN' ORDER BY ranks.rank ASC LIMIT 1 """
            )
            results = cursor.fetchone()
            lowest_cache_rank = results and results[0]
        if (
            highest_remote_rank
            and lowest_cache_rank
            and highest_remote_rank > lowest_cache_rank
        ):
            return False
        return True
