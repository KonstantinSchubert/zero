import sqlite3


class RankStore:
    """ This class is NOT thread safe"""

    def __init__(self, db_path):
        self.connection = sqlite3.connect(db_path, timeout=5)
        with self.connection:
            self.connection.execute(
                """CREATE TABLE IF NOT EXISTS ranks (inode integer primary key, rank real)"""
            )

    def change_rank_on_inode(self, inode, rank_delta):
        with self.connection:
            cursor = self.connection.execute(
                """SELECT rank FROM ranks WHERE inode = ?""", (inode,)
            )
            result = cursor.fetchone()
            rank = result and float(result[0]) or 0
            self._set_rank_on_path(inode, rank + rank_delta)

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

    def _set_rank_on_path(self, inode, rank):
        with self.connection:

            self.connection.execute(
                """INSERT OR REPLACE INTO ranks (inode, rank) VALUES (?, ?)""",
                (inode, rank),
            )
