import sqlite3


class RankStore:
    """ This class is NOT thread safe"""

    def __init__(self, db_path):
        self.connection = sqlite3.connect(db_path, timeout=5)
        with self.connection:
            self.connection.execute(
                """CREATE TABLE IF NOT EXISTS ranks (inode text primary key, rank text)"""
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
        self.connection.execute(
            """DELETE from ranks WHERE inode = ?""", (inode,)
        )

    def apply_rank_factor(self, factor):
        self.connection.execute(
            """UPDATE rank SET Quantity = Quantity * ?""", (factor,)
        )

    def _set_rank_on_path(self, inode, rank):
        with self.connection:

            self.connection.execute(
                """INSERT OR REPLACE INTO ranks (inode, rank) VALUES (?, ?)""",
                (inode, rank),
            )
