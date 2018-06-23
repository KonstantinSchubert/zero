import sqlite3


class InodeStore:
    """Converts between inode numbers and fuse paths"""

    def __init__(self, db_path):
        self.connection = sqlite3.connect(db_path, timeout=5)
        with self.connection:
            self.connection.execute(
                """CREATE TABLE IF NOT EXISTS inodes (nodepath text primary key, inode text)"""
            )

    def create_and_get_inode(self, path):
        with self.connection:
            print("creating path", path)
            self._create_path(path)
            return self._get_inode(path)

    def get_inode(self, path):
        with self.connection:
            return self._get_inode(path)

    def get_paths(self, inode):
        with self.connection:
            cursor = self.connection.execute(
                """SELECT nodepath FROM inodes WHERE inode = ?""", (inode,)
            )
            return [result[0] for result in cursor.fetchall()]

    def delete_path(self, path):
        with self.connection:
            self.connection.execute(
                """DELETE from inodes WHERE nodepath = ?""", (path,)
            )

    def _create_path(self, path):
        self.connection.execute(
            """INSERT INTO inodes (nodepath, inode)
            VALUES (?, (SELECT IFNULL(MAX(inode), 0) + 1 FROM inodes))
            """,
            (path,),
        )

    def _get_inode(self, path):
        cursor = self.connection.execute(
            """SELECT inode FROM inodes WHERE nodepath = ?""", (path,)
        )
        result = cursor.fetchone()
        return result and result[0] or 0
