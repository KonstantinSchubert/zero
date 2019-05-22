import sqlite3


class InodeStore:
    """Converts between inode numbers and fuse paths"""

    def __init__(self, db_path):
        self.connection = sqlite3.connect(db_path, timeout=5)
        with self.connection:
            self.connection.execute(
                """CREATE TABLE IF NOT EXISTS inodes (nodepath text primary key, inode integer)"""
            )
            # Initialize sequence, needs refactoring
            self.connection.execute(
                """CREATE TABLE IF NOT EXISTS sequences (name text primary key, value integer)"""
            )

    def create_path(self, path):
        # TODO: In order to replace the inode,
        # we will first need to introduce a random unique identifier for each file that we store in a meta-file
        # (see comment in `purge` method of worker.py)
        with self.connection:
            self._create_path(path)

    def get_inode(self, path):
        assert path[-1] != "/"
        # All paths end without trailing slash
        with self.connection:
            return self._get_inode(path)

    def get_inodes(self, folder_path):
        with self.connection:
            return self._get_inodes(folder_path)

    def get_paths(self, inode):
        with self.connection:
            cursor = self.connection.execute(
                """SELECT nodepath FROM inodes WHERE inode = ?""", (inode,)
            )
            return [result[0] for result in cursor.fetchall()]

    def delete_path(self, path):
        with self.connection:
            self._delete_path(path)

    def rename_paths(self, old_partial, new_partial):
        with self.connection:
            # Find all rows where the path starts with old_partial.
            # Update all rows, replaceing old_partial with new_partial in their path
            cursor = self.connection.execute(
                f"""SELECT nodepath, inode FROM inodes WHERE nodepath LIKE '{old_partial}%'"""
            )
            matches = cursor.fetchall()
            for nodepath, inode in matches:
                new_path = nodepath.replace(old_partial, new_partial)
                print("current path:", nodepath)
                print("new path", new_path)
                self.connection.execute(
                    """UPDATE inodes SET nodepath=? WHERE inode=?""",
                    (new_path, inode),
                )

    def _create_path(self, path):
        inode = self._get_inode_sequence()
        self.connection.execute(
            # The problem is using max here is that I might re-use
            # old values if the old max is removed. Better would be
            # a sequence as it is supported by postgresql
            """INSERT INTO inodes (nodepath, inode) VALUES (?, ?)
            """,
            (path, inode),
        )

    def _delete_path(self, path):
        self.connection.execute(
            """DELETE from inodes WHERE nodepath = ?""", (path,)
        )

    def _get_inode(self, path):
        cursor = self.connection.execute(
            """SELECT inode FROM inodes WHERE nodepath = ?""", (path,)
        )
        result = cursor.fetchone()
        return result and result[0]

    def _get_inodes(self, folder_path):
        cursor = self.connection.execute(
            """SELECT inode FROM inodes WHERE nodepath LIKE ?""",
            (f"folder_path%",),
        )
        return cursor.fetchall()

    def _get_inode_sequence(self):
        cursor = self.connection.execute(
            """SELECT value FROM sequences WHERE name='inode_sequence'"""
        )
        result = cursor.fetchone()
        value = result and result[0]
        if value is None:
            self.connection.execute(
                """INSERT INTO sequences (name, value) VALUES ('inode_sequence', 1)"""
            )
            value = 1
        self.connection.execute(
            """UPDATE sequences SET value=value+1 WHERE name='inode_sequence'"""
        )
        return value
