import sqlite3
from datetime import datetime


class TIMES:
    ATIME = "atime"
    MTIME = "mtime"
    CTIME = "ctime"


# TODO: Add other metadata? Or is some of it stored via the dummy?:

#             "st_gid",
#             "st_mode",
#             "st_nlink",
#             "st_size",
#             "st_uid",

class MetaData:
    """Stores meta data about the file such as file access times."""

    def __init__(self, db_inode):
        self.connection = sqlite3.connect(
            db_inode, timeout=5, detect_types=sqlite3.PARSE_DECLTYPES
        )
        with self.connection:
            self.connection.execute(
                """CREATE TABLE IF NOT EXISTS metadata (inode integer primary key, atime timestamp, mtime timestamp, ctime timestamp)"""
            )

    # TODO: These hooks need to get called in more places.
    def record_content_modification(self, inode):
        self._record_change(inode)
        self._record_modification(inode)
        self._record_access(inode)

    def record_access(self, inode):
        self._record_access(inode)

    def _record_access(self, inode):
        """Sets the atime"""
        with self.connection:
            self._initialize_entry_if_not_exists()
            self._set_column_to_now(TIMES.ATIME)

    def _record_modification(self, inode):
        """Sets the mtime"""
        with self.connection:
            self._initialize_entry_if_not_exists()
            self._set_column_to_now(TIMES.MTIME)

    def _record_change(self, inode):
        """Sets the ctime"""
        with self.connection:
            self._initialize_entry_if_not_exists()
            self._set_column_to_now(TIMES.CTIME)

    def get_access_time(self, inode):
        return self._get_time(inode, TIMES.ATIME)

    def get_modification_time(self, inode):
        return self._get_time(inode, TIMES.MTIME)

    def get_change_time(self, inode):
        return self._get_time(inode, TIMES.CTIME)

    def _get_time(self, inode, column):
        cursor = self.connection.execute(
            """SELECT ? FROM metadata WHERE inode = ?""", (column, inode)
        )
        result = cursor.fetchone()
        return result or 0

    def _initialize_entry_if_not_exists(self, inode):
        now = datetime.now()
        self.connection.execute(
            """INSERT OR IGNORE INTO metadata (inode, atime, mtime, ctime) VALUES (?,?,?,?)""",
            (inode, now, now, now),
        )

    def _set_column_to_now(self, inode: int, column_name: TIMES):
        now = datetime.now()
        self.connection.execute(
            f"""UPDATE metadata set ? = ? WHERE inode = ?""",
            (column_name, now, inode),
        )
