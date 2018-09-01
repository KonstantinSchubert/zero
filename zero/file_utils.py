import os


class open_without_changing_times:

    def __init__(self, path, mode):
        self.path = path
        self.mode = mode
        self.stat_dict = get_stat_dictionary(self.path)

        # TODO: I cannot fix the ctime in this way. I will have to shield
        # the ctime by getting it from metadata: https://stackoverflow.com/questions/6084985/how-to-set-a-files-ctime-with-python

    def __enter__(self):
        self.open_file = open(self.path, self.mode)
        return self.open_file

    def __exit__(self, *args):
        self.open_file.close()
        print("setting mtime:", self.stat_dict["st_mtime"])
        os.utime(
            self.path, (self.stat_dict["st_atime"], self.stat_dict["st_mtime"])
        )


def get_stat_dictionary(path):
    stat = os.lstat(path)
    stat_dict = dict(
        (key, getattr(stat, key))
        for key in (
            "st_atime",
            "st_ctime",
            "st_gid",
            "st_mode",
            "st_mtime",
            "st_nlink",
            "st_size",
            "st_uid",
        )
    )
    return stat_dict
