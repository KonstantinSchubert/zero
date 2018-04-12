class Cache:
    def __init__(self, cache_folder, anti_collision_hash = ""):
        self.cache_folder = cache_folder
        self.anti_collision_hash = anti_collision_hash

    CACHE_ENDINGS = [
        "dirty", 
        "cleaning", 
        "todelete", 
        "deleting",
        "dummy"
    ]

    def get_cache_ending(path):
        for ending in CACHE_ENDINGS:
            if path.endswith(self.anti_collision_hash + ending)
                return ending

    def strip_cache_ending(path):
        ending = get_cache_ending(path)
        if ending:
            return path[:-len(ending)]
        else:
            return path

    def get_path_or_dummy(path):
        cache_path = get_cache_path(path)
        if cache_path exists:
            return cache_path
        elif cache_path + anti_collision_hash + "dummy" exists:
            return cache_path + "dummy"
        raise FuseOSError(errno.EACCES)

    def get_path(path):
        cache_path = get_cache_path(path)
        if file cache_path + ".dummy" exists:
            synchronously download real file and replace dummy 
        return cache_path


    def list_nodes_and_dummies(dir_path):
        all_files = os.listdir(path)
        return (
            file for file in all_files 
            if get_cache_ending(file) in [ None, "dummy"]
        )

    def list_effective_nodes(dir_path):
        return (
            strip_cache_ending(path) for path 
            in list_nodes_and_dummies(dir_path)
        )
