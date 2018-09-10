
import time

from fuse import FUSE


from .operations import Filesystem
from .state_store import StateStore
from .inode_store import InodeStore
from .cache import Cache
from .worker import Worker
from .path_converter import PathConverter
from .b2_api import FileAPI
from .ranker import Ranker
from .rank_store import RankStore
from .b2_file_info_store import FileInfoStore


from .config_utils import get_config, parse_fuse_args, parse_worker_args


def fuse_main():

    args = parse_fuse_args()
    config = get_config()

    file_info_store = FileInfoStore(config["sqliteFileLocation"])
    api = FileAPI(
        file_info_store=file_info_store,
        account_id=config["accountId"],
        application_key=config["applicationKey"],
        bucket_id=config["bucketId"],
    )
    converter = PathConverter(args.cache_folder)
    state_store = StateStore(config["sqliteFileLocation"])
    inode_store = InodeStore(config["sqliteFileLocation"])
    rank_store = RankStore(config["sqliteFileLocation"])
    ranker = Ranker(rank_store, inode_store)
    cache = Cache(converter, state_store, inode_store, ranker, api)
    filesystem = Filesystem(cache)
    FUSE(filesystem, args.mountpoint, nothreads=True, foreground=True)


TARGET_DISK_USAGE = 0.01  # GB


def worker_main():

    args = parse_worker_args()
    config = get_config()

    file_info_store = FileInfoStore(config["sqliteFileLocation"])
    api = FileAPI(
        file_info_store=file_info_store,
        account_id=config["accountId"],
        application_key=config["applicationKey"],
        bucket_id=config["bucketId"],
    )

    converter = PathConverter(args.cache_folder)
    state_store = StateStore(config["sqliteFileLocation"])
    inode_store = InodeStore(config["sqliteFileLocation"])
    rank_store = RankStore(config["sqliteFileLocation"])
    ranker = Ranker(rank_store, inode_store)
    cache = Cache(converter, state_store, inode_store, ranker, api)
    worker = Worker(cache, api, target_disk_usage=config["targetDiskUsage"])
    while True:
        worker.run()
        time.sleep(10)


def reset_all():
    import shutil
    import os

    args = parse_worker_args()
    config = get_config()
    shutil.rmtree(args.cache_folder)
    os.mkdir(args.cache_folder)
    os.remove(config["sqliteFileLocation"])
