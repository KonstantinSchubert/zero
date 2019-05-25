
import time

from fuse import FUSE


from .operations import Filesystem
from .inode_store import InodeStore
from .cache import Cache
from .worker import Worker
from .b2_api import FileAPI
from .ranker import Ranker
from .rank_store import RankStore
from .sqlite_queue import DB_NAME as QUEUE_DB_NAME

import multiprocessing


from .config_utils import get_config, parse_args

TARGET_DISK_USAGE = 0.01  # GB


def main():

    args = parse_args()
    config = get_config()

    fuse = multiprocessing.Process(
        name="fuse", target=fuse_main, args=(args, config)
    )
    # worker = multiprocessing.Process(
    #     name="worker", target=worker_main, args=(args, config)
    # )

    deleter = multiprocessing.Process(
        name="deleter", target=worker_delete_watcher, args=(args, config)
    )

    cleaner = multiprocessing.Process(
        name="cleaner", target=worker_clean_watcher, args=(args, config)
    )

    fuse.start()
    # worker.start()
    deleter.start()
    cleaner.start()


def fuse_main(args, config):
    print("Starting fuse main")

    api = FileAPI(
        account_id=config["accountId"],
        application_key=config["applicationKey"],
        bucket_id=config["bucketId"],
        db_file=config["sqliteFileLocation"],
    )
    inode_store = InodeStore(config["sqliteFileLocation"])
    cache = Cache(
        cache_folder=args.cache_folder, inode_store=inode_store, api=api
    )
    filesystem = Filesystem(cache)
    FUSE(filesystem, args.mountpoint, nothreads=True, foreground=True)


def worker_clean_watcher(args, config):
    print("Starting worker main")

    # TODO: Extract different worker roles into their own processes

    api = FileAPI(
        account_id=config["accountId"],
        application_key=config["applicationKey"],
        bucket_id=config["bucketId"],
        db_file=config["sqliteFileLocation"],
    )

    inode_store = InodeStore(config["sqliteFileLocation"])
    # rank_store = RankStore(config["sqliteFileLocation"])
    # ranker = Ranker(rank_store, inode_store)
    cache = Cache(
        cache_folder=args.cache_folder, inode_store=inode_store, api=api
    )
    worker = Worker(
        cache=cache,
        # ranker=ranker,
        api=api,
        target_disk_usage=config["targetDiskUsage"],
    )

    worker.run_clean_watcher()


# TODO: subcribe Ranker to events
# TODO: create loop to pull and handle events in Ranker
# - or maybe this Ranker handling stuff should go into yet another process?


def worker_delete_watcher(args, config):
    print("Starting delete watcher")

    api = FileAPI(
        account_id=config["accountId"],
        application_key=config["applicationKey"],
        bucket_id=config["bucketId"],
        db_file=config["sqliteFileLocation"],
    )

    inode_store = InodeStore(config["sqliteFileLocation"])
    # rank_store = RankStore(config["sqliteFileLocation"])
    # ranker = Ranker(rank_store, inode_store)
    cache = Cache(
        cache_folder=args.cache_folder, inode_store=inode_store, api=api
    )
    worker = Worker(
        cache=cache,
        # ranker=ranker,
        api=api,
        target_disk_usage=config["targetDiskUsage"],
    )

    worker.run_delete_watcher()


def reset_all():
    import shutil
    import os

    args = parse_args()
    config = get_config()
    shutil.rmtree(args.cache_folder)
    os.mkdir(args.cache_folder)
    os.remove(config["sqliteFileLocation"])
    os.remove(QUEUE_DB_NAME)
