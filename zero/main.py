
import time

from fuse import FUSE


from .operations import Filesystem
from .state_store import StateStore
from .inode_store import InodeStore
from .cache import Cache
from .worker import Worker
from .b2_api import FileAPI
from .ranker import Ranker
from .rank_store import RankStore

import multiprocessing


from .config_utils import get_config, parse_args

TARGET_DISK_USAGE = 0.01  # GB


def main():

    args = parse_args()
    config = get_config()

    fuse = multiprocessing.Process(
        name="fuse", target=fuse_main, args=(args, config)
    )
    worker = multiprocessing.Process(
        name="worker", target=worker_main, args=(args, config)
    )

    fuse.start()
    worker.start()


def fuse_main(args, config):

    api = FileAPI(
        account_id=config["accountId"],
        application_key=config["applicationKey"],
        bucket_id=config["bucketId"],
        db_file=config["sqliteFileLocation"],
    )
    state_store = StateStore(config["sqliteFileLocation"])
    inode_store = InodeStore(config["sqliteFileLocation"])
    cache = Cache(
        cache_folder=args.cache_folder,
        state_store=state_store,
        inode_store=inode_store,
        api=api,
    )
    filesystem = Filesystem(cache)
    FUSE(filesystem, args.mountpoint, nothreads=True, foreground=True)


def worker_main(args, config):

    api = FileAPI(
        account_id=config["accountId"],
        application_key=config["applicationKey"],
        bucket_id=config["bucketId"],
        db_file=config["sqliteFileLocation"],
    )

    state_store = StateStore(config["sqliteFileLocation"])
    inode_store = InodeStore(config["sqliteFileLocation"])
    rank_store = RankStore(config["sqliteFileLocation"])
    ranker = Ranker(rank_store, inode_store)
    cache = Cache(
        cache_folder=args.cache_folder,
        state_store=state_store,
        inode_store=inode_store,
        api=api,
    )
    worker = Worker(
        cache=cache,
        ranker=ranker,
        api=api,
        target_disk_usage=config["targetDiskUsage"],
    )

    while True:
        worker.run()
        time.sleep(10)

    # TODO: subcribe Ranker to events
    # TODO: create loop to pull and handle events in Ranker
    # - or maybe this Ranker handling stuff should go into yet another process?


def reset_all():
    import shutil
    import os

    args = parse_args()
    config = get_config()
    shutil.rmtree(args.cache_folder)
    os.mkdir(args.cache_folder)
    os.remove(config["sqliteFileLocation"])
