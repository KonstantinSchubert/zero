
import time

from fuse import FUSE


from .operations import Filesystem
from .cache import Cache
from .cache_management.balancer import Balancer
from .cache_management.ranker import Ranker
from .cache_management.rank_store import RankStore
from .b2_api import FileAPI
from .deleter import Deleter
from .cleaner import Cleaner
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
        name="deleter", target=delete_watcher, args=(args, config)
    )

    cleaner = multiprocessing.Process(
        name="cleaner", target=clean_watcher, args=(args, config)
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
    cache = Cache(cache_folder=args.cache_folder, api=api)
    filesystem = Filesystem(cache)
    FUSE(
        filesystem,
        args.mountpoint,
        nothreads=True,
        foreground=True,
        big_writes=True,
    )


def clean_watcher(args, config):
    print("Starting cleaner")

    # TODO: Extract different worker roles into their own processes

    api = FileAPI(
        account_id=config["accountId"],
        application_key=config["applicationKey"],
        bucket_id=config["bucketId"],
        db_file=config["sqliteFileLocation"],
    )

    # rank_store = RankStore(config["sqliteFileLocation"])
    # ranker = Ranker(rank_store, inode_store)
    cleaner = Cleaner(cache_folder=args.cache_folder, api=api)

    cleaner.run_watcher()


# TODO: subcribe Ranker to events
# TODO: create loop to pull and handle events in Ranker
# TODO: Ranker creates ranking, also sometimes scans the files in case he doens't correctly keep track of all events (folder moves)
# TODO: Balancer balances files based on ranking by ranker
# TODO: Balancer and ranker could also be one thing


def delete_watcher(args, config):
    print("Starting deleter")

    api = FileAPI(
        account_id=config["accountId"],
        application_key=config["applicationKey"],
        bucket_id=config["bucketId"],
        db_file=config["sqliteFileLocation"],
    )

    deleter = Deleter(api=api)
    deleter.run_watcher()


def reset_all():
    import shutil
    import os

    args = parse_args()
    config = get_config()
    shutil.rmtree(args.cache_folder)
    os.mkdir(args.cache_folder)
    os.remove(config["sqliteFileLocation"])
    os.remove(QUEUE_DB_NAME)
