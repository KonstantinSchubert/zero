
from fuse import FUSE


from .operations import Filesystem
from .cache import Cache
from .cache_management.balancer import Balancer
from .cache_management.ranker import Ranker
from .b2_api import FileAPI
from .deleter import Deleter
from .cleaner import Cleaner
from .events import get_rabbitmq

import multiprocessing


from .config_utils import get_config, parse_args

TARGET_DISK_USAGE = 0.001  # GB


def main():

    args = parse_args()
    config = get_config()

    fuse = multiprocessing.Process(
        name="fuse", target=fuse_main, args=(args, config)
    )
    ranker_watcher = multiprocessing.Process(
        name="ranker_watcher", target=ranker_events_watcher, args=(args, config)
    )

    # ranker_scanner = multiprocessing.Process(
    #     name="ranker_scanner", target=ranker_scanner, args=(args, config)
    # )

    balancer = multiprocessing.Process(
        name="ranker_watcher", target=run_balancer, args=(args, config)
    )

    deleter = multiprocessing.Process(
        name="deleter", target=delete_watcher, args=(args, config)
    )

    cleaner = multiprocessing.Process(
        name="cleaner", target=clean_watcher, args=(args, config)
    )

    fuse.start()
    deleter.start()
    cleaner.start()
    ranker_watcher.start()
    # ranker_scanner.start()
    balancer.start()


def fuse_main(args, config):
    print("Starting fuse main")
    _, events_channel = get_rabbitmq()
    api = FileAPI(
        account_id=config["accountId"],
        application_key=config["applicationKey"],
        bucket_id=config["bucketId"],
        db_file=config["sqliteFileLocation"],
    )
    cache = Cache(
        cache_folder=args.cache_folder, api=api, events_channel=events_channel
    )
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

    api = FileAPI(
        account_id=config["accountId"],
        application_key=config["applicationKey"],
        bucket_id=config["bucketId"],
        db_file=config["sqliteFileLocation"],
    )

    cleaner = Cleaner(cache_folder=args.cache_folder, api=api)
    cleaner.run_watcher()


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


def ranker_events_watcher(args, config):
    print("Starting ranker (event watcher)")
    ranker = Ranker(
        db_file=config["sqliteFileLocation"], cache_folder=args.cache_folder
    )
    ranker.watch_events()


def ranker_scanner(args, config):
    print("Starting ranker (scanner)")
    ranker = Ranker(
        db_file=config["sqliteFileLocation"], cache_folder=args.cache_folder
    )
    ranker.scan()


def run_balancer(args, config):
    print("Starting balancer")
    _, events_channel = get_rabbitmq()
    api = FileAPI(
        account_id=config["accountId"],
        application_key=config["applicationKey"],
        bucket_id=config["bucketId"],
        db_file=config["sqliteFileLocation"],
    )
    cache = Cache(
        cache_folder=args.cache_folder, api=api, events_channel=events_channel
    )
    balancer = Balancer(
        cache=cache,
        api=api,
        target_disk_usage=TARGET_DISK_USAGE,
        db_file=config["sqliteFileLocation"],
    )
    balancer.run()


def reset_all():
    import shutil
    import os

    args = parse_args()
    config = get_config()
    shutil.rmtree(args.cache_folder)
    os.mkdir(args.cache_folder)
    os.remove(config["sqliteFileLocation"])
