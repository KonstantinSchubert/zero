from fuse import FUSE
import argparse

from .operations import Filesystem
from .cache import Cache
from .worker import Worker
from .paths import PathConverter
from .b2_api import FileAPI
from .b2_real_credentials import account_id, application_key, bucket_id


def parse_fuse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("mountpoint", type=str, help="Mountpoint")
    parser.add_argument("cache_folder", type=str, help="Cache folder")
    return parser.parse_args()


def parse_worker_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("cache_folder", type=str, help="Cache folder")
    return parser.parse_args()


def fuse_main():

    args = parse_fuse_args()

    api = FileAPI(
        account_id=account_id,
        application_key=application_key,
        bucket_id=bucket_id,
    )

    converter = PathConverter(args.cache_folder)
    cache = Cache(converter)
    filesystem = Filesystem(cache)
    FUSE(
        filesystem, args.mountpoint, nothreads=True, foreground=True, debug=True
    )


def worker_main():

    args = parse_worker_args()
    api = FileAPI(
        account_id=account_id,
        application_key=application_key,
        bucket_id=bucket_id,
    )

    converter = PathConverter(args.cache_folder)
    cache = Cache(converter)
    worker = Worker(cache, api)
    worker.run()


if __name__ == "__main__":
    main()
