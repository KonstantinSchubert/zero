from fuse import FUSE
import argparse

from .operations import Filesystem
from .cache import Cache
from .worker import Worker
from .paths import PathConverter
from .b2_api import FileAPI
from .b2_real_credentials import account_id, application_key, bucket_id


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("mountpoint", type=str, help="Mountpoint")
    parser.add_argument("cache_folder", type=str, help="Cache folder")
    return parser.parse_args()


def main():

    args = parse_args()

    api = FileAPI(
        account_id=account_id,
        application_key=application_key,
        bucket_id=bucket_id,
    )
    converter = PathConverter(args.cache_folder)
    worker = Worker(converter, api)
    cache = Cache(converter, worker)
    filesystem = Filesystem(cache)
    FUSE(
        filesystem, args.mountpoint, nothreads=True, foreground=True, debug=True
    )


if __name__ == "__main__":
    main()
