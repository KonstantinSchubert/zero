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

    converter = PathConverter(args.cache_folder)
    cache = Cache(converter)
    worker = Worker(cache)
    filesystem = Filesystem(cache)
    FUSE(
        filesystem, args.mountpoint, nothreads=True, foreground=True, debug=True
    )


if __name__ == "__main__":
    main()
