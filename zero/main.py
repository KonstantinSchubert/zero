from fuse import FUSE
import argparse

from .operations import Filesystem
from .cache import Cache
from .b2_api import FileAPI

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("mountpoint", type=str, help="Mountpoint")
    parser.add_argument("cache_folder", type=str, help="Cache folder")
    return parser.parse_args()



def main():

    args = parse_args()

    api = FileAPI(account_info=None, account_id=None, application_key=None, bucket_id=None)
    cache = Cache(args.cache_folder)
    filesystem = Filesystem(api, cache)
    FUSE(filesystem, args.mountpoint, nothreads=True, foreground=True)


if __name__ == '__main__':
    main()