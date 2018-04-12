from fuse import FUSE

from operations import Filesystem
from cache import Cache
from b2_api import FileAPI

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("mountpoint", type=str, help="Mountpoint for the B2 bucket")
    args = parser.parse_args()



def main():

    api = FileAPI(account_info, account_id, application_key, bucket_id)
    cache = Cache(cache_folder)
    filesystem = Filesystem(api, cache)
    FUSE(filesystem, args.mountpoint, nothreads=True, foreground=True)
