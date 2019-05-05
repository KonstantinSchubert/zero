import argparse
import yaml
from os.path import expanduser


def get_config():
    with open(expanduser("~/.config/zero/config.yml"), "r") as config:
        return yaml.load(config)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("mountpoint", type=str, help="Mountpoint")
    parser.add_argument("cache_folder", type=str, help="Cache folder")
    return parser.parse_args()
