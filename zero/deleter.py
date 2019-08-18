import logging
import time
from .events import EventListener, FileDeleteEvent

logger = logging.getLogger("spam_application")


class Deleter:

    def __init__(self, api):
        self.api = api

    def run_watcher(self):
        with EventListener((FileDeleteEvent.topic,)) as deletion_listener:
            while True:
                time.sleep(1)
                for message in deletion_listener.yield_events():
                    uuid = message["uuid"]
                    # TODO: the message must contain the uuid of the file to be deleted already,
                    # since the path may no longer exist.
                    if uuid is not None:
                        self.api.delete(uuid)
