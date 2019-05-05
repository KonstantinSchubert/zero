import sqlite_queue as message_queue
import json


class Event:
    TOPIC = ...
    arguments = ...

    def submit(self, **kwargs):
        kwargs = {
            key: value for key, value in kwargs.items() if key in self.arguments
        }
        message_queue.publish_message(self.TOPIC, json.dumps(kwargs))


class FileAccessEvent:
    TOPIC = "FILE_WAS_ACCESSED"
    arguments = ["path"]


class FileDeleteEvent:
    TOPIC = "FILE_WAS_DELETED"
    arguments = ["path"]


class EventListener(message_queue.Listener):
    """
    Use this context manager to listen to events on the queue
    """

    def yield_events(self):
        yield from self.yield_messages()
