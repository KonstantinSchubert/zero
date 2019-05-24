import zero.sqlite_queue as message_queue
import json


class Event:
    topic = ...
    arguments = ...

    @classmethod
    def submit(cls, **kwargs):
        kwargs = {
            key: value for key, value in kwargs.items() if key in cls.arguments
        }
        message_queue.publish_message(cls.topic, json.dumps(kwargs))


class FileAccessEvent(Event):
    topic = "FILE_WAS_ACCESSED"
    arguments = ["path"]


class FileDeleteEvent(Event):
    topic = "FILE_WAS_DELETED"
    arguments = ["path", "uuid"]


class EventListener(message_queue.Listener):
    """
    Use this context manager to listen to events on the queue
    """

    def yield_events(self):
        yield from self.yield_messages()
