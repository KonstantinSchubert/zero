import zero.sqlite_queue as message_queue
import json


class Event:
    topic = ...
    arguments = ...

    @classmethod
    def submit(cls, **kwargs):
        if set(kwargs.keys()) != set(cls.arguments):
            # When I translate this code to a statically-typed language, this check should happen at compile time.
            raise Exception(
                "Submit method of {topic} events is expecting the following arguments: {arguments}"
            )
        kwargs = {
            key: value for key, value in kwargs.items() if key in cls.arguments
        }
        message_queue.publish_message(cls.topic, json.dumps(kwargs))


class FileAccessEvent(Event):
    topic = "FILE_WAS_ACCESSED"
    arguments = ["path"]


class FileUpdateOrCreateEvent(Event):
    topic = "FILE_WAS_UPDATED_OR_CREATED"
    arguments = ["path"]


class FileDeleteEvent(Event):
    topic = "FILE_WAS_DELETED"
    arguments = ["path", "uuid"]


class FileRenameOrMoveEvent(Event):
    topic = "FILE_WAS_RENAMED_OR_MOVED"
    arguments = ["old_path", "new_path"]
    # TODO: Make sure to submit this even where needed


class FolderRenameOrMoveEvent(Event):
    topic = "FOLDER_WAS_RENAMED_OR_MOVED"
    arguments = ["old_path", "new_path"]
    # TODO: Make sure to submit this even where needed


class FileEvictedFromCacheEvent(Event):
    topic = "FILE_WAS_EVICTED_FROM_CACHE"
    arguments = ["path"]
    # TODO: Make sure to submit this even where needed


class FileLoadedIntoCacheEvent(Event):
    topic = "FILE_WAS_LOADED_INTO_THE_CACHE"
    arguments = ["path"]
    # TODO: Make sure to submit this even where needed
    # THe current plan is that this event is only fired when a dummy
    # is replaced, not when the file is initially created.


class EventListener(message_queue.Subscriber):
    """
    Use this context manager to listen to events on the queue
    """

    def yield_events(self):
        yield from self.yield_messages()
