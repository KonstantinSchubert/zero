import json
import pika
import logging
from multiprocessing import current_process

log = logging.getLogger(current_process().name)


def get_rabbitmq():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host="localhost")
    )
    channel = connection.channel()
    channel.exchange_declare(exchange="events", exchange_type="direct")
    return connection, channel


def close_rabbitmq(connection, channel):
    channel.close()
    connection.close()


def register_subscriber(channel, topics):
    result = channel.queue_declare(queue="", exclusive=True)
    queue_name = result.method.queue
    for topic in topics:
        channel.queue_bind(
            exchange="events", queue=queue_name, routing_key=topic
        )
    return queue_name


def unregister_subscriber(channel):
    number_or_requeued_messages = channel.cancel()
    print(f"Requeued {number_or_requeued_messages} messages")


class EventListener:

    def __init__(self, topics):
        self.topics = topics

    def __enter__(self):
        # Creating queue connection
        self.connection, self.channel = get_rabbitmq()
        self.queue_name = register_subscriber(self.channel, self.topics)
        return self

    def __exit__(self, *args):
        unregister_subscriber(self.channel)
        close_rabbitmq(connection=self.connection, channel=self.channel)

    def yield_events(self):
        for method_frame, properties, body in self.channel.consume(
            queue=self.queue_name
        ):
            message = json.loads(body)
            # print(f"Yielding message {message}")
            yield message
            self.channel.basic_ack(method_frame.delivery_tag)


class Event:
    topic = ...
    arguments = ...

    def __init__(self, channel):
        self.channel = channel

    def submit(self, **kwargs):
        if set(kwargs.keys()) != set(self.arguments):
            # When I translate this code to a statically-typed language, this check should happen at compile time.
            raise Exception(
                f"Submit method of {self.topic} events is expecting the following arguments: {self.arguments}"
            )
        kwargs = {
            key: value for key, value in kwargs.items() if key in self.arguments
        }
        # This is a bit of a hack, I could get the topic also from rabbitmq
        kwargs["topic"] = self.topic

        self.channel.basic_publish(
            exchange="events", routing_key=self.topic, body=json.dumps(kwargs)
        )


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
