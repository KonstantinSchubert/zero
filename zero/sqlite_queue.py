import sqlite3

"""
A simple multi-recipient message queue.
The code is not opimized for performance, in particular
receiving messages could probably be optimized quite a bit.
"""

DB_NAME = "/home/kon/zero/queue.sqlite3"


class NoNextMessage(Exception):
    pass


class _MessageTable:

    def __init__(self, db_name):
        self.connection = sqlite3.connect(db_name, timeout=5)
        with self.connection:
            self.connection.execute(
                """CREATE TABLE IF NOT EXISTS messages (id integer primary key, topic text, message text)"""
            )

    def get_next_message(self, last_message_id, topic):
        with self.connection:
            cursor = self.connection.execute(
                """SELECT id, message FROM messages WHERE id > ? AND topic = ? ORDER BY id LIMIT 1""",
                (last_message_id, topic),
            )
            result = cursor.fetchone()
        if result is None:
            raise NoNextMessage
        else:
            return result

    def add_message(self, topic, message):
        with self.connection:
            self.connection.execute(
                """INSERT INTO messages (topic, message) VALUES (?,?)""",
                (topic, message),
            )

    def delete_messages_older_than_id(self, message_id):
        with self.connection:
            self.connection.execute(
                """DELETE from messages WHERE id < ?""", (message_id,)
            )


class _SubscriberTable:

    def __init__(self, db_name):
        self.connection = sqlite3.connect(db_name, timeout=5)
        with self.connection:
            self.connection.execute(
                """CREATE TABLE IF NOT EXISTS subscribers (id integer primary key autoincrement, last_received_message integer, topic text)"""
            )

    def add_subscriber(self, topic):
        # Inserts row if it does not exist
        with self.connection:
            cursor = self.connection.cursor()
            cursor.execute(
                """INSERT INTO subscribers (last_received_message, topic) VALUES (0, ?)""",
                (topic,),
            )
            subscriber_id = cursor.lastrowid
            return subscriber_id

    def remove_subscriber(self, subscriber_id):
        self.connection.execute(
            """DELETE from subscribers WHERE id = ?""", (subscriber_id,)
        )

    def set_id_of_last_received_message(self, subscriber_id, message_id):
        with self.connection:
            self.connection.execute(
                """UPDATE subscribers SET last_received_message = ? WHERE id = ?""",
                (message_id, subscriber_id),
            )

    def get_subscriber_status(self, subscriber_id):
        with self.connection:
            cursor = self.connection.execute(
                """SELECT last_received_message, topic FROM subscribers WHERE id = ?""",
                (subscriber_id,),
            )
            result = cursor.fetchone()
            if result is None:
                raise Exception(
                    "This should not happen. Subscriber must exist!"
                )
        return result

    def get_id_of_oldest_received_message(self):
        with self.connection:
            cursor = self.connection.execute(
                """SELECT MIN(last_received_message) FROM subscribers"""
            )
            return cursor.fetchone()[0]


# Public methods and classes


def publish_message(topic, message):
    _message_table = _MessageTable(DB_NAME)
    _message_table.add_message(topic=topic, message=message)


def get_next_message(subscriber_id):
    _subscriber_table = _SubscriberTable(DB_NAME)
    _message_table = _MessageTable(DB_NAME)
    id_of_last_received_message, topic = _subscriber_table.get_subscriber_status(
        subscriber_id
    )
    message_id, message = _message_table.get_next_message(
        id_of_last_received_message, topic
    )
    _subscriber_table.set_id_of_last_received_message(
        subscriber_id=subscriber_id, message_id=message_id
    )
    return message


def purge_messages():
    _subscriber_table = _SubscriberTable(DB_NAME)
    _message_table = _MessageTable(DB_NAME)
    oldest_received_message_id = (
        _subscriber_table.get_id_of_oldest_received_message()
    )
    _message_table.delete_messages_older_than_id(oldest_received_message_id)


def register_subscriber(topic):
    _subscriber_table = _SubscriberTable(DB_NAME)
    subscriber_id = _subscriber_table.add_subscriber(topic)
    return subscriber_id


def unregister_subscriber(subscriber_id):
    _subscriber_table = _SubscriberTable(DB_NAME)
    _subscriber_table.remove_subscriber(subscriber_id)


class Listener:

    def __init__(self, topic):
        self.topic = topic

    def __enter__(self):
        self.subscriber_id = register_subscriber(self.topic)
        return self

    def __exit__(self, *args):
        unregister_subscriber(self.subscriber_id)

    def yield_messages(self):
        try:
            while True:
                yield get_next_message(self.subscriber_id)
                purge_messages()  # This can of course be done more rarely
        except NoNextMessage:
            pass
