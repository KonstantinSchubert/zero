import unittest
from ...events import FileAccessEvent


class EventTest(unittest.TestCase):

    def test_can_submit_event(self):
        FileAccessEvent.submit(path="/hello/to/this.path")
