import unittest
import time
from zero.file_utils import open_without_changing_times, get_stat_dictionary
from ..asserts import assert_stat_unequal, assert_stat_equal

TESTFILE = "/tmp/testfile"


class FileUtilsTest(unittest.TestCase):

    def access_file(self, context_manager):
        with context_manager(TESTFILE, "w+") as f:
            f.write("hi")

    def test_open_file_without_changing_times(self):
        self.access_file(open)

        # assert that normal open changes stat:
        stat = get_stat_dictionary(TESTFILE)
        time.sleep(0.1)
        self.access_file(open)
        assert_stat_unequal(stat, get_stat_dictionary(TESTFILE))

        # assert that special open does not change stat:
        stat = get_stat_dictionary(TESTFILE)
        print(stat)
        time.sleep(0.1)
        self.access_file(open_without_changing_times)
        assert_stat_equal(stat, get_stat_dictionary(TESTFILE))
