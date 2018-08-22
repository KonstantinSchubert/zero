import unittest
from zero.path_utils import yield_partials


class PathUtilsTest(unittest.TestCase):

    def test_yield_partials(self):
        path = "/a/b/c"
        partials = ["/a", "/a/b", "/a/b/c"]

        assert list(yield_partials(path)) == partials
