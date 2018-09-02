def assert_stat_equal(first, second):
    # We are not managing the ctime at the moment
    # because it can only be written by the system
    # and we would have to permanently obtain it from a different
    # storage place, such a meta data file
    first = first.copy()
    second = second.copy()
    first.pop("st_ctime")
    second.pop("st_ctime")
    assert first == second


def assert_stat_unequal(first, second):
    # We are not managing the ctime at the moment
    # because it can only be written by the system
    # and we would have to permanently obtain it from a different
    # storage place, such a meta data file
    first = first.copy()
    second = second.copy()
    first.pop("st_ctime")
    second.pop("st_ctime")
    assert first != second
