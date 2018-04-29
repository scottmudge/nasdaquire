from datetime import time
from datetime import timedelta


class QueryBase:
    """The base class for all queries. This will do the base work for interfacing with the web and also store
    information that will be used by all child classes.
    """

    def __init__(self, symbol):
        self.symbol = symbol



class MinuteQuery(QueryBase):
    """This returns 1-minute resolution data for a supplied symbol.

    The data should ideally be 1-minute resolution, but there may be missing minutes here and there.
    """

    def __init__(self, symbol, start_time, end_time):
        QueryBase.__init__(self, symbol)
        # Do some checks
        time_delta = timedelta(end_time - start_time)
        if time_delta.seconds < 0:
            raise RuntimeError("Supplied time range is negative.")

        # Assign variables
        self.start_time = time(start_time)
        self.end_time = time(end_time)


