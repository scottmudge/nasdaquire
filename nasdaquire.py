import os
import datetime
import time
import urllib
import urllib.request
import shutil
import pandas as pd
import io

class QueryBase:
    """The base class for all queries. This will do the base work for interfacing with the web and also store
    information that will be used by all child classes.
    """

    _REQUEST_ROOT = "https://charting.nasdaq.com/ext/charts.dll?"

    def __init__(self, symbol):
        self.symbol = symbol.upper()
        self.request_url = ""
        self.output_filename = ""
        self.downloaded_file = ""
        self.file_saved = False
        self.file_buffer = None

    def download_data(self):
        """Downloads the data and returns a string containing its path."""

        if len(self.request_url) < 10:
            raise ValueError("Request URL is too short")

        root = os.path.dirname(os.path.realpath(__file__)) + "\\..\\temp_data\\"

        if not os.path.exists(root):
            os.makedirs(root)

        if len(self.output_filename) < 3:
            timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d_%H-%M-%S')
            self.downloaded_file = root + self.symbol + "_" + timestamp + ".csv"
        else:
            self.downloaded_file = root + self.symbol + "_" + self.output_filename + ".csv"

        with urllib.request.urlopen(self.request_url) as response, open(self.downloaded_file, 'wb') as out_file:
            s = response.fp.read()
            bts = s.replace(b',', b'')
            bts = bts.replace(b'\t\r', b'\r')
            bts = bts.replace(b'\t', b',')
            self.file_buffer = io.BytesIO(bts)
            out_file.write(bts)
            out_file.close()

        if not os.path.exists(self.downloaded_file):
            raise OSError("Could not save file.")

        self.file_saved = True

        return self.downloaded_file
            
    def get_pandas_dataframe(self):
        """Returns a pandas data frame for use by anything which supports pandas."""
        if not self.file_saved:
            raise RuntimeError("No data yet acquired. Please acquire data first.")

        dataframe = pd.read_csv(self.file_buffer, parse_dates=['Time'])
        dataframe=dataframe.reindex(index=dataframe.index[::-1])
        return dataframe


class MinuteQuery(QueryBase):
    """This returns 1-minute resolution data for a supplied symbol.

    The data should ideally be 1-minute resolution, but there may be missing minutes here and there.

    Returned are timestamp (HH:MM), Price, and Volume
    """

    def __init__(self, symbol, start_time, end_time):
        QueryBase.__init__(self, symbol)
        # Do some checks

        if time_delta_seconds(start_time, end_time) <= 0:
            raise RuntimeError("Supplied time range is negative.")

        # Assign variables
        self.start_time = start_time
        self.end_time = end_time

        starttime_str = self.start_time.strftime('%M%S')
        endtime_str = self.end_time.strftime('%M%S')

        subroot_str = "2-1-17-0-0,0,0,0,0|0,0,0,0,0|0,0,0,0,0|0,0,0,0,0|0,0,0,0,0|0,0,0,0,0|0,0,0,0,0|0,0,0,0,0|0,0," \
                      "0,0,0|0,0,0,0,0|0,0,0,0,0|0,0,0,0,0|0,0,0,0,0|0,0,0,0,0|0,0,0,0,0|0,0,0,0,0|0,0,0,0,0|0,0,0,0," \
                      "0|0,0,0,0,0|0,0,0,0,0|0,0,0,0,0|0,0,0,0,0|0,0,0,0,0|0,0,0,0,0|0,0,0,0,0|0,0,0,0,0|0,0,0,0,0|0," \
                      "0,0,0,0|0,0,0,0,0|0,0,0,0,0|0,0,0,0,0|0,0,0,0,0|0,0,0,0,0|0,0,0,0,0|0,0,0,0,0|0,0,0,0,0|0,0,0," \
                      "0,0|0,0,0,0,0|0,0,0,0,0|0,0,0,0,0-0"
        postamble_str = "-BG=FFFFFF-BT=0-&WD=635-HT=395---XXCL-"

        self.request_url = self._REQUEST_ROOT + subroot_str + self.start_time.strftime('%H%M') + \
                           self.end_time.strftime('%H%M') + "-03NA000000" + self.symbol + postamble_str

        timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d')

        self.output_filename = timestamp + "_" + self.start_time.strftime('%H%M') + self.end_time.strftime('%H%M')

        self.download_data()


def time_delta_seconds(t1, t2):
    """Returns the number of seconds between two datetime.time objects"""
    t1_s = (t1.hour * 60 * 60 + t1.minute * 60 + t1.second)
    t2_s = (t2.hour * 60 * 60 + t2.minute * 60 + t2.second)

    return max([t1_s, t2_s]) - min([t1_s, t2_s])
