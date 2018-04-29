import os
import datetime
import time
import urllib
import urllib.request
import pandas as pd
import io
import json
from enum import Enum


class Exchange(Enum):
    NYSE = 1
    AMEX = 2
    NASDAQ = 3
    UNKNOWN = 0


class SymbolInfo:
    """This class stores information on a symbol"""

    def __init__(self):
        self.symbol = ""
        self.name = ""
        self.last_sale = 0
        self.market_cap = 0
        self.ipo_year = 0
        self.sector = ""
        self.industry = ""
        self.exchange = Exchange.UNKNOWN


class SymbolDatabase:
    """This downloads and stores information on all symbols available on NYSE, AMEX, and NASDAQ"""

    NASDAQ_URL = "https://www.nasdaq.com/screening/companies-by-name.aspx?exchange=NASDAQ&render=download"
    AMEX_URL = "https://www.nasdaq.com/screening/companies-by-name.aspx?exchange=AMEX&render=download"
    NYSE_URL = "https://www.nasdaq.com/screening/companies-by-name.aspx?exchange=NYSE&render=download"

    def __init__(self):
        self.total_number_of_symbols = 0
        self.last_updated = datetime.datetime(1900,1,1,0,0,0,0)
        self.root_dir = os.path.dirname(os.path.realpath(__file__)) + "\\..\\data\\"
        self.database_file = self.root_dir + "symbol_database.csv"
        self.database_metadata_file = self.root_dir + "db_metadata.csv"
        self.new_database = False
        self.database = None
        self.updated = False
        self._force_update = False

        if not os.path.exists(self.database_metadata_file):
            self.new_database = True

        self.update_database()

    def force_update(self):
        self._force_update = True
        self.force_update()
        self._force_update = False

    def update_database(self):

        if self._force_update:
            self.updated = False

        if self.updated:
            return

        need_update = self._force_update

        if self.new_database:
            need_update = True
        elif not need_update:
            parsed_df = pd.DataFrame.from_csv(self.database_metadata_file, parse_dates=['LastUpdated'])
            parsed_df.reset_index(level=0, inplace=True)
            last_updated_dt = parsed_df.at[0,'LastUpdated']
            time_delt = datetime.datetime.now() - last_updated_dt

            if time_delt.days > 1 or (time_delt.seconds/60/60) > 8:
                days = int(time_delt.days)
                hours = int(time_delt.seconds / 60 / 60)
                minutes = int(time_delt.seconds / 60)
                print("Updating stale symbol database -- last updated {0} days, {1} hours and {2} minutes ago").\
                    format(days, hours, minutes)
                need_update = True

        df = pd.DataFrame({
            'LastUpdated': pd.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'TotalSymbols': [self.total_number_of_symbols]
        })

        df.to_csv(self.database_metadata_file, mode='w', index=False)

        self.updated = True



symbol_db = SymbolDatabase()


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

        root = os.path.dirname(os.path.realpath(__file__)) + "\\..\\data\\"

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
        postamble_str = '-BG=FFFFFF-BT=0-&WD=635-HT=395---XXCL-'

        self.request_url = self._REQUEST_ROOT + subroot_str + self.start_time.strftime('%H%M') + \
                           self.end_time.strftime('%H%M') + "-03NA000000" + self.symbol + postamble_str

        timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d')

        self.output_filename = timestamp + "_" + self.start_time.strftime('%H%M') + self.end_time.strftime('%H%M')

        self.download_data()

    def get_pandas_dataframe(self):
        """Returns a pandas data frame for use by anything which supports pandas."""
        if not self.file_saved:
            raise RuntimeError("No data yet acquired. Please acquire data first.")

        dataframe = pd.read_csv(self.file_buffer, parse_dates=['Time'])
        dataframe = dataframe.reindex(index=dataframe.index[::-1])
        return dataframe


def time_delta_seconds(t1, t2):
    """Returns the number of seconds between two datetime.time objects"""
    t1_s = (t1.hour * 60 * 60 + t1.minute * 60 + t1.second)
    t2_s = (t2.hour * 60 * 60 + t2.minute * 60 + t2.second)

    return max([t1_s, t2_s]) - min([t1_s, t2_s])
