import cbpro

from influxdb import InfluxDBClient
import configparser
import pathlib
import datetime

import source.json_formatters as formatter

#TODO rename to download data, make configurable to dif products, add date range, add pick up where left off
# Add create db if not exist
# If exception try again



class CoinbaseToInfluxDBDownloader:
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read(pathlib.Path(__file__).parent / 'config.ini')

        self.db_client_connection = InfluxDBClient(host=self.config['INFLUXDB']['SERVER'],
                                                   port=int(self.config['INFLUXDB']['PORT']),
                                                   username=self.config['INFLUXDB']['USER'],
                                                   password=self.config['INFLUXDB']['PWD'])

        self.public_client = cbpro.PublicClient()

    def DownloadHistoricRates(self,
                              start_time=datetime.datetime(2018, 8, 10, 16, 0, 0),
                              end_time=datetime.datetime.today(),
                              coinbase_product='BTC-USD',
                              granularity=60):
        """
        This is the main method to call to execute a transfer of coinbase's historic rates
        to and influxdb instance.

        Data points stored in influxDB for this transaction are known as "candles."
        The schema for the candles is as follows

            "measurement": coinbase_product ('BTC-USD', 'LTC-USD', 'BTC-EUR', etc.),
            "time": ISO 8601,
            "fields": {
                "low": float,
                "high": float,
                "open": float,
                "close": float,
                "volume": float
                }

        """

        for start_time in self._minutessrange(start_time, end_time):

            # For one minute granularity generate a start and end time with a delta of 300 minutes
            # This corresponds to the 300 data point per transaction limit imposed by Coinbase
            end_time = start_time + datetime.timedelta(minutes=300)

            print("Processing time slice: ", start_time, "-", end_time)

            # Sometimes a transaction to coinbase fails for various reasons
            # In that case attempt the transaction again up to 5 times
            while True:
                transaction_tries = 0

                try:
                    candles = self.public_client.get_product_historic_rates(product_id=coinbase_product,
                                                                            start=start_time,
                                                                            end=end_time,
                                                                            granularity=granularity)

                    for candle in candles:
                        self.db_client_connection.write_points(
                                                    database='coinbase-historic-rates',
                                                    points=formatter.CANDLE_TO_INFLUXDB_JSON(coinbase_product, candle),
                                                    time_precision='s')

                except Exception as error:
                    print(str(error))

                    transaction_tries += 1
                    if transaction_tries > 5:
                        break

                    print("Retrying transaction (attempt #", transaction_tries, ")...")

    def _daterange(self, start_date, end_date):
        for n in range(int((end_date - start_date).days)):
            yield start_date + datetime.timedelta(n)

    def _minutessrange(self, start_time, end_time):
        for n in range(0, int((end_time - start_time).days) * 24 * 60, 300):
            yield start_time + datetime.timedelta(minutes=n)