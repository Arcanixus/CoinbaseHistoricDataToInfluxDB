import cbpro

from influxdb import InfluxDBClient
import datetime
import time

import source.json_formatters as formatter
import source.time_utils as time_utils

import logging

#TODO add pick up where left off
# Add create db if not exist
# Create an update routine that can be run as a daemon

class CoinbaseHistoricDataToInfluxDB:
    """
    A utility program that wraps the cbpro and influxdb python libraries.
    Its primary purpose is to provide a simple interface for downloading various
    historic data from coinbase and storing that data directly into influxdb databases.
    This allows one to pull the coinbase data into a local storage facility
    where it can be analyzed and manipulated more easily.

    :param host: The hostname/URL of the InfluxDB instance this object should use.
    :param port: The port number of the InfluxDB instance.
    :param username: The username of the InfluxDB instance.
    :param password: The password of the InfluxDB instance.
    """

    def __init__(self, host='localhost', port=8086, username='root', password='root', verbose=False):
        self._db_client_connection = InfluxDBClient(host=host, port=port, username=username, password=password)
        self._public_client = cbpro.PublicClient()
        self.verbose = verbose

    def __del__(self):
        self._db_client_connection.close()

    def download_historic_rates(self,
                                start_time=datetime.datetime(2017, 1, 1, 0, 0, 0),
                                end_time=datetime.datetime.today(),
                                coinbase_product='BTC-USD',
                                granularity=60):
        """
        Call this method to execute a transfer of coinbase's historic rates
        to and influxdb instance.

        Data points stored in influxDB for this transaction are known as "candles."
        The schema for the candles is as follows

            "measurement": coinbase_product ('BTC-USD', 'LTC-USD', 'BTC-EUR', etc.),
            \n\t"time": ISO 8601,
            "fields": {
                "low": float,
                \n\t\t"high": float,
                \n\t\t"open": float,
                \n\t\t"close": float,
                \n\t\t"volume": float
                \n\t\t}


        :param start_time: The start datetime of the meta time-slice to download (ISO 8601).
        :param end_time: The end datetime of the meta time-slice to download (ISO 8601).
        :param coinbase_product: The coinbase product to download data for ('BTC-USD', 'LTC-USD', 'BTC-EUR', etc.).
        :param granularity: (TBD not currently used) The granularity of the time slice {60, 300, 900, 3600, 21600, 86400}.
                            These values correspond to timeslices representing one minute, five minutes,
                            fifteen minutes, one hour, six hours, and one day, respectively.
        """

        logger = logging.getLogger()

        db_found = False
        while not db_found:

            dblist = self._db_client_connection.get_list_database()

            for db in dblist:
                if db['name'] == 'coinbase-historic-rates':
                    db_found = True
            if not (db_found):
                if self.verbose:
                    print('Database coinbase-historic-rates not found, trying to create it')

                logger.info('Database coinbase-historic-rates not found, trying to create it')

                self._db_client_connection.create_database('coinbase-historic-rates')

        num_data_points_pertransaction = 300

        # Add a slight delay so that we don't run into Coinbase's request per second rate limits
        # TODO make configurable for those who do not have this limit
        transaction_delay = 0.0

        for start_time in time_utils.minutes_range(start_time, end_time, num_data_points_pertransaction):

            # For one minute granularity generate a start and end time with a delta of 300 minutes
            # This corresponds to the 300 data point per transaction limit imposed by Coinbase
            end_time = start_time + datetime.timedelta(minutes=num_data_points_pertransaction)

            if self.verbose:
                print("Processing time slice: ", start_time, "-", end_time)

            logger.info("Processing time slice: ", start_time, "-", end_time)

            # Sometimes a transaction to coinbase fails for various reasons
            # In that case attempt the transaction again up to 5 times
            transaction_tries = 0
            while True:
                try:
                    candles = self._public_client.get_product_historic_rates(product_id=coinbase_product,
                                                                             start=start_time,
                                                                             end=end_time,
                                                                             granularity=60)

                    # If the transaction returned a dict, there is probably an exception message for us to handle
                    if type(candles) is dict and \
                       candles['message'] != "":

                        # If we got the message that the rate limit was exceeded, we need to slow things down
                        if candles['message'] == "Public rate limit exceeded" or \
                           candles['message'] == "Slow rate limit exceeded":
                            if self.verbose:
                                print("Adding 0.25s to the transaction delay to comply with Coinbase's rate limit...")

                            logger.warning("Adding 0.25s to the transaction delay to comply with Coinbase's rate limit...")

                            transaction_delay += 0.25
                            time.sleep(1)

                        raise Exception("Coinbase transaction returned exception: %s" % candles['message'])

                    for candle in candles:
                        # TODO check to see if the candle exists already and skip if it does
                        self._db_client_connection.write_points(
                                                    database='coinbase-historic-rates',
                                                    points=formatter.CANDLE_TO_INFLUXDB_JSON(coinbase_product, candle),
                                                    time_precision='s')

                    break

                except Exception as error:
                    if self.verbose:
                        print(str(error))

                    logger.error(str(error))

                    transaction_tries += 1
                    if transaction_tries > 5:
                        if self.verbose:
                            print("Transaction failed...exiting")

                        logger.error("Transaction failed...exiting")
                        return

                    if self.verbose:
                        print("Retrying transaction (attempt #", transaction_tries, ")...")

                    logger.info("Retrying transaction (attempt #", transaction_tries, ")...")

            time.sleep(transaction_delay)
