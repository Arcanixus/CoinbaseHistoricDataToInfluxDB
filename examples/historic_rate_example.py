"""
This very simple example app instantiates an instance of the coinbase_historic_data_to_influxdb object
and uses it to stream the historic rate candle data for BTC-USD into a vanilla setup locally hosted influxdb instance.
The data is from the date range 1/1/2017-today's date.
"""

import source.coinbase_historic_data_to_influxdb as CB_TO_DB
import datetime
import logging

logging.basicConfig(filename='historic_rate_example_log.txt',
                    format='%(asctime)s %(levelname)s: %(filename)s'
                           ' ln #%(lineno)d %(funcName)s() - %(message)s',
                    datefmt='%Y-%m-%dT%H:%M:%SZ')

coinbase_downloader = CB_TO_DB.CoinbaseHistoricDataToInfluxDB(verbose=True)

coinbase_downloader.download_historic_rates(start_time=datetime.datetime(2017, 1, 1, 0, 0, 0),
                                            end_time=datetime.datetime.today(),
                                            coinbase_product='BTC-USD')
