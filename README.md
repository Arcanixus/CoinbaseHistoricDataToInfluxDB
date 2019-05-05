CoinbaseHistoricDataToInfluxDB

A utility program that wraps the cbpro and influxdb python libraries. Its primary purpose is to provide a simple interface for downloading various historic data from coinbase and storing that data directly into influxdb databases. This allows one to pull the coinbase data into a local storage facility where it can be analyzed and manipulated more easily.

######Under Construction######

TODO:

-Add unit tests

-Allow for all Coinbase time granularity levels

-Add functions for other historic data besides rates

-Add an update routine that will download all missing data from the last datetime in the influxdb instance to datetime.today(). Create this routine to be runnable as a daemon?

-Add examples
