def CANDLE_TO_INFLUXDB_JSON(coinbase_product, candle):
    return [
        {
            "measurement": coinbase_product,
            "time": int(candle[0]),
            "fields": {
                "low": float(candle[1]),
                "high": float(candle[2]),
                "open": float(candle[3]),
                "close": float(candle[4]),
                "volume": float(candle[5])
            }
        }
    ]