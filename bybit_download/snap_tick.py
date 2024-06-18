import os

base_path = r"F:\\bybit\\tick"

import pandas as pd


def process_tick(tick, level="500ms"):
    tick["datetime"] = pd.to_datetime(tick["snap_time"] * 1000 + tick["ms"], unit="ms")
    tick["date"] = tick["datetime"].dt.date.astype(str)
    tick["time"] = tick["datetime"].dt.time.astype(str)
    tick.set_index("datetime", inplace=True)
    tick["mid_price"] = (tick["ask_price_1"] + tick["bid_price_1"]) / 2
    tick["high"] = tick["mid_price"]
    tick["low"] = tick["mid_price"]
    tick["open"] = tick["mid_price"]
    tick["close"] = tick["mid_price"]
    resample = tick.resample(level).agg({"ask_price_1": "last",
                                         "ask_price_2": "last",
                                         "ask_price_3": "last",
                                         "ask_price_4": "last",
                                         "ask_price_5": "last",
                                         "ask_volume_1": "last",
                                         "ask_volume_2": "last",
                                         "ask_volume_3": "last",
                                         "ask_volume_4": "last",
                                         "ask_volume_5": "last",
                                         "bid_price_1": "last",
                                         "bid_price_2": "last",
                                         "bid_price_3": "last",
                                         "bid_price_4": "last",
                                         "bid_price_5": "last",
                                         "bid_volume_1": "last",
                                         "bid_volume_2": "last",
                                         "bid_volume_3": "last",
                                         "bid_volume_4": "last",
                                         "bid_volume_5": "last",
                                         "buy_volume": "sum",
                                         "sell_volume": "sum",
                                         "volume": "sum",
                                         "turnover": "sum",
                                         "open": "first",
                                         "close": "last",
                                         "high": "max",
                                         "low": "min",
                                         "mid_price": "last"
                                         })
    resample.reset_index(inplace=True)
    resample["date"] = resample["datetime"].dt.date.astype(str)
    resample["time"] = resample["datetime"].dt.time.astype(str)
    del tick
    return resample[:-1]


target_path = r"F:\bybit\level2"
if not os.path.exists(target_path):
    os.mkdir(target_path)

from multiprocessing import Pool


def process_file(file):
    print("process: ", file)
    frame = pd.read_parquet(os.path.join(base_path, file))
    frame = process_tick(frame, level="1min")
    frame.to_parquet(os.path.join(target_path, file))


def process_files_in_parallel(f):
    with Pool() as pool:
        pool.map(process_file, f)


files = os.listdir(base_path)
print(f"files: {files}")
if __name__ == "__main__":
    process_files_in_parallel(files)
