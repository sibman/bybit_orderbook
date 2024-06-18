import os.path
from tqdm import tqdm
import requests
import os
from fake_useragent import UserAgent
from tenacity import retry, stop_after_attempt, wait_exponential

proxy = {
    "http": "127.0.0.1:5619",
    "https": "127.0.0.1:5619"
}


class DownloadTask:
    def __init__(self, date, symbol="ETHUSDT", base_path=r"F:\bybit"):
        self.symbol = symbol
        self.date = date
        self.base_path = base_path
        if not os.path.exists(os.path.join(self.base_path, "order_book_zip")):
            os.mkdir(os.path.join(self.base_path, "order_book_zip"))
        if not os.path.exists(os.path.join(self.base_path, "order_book_zip")):
            os.mkdir(os.path.join(self.base_path, "trade_zip"))

    @property
    def exist(self):
        return os.path.exists(self.orderbook_save_path) and os.path.exists(self.trading_save_path)

    @property
    def orderbook_save_path(self):
        return os.path.join(self.base_path, "order_book_zip", self.orderbook_filename)

    @property
    def trading_save_path(self):
        return os.path.join(self.base_path, "trade_zip", self.trade_filename)

    @property
    def orderbook_filename(self):
        return f"{self.date}_{self.symbol}_ob500.data.zip"

    @property
    def trade_filename(self):
        return f"{self.symbol}{self.date}.csv.gz"

    @property
    def orderbook_url(self):
        return f"https://quote-saver.bycsi.com/orderbook/linear/{self.symbol}/{self.orderbook_filename}"

    @property
    def trade_url(self):
        return f"https://public.bybit.com/trading/{self.symbol}/{self.trade_filename}"


class Downloader(object):
    def __init__(self, task: DownloadTask):
        self.task = task

    def auto_download(self):
        self.start(self.task.orderbook_url, self.task.orderbook_save_path)
        self.start(self.task.trade_url, self.task.trading_save_path)

    @staticmethod
    @retry(stop=stop_after_attempt(10), wait=wait_exponential(multiplier=1, min=4, max=10))
    def start(url, destination):
        user_agent = UserAgent()
        agent = user_agent.random
        print(f"request: {url} proxy: {proxy}")
        r1 = requests.get(url, stream=True, proxies=proxy, headers={"User-Agent": agent})
        total_size = int(r1.headers['Content-Length'])
        if os.path.exists(destination):
            temp_size = os.path.getsize(destination)  # 本地已经下载的文件大小
        else:
            temp_size = 0
        left_size = total_size - temp_size  # 剩余需要下载的数量
        if left_size == 0:
            print(f"left size: {left_size} ignore")
        else:
            progress_bar = tqdm(total=left_size, unit='iB', unit_scale=True)
            headers = {'Range': 'bytes=%d-' % temp_size, "User-Agent": agent}
            response = requests.get(url, stream=True, proxies=proxy, headers=headers)
            with open(destination, "ab") as f:
                for chunk in response.iter_content(chunk_size=1024):
                    progress_bar.update(len(chunk))
                    f.write(chunk)
                    f.flush()
            progress_bar.close()


if __name__ == '__main__':
    from datetime import datetime, timedelta

    now = datetime.now()
    start = now - timedelta(days=520)
    end = now - timedelta(days=1)
    while start != end:
        task_today = DownloadTask(date=str(start.date()))
        if task_today.exist:
            print(f"ignore {start.date()}")
        else:
            downloader = Downloader(task_today)
            downloader.auto_download()
        start += timedelta(days=1)
