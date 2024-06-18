import download_bybit
# from bybit.download_bybit import Downloader, DownloadTask

down_task = download_bybit.DownloadTask(date="2024-04-04")

download_bybit.Downloader(down_task).auto_download()
