import os
import sys
from argparse import ArgumentParser


DOWNLOADER_FILE = "algotrader/data/binanceDownloader/download-kline.py"

class DataDownloader:

    def __init__(
        self,
        symbols: str = "BTCUSDT ETHUSDT",
        start_date: str = "2025-01-01",
        end_date: str = "2025-01-05"
    ):
        self.downloader_script = DOWNLOADER_FILE
        self.symbols = symbols
        self.start_date = start_date
        self.end_date = end_date
    
    def data_download(self):

        args = f"-t spot -s {self.symbols} -startDate {self.start_date} -endDate {self.end_date} -i 1s -skip-monthly 1"
        os.system(f"python {self.downloader_script} {args}")

    def data_preprocess(self):

        # go through each symbol
        # go through each date
        # unzip
        # read into pandas, apply column headings

        # set up new file location
        # save to new location with symbole/date

        return

if __name__ == "__main__":

    parser = ArgumentParser(description=("Download historical data"))
    parser.add_argument(
        '-s', dest='symbols', nargs='+',
        help='Single symbol or multiple symbols separated by space')
    parser.add_argument(
        '-startDate', dest='startDate',
        help='Starting date to download in [YYYY-MM-DD] format')
    parser.add_argument(
        '-endDate', dest='endDate',
        help='Ending date to download in [YYYY-MM-DD] format')
    parser.add_argument(
        '-folder', dest='folder',
        help='Directory to store the downloaded data')
    args = parser.parse_args(sys.argv[1:])

    downloader = DataDownloader(
        symbols = " ".join(args.symbols),
        start_date = args.startDate,
        end_date = args.endDate
    )
    downloader.data_download()
