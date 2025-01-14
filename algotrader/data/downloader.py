import os
import sys
from argparse import ArgumentParser


DOWNLOADER_FILE = "algotrader/data/binanceDownloader/download-kline.py"


def download(
    downloader_file: str = DOWNLOADER_FILE,
    symbols: str = "BTCUSDT ETHUSDT",
    start_date: str = "2025-01-01",
    end_date: str = "2025-01-05",
):

    args = f"-t spot -s {symbols} -startDate {start_date} -endDate {end_date} -i 1s -skip-monthly 1"
    os.system(f"python {downloader_file} {args}")

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

    download(
        symbols = " ".join(args.symbols),
        start_date = args.startDate,
        end_date = args.endDate
    )
