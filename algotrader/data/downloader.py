import os
import sys
import shutil
import pandas as pd
import zipfile
from argparse import ArgumentParser



DOWNLOADER_DIR = "algotrader/data/binanceDownloader"
DOWNLOADER_FILE = os.path.join(DOWNLOADER_DIR, "download-kline.py")

CLEANED_DIR = "CRYPTO_DAILY_DATA"

COLUMN_HEADINGS = [
    "Open_time", 
    "Open", "High", "Low", "Close", "Volume",
    "Close_time",
    "Quote_asset_volume", "Number_of_trades",
    "Taker_buy_base_asset_volume", "Taker_buy_quote_asset_volume",
    "Ignore"
]



class DataDownloader:

    def __init__(
        self,
        symbols: list[str] = ["BTCUSDT", "ETHUSDT"],
        start_date: str = "2025-01-01",
        end_date: str = "2025-01-05",
        market: str = "spot",
        sample_rate: str = "1s",
        data_type: str = "klines",
    ):
        self.downloader_script = DOWNLOADER_FILE
        self.symbols = symbols
        self.start_date = pd.to_datetime(start_date)
        self.end_date = pd.to_datetime(end_date)
        self.market = market
        self.sample_rate = sample_rate
        self.data_type = data_type
    
    # def __post__init__(self):
        self.base_path = f"{DOWNLOADER_DIR}\data\{self.market}\daily\{self.data_type}"
    
    def data_download(self):
        os.system(
            (
                f"python {self.downloader_script} "
                f"-t {self.market} -s {' '.join(self.symbols)} "
                f"-startDate {self.start_date.strftime('%Y-%m-%d')} "
                f"-endDate {self.end_date.strftime('%Y-%m-%d')} "
                f"-i {self.sample_rate} -skip-monthly 1"
            )
        )

    def data_reformat(self):
        for symbol in self.symbols:
            
            os.mkdir(f"{CLEANED_DIR}\{symbol}")

            symbol_path = (
                f"{self.base_path}\{symbol}\{self.sample_rate}\\"
                f"{self.start_date.strftime('%Y-%m-%d')}_{self.end_date.strftime('%Y-%m-%d')}"
            )

            d_from, d_to = self.start_date, self.end_date
            for date in pd.date_range(d_from, d_to, freq='d'):
                
                file_name = f"{symbol}-{self.sample_rate}-{date.strftime('%Y-%m-%d')}"
                zip_file = zipfile.ZipFile(f"{symbol_path}\{file_name}.zip", 'r')

                df = pd.read_csv(zip_file.open(f'{file_name}.csv'), header=None)
                df.columns = COLUMN_HEADINGS

                df.to_parquet(f"{CLEANED_DIR}\{symbol}\{date.strftime('%Y-%m-%d')}.parquet")

    def cleanup(self):
        for symbol in self.symbols:
            shutil.rmtree(f"{self.base_path}\{symbol}")

        shutil.rmtree(f"{DOWNLOADER_DIR}\data")



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
        symbols = args.symbols,
        start_date = args.startDate,
        end_date = args.endDate
    )
    downloader.data_download()
    downloader.data_reformat()
    downloader.cleanup()
