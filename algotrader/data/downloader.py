import os
import sys
import shutil
import pandas as pd
import zipfile
from argparse import ArgumentParser



DOWNLOADER_DIR = "algotrader/data/binanceDownloader"
BAR_DOWNLOADER_FILE = os.path.join(DOWNLOADER_DIR, "download-kline.py")
TRADE_DOWNLOADER_FILE = os.path.join(DOWNLOADER_DIR, "download-trade.py")

CLEANED_DIR = "C:/Users/86155/Desktop/CRYPTO_DAILY_DATA"

BAR_COLUMN_HEADINGS = [
    "Open_time", 
    "Open", "High", "Low", "Close", "Volume",
    "Close_time",
    "Quote_asset_volume", "Number_of_trades",
    "Taker_buy_base_asset_volume", "Taker_buy_quote_asset_volume",
    "Ignore"
]
TRADE_COLUMN_HEADINGS = [
    "Trade_Id", "Price", "Qty", "QuoteQty", "Time",
    "IsBuyerMaker", "IsBestMatch"
]



class DataDownloader:

    def __init__(
        self,
        symbols: list[str] = ["BTCUSDT", "ETHUSDT"],
        start_date: str = "2025-01-01",
        end_date: str = "2025-01-05",
        data_type: str = "klines",
        market: str = "spot",
        sample_rate: str = "1s"
    ):
        self.symbols = symbols
        self.start_date = pd.to_datetime(start_date)
        self.end_date = pd.to_datetime(end_date)
        self.data_type = data_type
        self.market = market
        self.sample_rate = sample_rate

        self.base_path = f"{DOWNLOADER_DIR}\data\{self.market}\daily\{self.data_type}"

        self._validate()

    def _validate(self):
        if self.data_type != "klines" and self.data_type != "trades":
            assert False, "Unexpected data type. Only expect: klines or trades."
        if self.market != "spot":
            assert False, "Unexpected market type. Only accexpectept: spot."
        if self.sample_rate != "1s":
            assert False, "Unexpected sample rate. Only expect: 1s."

    def data_download(self):
        if self.data_type == "klines":
            os.system(
                (
                    f"python {BAR_DOWNLOADER_FILE} "
                    f"-t {self.market} -s {' '.join(self.symbols)} "
                    f"-startDate {self.start_date.strftime('%Y-%m-%d')} "
                    f"-endDate {self.end_date.strftime('%Y-%m-%d')} "
                    f"-i {self.sample_rate} -skip-monthly 1"
                )
            )
        elif self.data_type == "trades":
            os.system(
                (
                    f"python {TRADE_DOWNLOADER_FILE} "
                    f"-t {self.market} -s {' '.join(self.symbols)} "
                    f"-startDate {self.start_date.strftime('%Y-%m-%d')} "
                    f"-endDate {self.end_date.strftime('%Y-%m-%d')} "
                    f"-skip-monthly 1"
                )
            )

    def data_reformat(self):
        for symbol in self.symbols:
            
            output_dir = f"{CLEANED_DIR}\{symbol}"
            if not os.path.exists(output_dir):
                os.mkdir(output_dir)

            symbol_path = (
                f"{self.base_path}\{symbol}\{self.sample_rate}\\"
                f"{self.start_date.strftime('%Y-%m-%d')}_{self.end_date.strftime('%Y-%m-%d')}"
            ) if self.data_type == "klines" else \
            (
                f"{self.base_path}\{symbol}\\"
                f"{self.start_date.strftime('%Y-%m-%d')}_{self.end_date.strftime('%Y-%m-%d')}"
            )

            d_from, d_to = self.start_date, self.end_date
            for date in pd.date_range(d_from, d_to, freq='d'):
                
                file_name = f"{symbol}-{self.sample_rate}-{date.strftime('%Y-%m-%d')}" \
                    if self.data_type == "klines" \
                    else f"{symbol}-trades-{date.strftime('%Y-%m-%d')}"
                zip_file = zipfile.ZipFile(f"{symbol_path}\{file_name}.zip", 'r')

                df = pd.read_csv(zip_file.open(f'{file_name}.csv'), header=None)
                df.columns = BAR_COLUMN_HEADINGS \
                    if self.data_type == "klines" \
                    else TRADE_COLUMN_HEADINGS

                df.to_parquet(f"{CLEANED_DIR}\{symbol}\{self.data_type}_{date.strftime('%Y-%m-%d')}.parquet")

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
        end_date = args.endDate,
        data_type = "klines"
    )
    downloader.data_download()
    downloader.data_reformat()
    downloader.cleanup()

    downloader = DataDownloader(
        symbols = args.symbols,
        start_date = args.startDate,
        end_date = args.endDate,
        data_type = "trades"
    )
    downloader.data_download()
    downloader.data_reformat()
    downloader.cleanup()
