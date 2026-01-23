# -*- coding: utf-8 -*-
import yfinance as yf
import polars as pl
import pandas as pd
import requests
import utils
from io import StringIO
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# ==========================================
#  Part C (前半): データ取得
# ==========================================

def get_market_info(symbol):
    try:
        t = utils.get_ticker(symbol)
        ex = t.info.get('exchange', 'Unknown')
        m_map = {'NMS':'NASDAQ', 'NGM':'NASDAQ', 'NCM':'NASDAQ', 'NYQ':'NYSE', 'ASE':'AMEX', 'PCX':'NYSE', 'PNK':'OTC'}
        return symbol, m_map.get(ex, ex)
    except Exception as e:
        # print(f"Error fetching info for {symbol}: {e}") # Debug output
        return symbol, "NYSE"

def fetch_sp500_companies_optimized():
    print("S&P 500リストを取得中...")
    url = "https://en.wikipedia.org/wiki/List_of_S&P_500_companies"
    try:
        # Wikipediaのテーブルを取得
        wiki_df = pd.read_html(StringIO(requests.get(url, headers={"User-Agent": "Mozilla/5.0"}).text))[0]
        df = pl.from_pandas(wiki_df).select(['Symbol', 'Security', 'GICS Sector', 'GICS Sub-Industry'])
        
        # Symbol_YF: Yahoo Finance用 (ドットをハイフンに変換: BRK.B -> BRK-B)
        # Symbol: 表示用 (ドットに統一: BRK-B -> BRK.B)
        df = df.with_columns([
            pl.col('Symbol').str.replace(r"\.", "-", literal=False).alias('Symbol_YF'),
            pl.col('Symbol').str.replace(r"-", ".", literal=False).alias('Symbol')
        ])

        symbols = df['Symbol_YF'].to_list()
        ex_map = {}
        print(f"{len(symbols)} 銘柄の市場情報を取得中... (並列処理)")
        # Rate limit回避のため並列数を抑える
        with ThreadPoolExecutor(max_workers=2) as ex:
            f_map = {ex.submit(get_market_info, s): s for s in symbols}
            for f in tqdm(as_completed(f_map), total=len(symbols)):
                s, e = f.result()
                ex_map[s] = e

        return df.with_columns(pl.col('Symbol_YF').map_elements(lambda s: ex_map.get(s, "NYSE"), return_dtype=pl.Utf8).alias('Exchange'))
    except Exception as e:
        print(f"Failed to fetch S&P 500 list: {e}")
        return pl.DataFrame()

if __name__ == "__main__":
    print("S&P 500データの取得テストを実行します...")
    df = fetch_sp500_companies_optimized()
    print("\n--- S&P 500 List (First 5 rows) ---")
    print(df.head())
    print(f"Total records: {len(df)}")
