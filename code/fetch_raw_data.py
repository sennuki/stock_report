# -*- coding: utf-8 -*-
import os
import json
import time
import pandas as pd
import yfinance as yf
import polars as pl
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import utils
import market_data
from defeatbeta_api.data.ticker import Ticker as DBTicker
import boto3
from botocore.exceptions import NoCredentialsError
from dotenv import load_dotenv

load_dotenv()

# R2 接続設定
R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID")
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME", "stock-data-c1")

s3_client = None
if R2_ACCOUNT_ID and R2_ACCESS_KEY_ID and R2_SECRET_ACCESS_KEY:
    s3_client = boto3.client(
        's3',
        endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        region_name="auto"
    )

# 比較用ETFリスト
SECTOR_ETFS = ["SPY", "XLC", "XLY", "XLP", "XLE", "XLF", "XLV", "XLI", "XLK", "XLB", "XLRE", "XLU"]

def clean_value(v):
    """NaN や Inf を None (null) に変換し、Timestampなどを文字列に変換する"""
    if isinstance(v, (float, np.float64, np.float32)):
        if np.isnan(v) or np.isinf(v):
            return None
    if isinstance(v, (pd.Timestamp, pd.DatetimeIndex)):
        return str(v)
    return v

def stringify_keys_and_clean(d):
    """辞書のキーを文字列にし、値をJSONセーフにする"""
    if isinstance(d, dict):
        return {str(k): stringify_keys_and_clean(v) for k, v in d.items()}
    elif isinstance(d, list):
        return [stringify_keys_and_clean(i) for i in d]
    else:
        return clean_value(d)

def df_to_dict_safe(df):
    """Pandas/Polars DataFrameをJSONシリアライズ可能な辞書に変換"""
    if df is None or (hasattr(df, 'empty') and df.empty):
        return None
    try:
        if isinstance(df, pd.DataFrame):
            df_copy = df.copy()
            if isinstance(df_copy.columns, pd.DatetimeIndex):
                df_copy.columns = df_copy.columns.strftime('%Y-%m-%d')
            data = df_copy.reset_index().to_dict(orient='records')
            return stringify_keys_and_clean(data)
        if isinstance(df, pl.DataFrame):
            return stringify_keys_and_clean(df.to_dicts())
    except Exception as e:
        print(f"Conversion error: {e}")
    return None

def fetch_raw_data_for_ticker(symbol):
    """
    1銘柄の生データを yfinance と defeatbeta-api から取得
    """
    try:
        ticker = yf.Ticker(symbol)
        
        raw_payload = {
            "symbol": symbol,
            "info": stringify_keys_and_clean(ticker.info),
            "history": df_to_dict_safe(ticker.history(period="10y")),
            "income_stmt": df_to_dict_safe(ticker.income_stmt),
            "balancesheet": df_to_dict_safe(ticker.balance_sheet),
            "cashflow": df_to_dict_safe(ticker.cashflow),
            "quarterly_income_stmt": df_to_dict_safe(ticker.quarterly_income_stmt),
            "quarterly_balancesheet": df_to_dict_safe(ticker.quarterly_balance_sheet),
            "quarterly_cashflow": df_to_dict_safe(ticker.quarterly_cashflow),
            "earnings_dates": df_to_dict_safe(ticker.earnings_dates),
            "calendar": stringify_keys_and_clean(ticker.calendar) if ticker.calendar is not None else None,
            "analyst_ratings": df_to_dict_safe(ticker.recommendations_summary),
            "upgrades_downgrades": df_to_dict_safe(ticker.upgrades_downgrades)
        }

        # ETFの場合はdefeatbetaを使わない
        if symbol not in SECTOR_ETFS:
            try:
                db_ticker = DBTicker(symbol)
                raw_payload["dcf_valuation"] = utils.calculate_dcf(symbol, ticker=db_ticker)
                try:
                    raw_payload["db_metrics"] = {
                        "wacc": df_to_dict_safe(db_ticker.wacc()),
                        "revenue_growth": df_to_dict_safe(db_ticker.annual_revenue_yoy_growth()),
                        "fcf_growth": df_to_dict_safe(db_ticker.annual_fcf_yoy_growth())
                    }
                except:
                    raw_payload["db_metrics"] = None
            except Exception as e:
                raw_payload["dcf_valuation"] = None

        json_data = json.dumps(raw_payload, ensure_ascii=False, default=str)

        if s3_client:
            s3_client.put_object(
                Bucket=R2_BUCKET_NAME,
                Key=f"raw/{symbol}.json",
                Body=json_data.encode('utf-8'),
                ContentType='application/json'
            )
        else:
            # Fallback to local save if R2 is not configured
            raw_dir = os.path.join(os.path.dirname(__file__), "raw_data")
            os.makedirs(raw_dir, exist_ok=True)
            with open(os.path.join(raw_dir, f"{symbol}_raw.json"), "w", encoding="utf-8") as f:
                f.write(json_data)
        
        return True
    except Exception as e:
        print(f"Failed to fetch {symbol}: {e}")
        return False

def main():
    import sys
    args = [a for a in sys.argv[1:] if not a.startswith('--')]
    
    if args:
        print(f"Fetching specific symbols: {args}")
        symbols = args
    elif "--test-msft-only" in sys.argv or os.getenv("TEST_MODE") == "true":
        print("Test mode active: Fetching MSFT and SPY only.")
        symbols = ["MSFT", "SPY"]
    else:
        print("Fetching S&P 500 list...")
        df_sp500 = market_data.fetch_sp500_companies_optimized()
        if df_sp500.is_empty():
            symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "META"]
        else:
            symbols = df_sp500['Symbol_YF'].to_list()
        
        # 比較用のセクターETFを追加
        for etf in SECTOR_ETFS:
            if etf not in symbols:
                symbols.append(etf)
    
    max_workers = 1 if len(symbols) <= 3 else int(os.getenv("MAX_WORKERS", 2))
    
    if max_workers == 1:
        for s in symbols:
            print(f"Processing {s}...")
            fetch_raw_data_for_ticker(s)
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(fetch_raw_data_for_ticker, s): s for s in symbols}
            for future in tqdm(as_completed(futures), total=len(symbols)):
                try:
                    future.result()
                except: pass

if __name__ == "__main__":
    main()
